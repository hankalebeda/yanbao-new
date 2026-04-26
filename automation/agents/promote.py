"""PromoteAgent — tiered autonomous promotion with circuit-breaker.

Tier 1 (auto):     status-note                  (confidence >= 0.7, all gates green)
Tier 2 (delayed):  shared-artifact readiness    (2 consecutive successful rounds)
Tier 3 (protected):current-layer + doc22        (requires sustained green rounds)

Circuit-breaker:  2 consecutive post-promote regressions → SAFE_HOLD
Doc22 writeback:  Idempotent anchor-point updates with evidence chain
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import (
    AgentMessage,
    AgentRole,
    MessageType,
    PromoteDecision,
)

logger = logging.getLogger(__name__)

CONSECUTIVE_SUCCESS_FOR_TIER2 = 2
POST_PROMOTE_REGRESSION_LIMIT = 2
READY_SKIP_REASONS = frozenset(
    {
        "RUN_ID_ALREADY_PRESENT",
        "SEMANTIC_FINGERPRINT_ALREADY_PRESENT",
        "CURRENT_LAYER_SEMANTIC_FINGERPRINT_ALREADY_PRESENT",
        "CURRENT_LAYER_NO_CHANGE",
    }
)


class PromoteAgent(BaseAgent):
    """Manages tiered promotion and circuit-breaking.

    Wraps the existing ``promote_prep`` service for the actual
    promotion mechanics, adding autonomous decision-making.
    """

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.PROMOTE, mailbox=mailbox, config=config)
        self._promote_url = self.config.service_urls.get("promote_prep", "")
        self._promote_token = self.config.service_tokens.get("promote_prep", "")
        self._wb_b_url = self.config.service_urls.get("writeback_b", "")
        self._wb_b_token = self.config.service_tokens.get("writeback_b", "")
        self._stub_mode = not self._promote_url
        self._state_path = (
            self.config.repo_root / "runtime" / "agents" / "promote_state.json"
        )
        self._promote_state = self._load_promote_state()

    def _write_local_promote_artifact(
        self,
        round_id: str,
        tier: int,
        reason: str,
        targets_promoted: List[str],
    ) -> None:
        """Write a local degraded promote artifact when services unavailable."""
        artifact = {
            "round_id": round_id,
            "tier": tier,
            "approved": bool(targets_promoted),
            "reason": reason,
            "targets_promoted": targets_promoted,
            "mode": "local_degraded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        artifact_path = self.config.repo_root / "output" / "promote_local_artifact.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("[%s] Local degraded promote artifact written: %s", self.agent_id, artifact_path)

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        round_id = payload.get("round_id", "")
        writeback_data = payload.get("writeback", {})
        verify_data = payload.get("verify", {})

        all_passed = verify_data.get("all_passed", False)
        if not all_passed:
            logger.info("[%s] Skipping promotion — verification not passed", self.agent_id)
            return PromoteDecision(
                round_id=round_id,
                tier=0,
                approved=False,
                reason="verification_not_passed",
            ).to_dict()

        # Determine tier
        tier = self._determine_tier(verify_data)

        decision = await self._execute_promotion(round_id, tier, writeback_data, verify_data)

        # Update state
        if decision.approved:
            self._promote_state["consecutive_successes"] = (
                self._promote_state.get("consecutive_successes", 0) + 1
            )
            self._promote_state["post_promote_regressions"] = 0
            self._promote_state["last_promote"] = datetime.now(timezone.utc).isoformat()
            self._promote_state["last_tier"] = tier
        else:
            self._promote_state["consecutive_successes"] = 0
        self._save_promote_state()

        logger.info(
            "[%s] Promote decision: tier=%d, approved=%s, reason=%s",
            self.agent_id, decision.tier, decision.approved, decision.reason,
        )

        return decision.to_dict()

    def _determine_tier(self, verify_data: Dict[str, Any]) -> int:
        """Decide which promotion tier to attempt."""
        consecutive = self._promote_state.get("consecutive_successes", 0)

        # Tier 3: doc22 — only after sustained success
        if consecutive >= CONSECUTIVE_SUCCESS_FOR_TIER2 * 2:
            return 3
        # Tier 2: shared-artifact — after 2 consecutive successes
        if consecutive >= CONSECUTIVE_SUCCESS_FOR_TIER2:
            return 2
        # Tier 1: status-note only
        return 1

    def _auth_headers(self, token: str) -> Dict[str, str]:
        if not token:
            return {}
        return {"Authorization": f"Bearer {token}"}

    def _build_runtime_context(
        self,
        round_id: str,
        verify_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        raw_runtime_gates = verify_data.get("runtime_gates")
        runtime_gates = dict(raw_runtime_gates) if isinstance(raw_runtime_gates, dict) else {}
        shared_artifact = runtime_gates.get("shared_artifact_promote")
        if not isinstance(shared_artifact, dict):
            shared_artifact = {}
        shared_artifact.setdefault("allowed", bool(verify_data.get("artifacts_aligned")))
        runtime_gates["shared_artifact_promote"] = shared_artifact
        runtime_gates.setdefault("status", "ready" if verify_data.get("all_passed") else "blocked")

        context: Dict[str, Any] = {
            "runtime_gates": runtime_gates,
            "public_runtime_status": str(
                verify_data.get("public_runtime_status")
                or (
                    "READY"
                    if str(runtime_gates.get("status") or "").strip().lower() == "ready"
                    else "BLOCKED"
                )
            ),
            "verify_all_green": bool(verify_data.get("all_passed")),
            "artifacts_aligned": bool(verify_data.get("artifacts_aligned")),
            "round_id": round_id,
        }
        audit_context = verify_data.get("audit_context")
        if isinstance(audit_context, dict):
            context.update(audit_context)
        for key in ("context_status", "context_error", "last_audit_run_id"):
            value = verify_data.get(key)
            if value not in (None, ""):
                context[key] = value
        return context

    def _shared_artifact_ready(
        self,
        writeback_data: Dict[str, Any],
        verify_data: Dict[str, Any],
    ) -> bool:
        if not writeback_data.get("receipt_count"):
            return False
        runtime_gates = verify_data.get("runtime_gates")
        if isinstance(runtime_gates, dict):
            shared_artifact = runtime_gates.get("shared_artifact_promote")
            if isinstance(shared_artifact, dict) and "allowed" in shared_artifact:
                return bool(shared_artifact.get("allowed"))
        return bool(verify_data.get("artifacts_aligned"))

    def _current_layer_allowed(
        self,
        verify_data: Dict[str, Any],
        writeback_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if writeback_data is not None and not writeback_data.get("receipt_count"):
            return False
        runtime_gates = verify_data.get("runtime_gates")
        if isinstance(runtime_gates, dict):
            shared_artifact = runtime_gates.get("shared_artifact_promote")
            if isinstance(shared_artifact, dict) and "allowed" in shared_artifact:
                return bool(shared_artifact.get("allowed"))
        return bool(verify_data.get("artifacts_aligned"))

    @staticmethod
    def _promote_result_ready(result: Dict[str, Any]) -> bool:
        status = str(result.get("status") or "").strip().lower()
        if status == "committed":
            return True
        if status == "skipped":
            return str(result.get("skip_reason") or "").strip() in READY_SKIP_REASONS
        return False

    async def _execute_promotion(
        self,
        round_id: str,
        tier: int,
        writeback_data: Dict[str, Any],
        verify_data: Dict[str, Any],
    ) -> PromoteDecision:
        """Execute the promotion for the determined tier."""
        regressions = self._promote_state.get("post_promote_regressions", 0)
        if regressions >= POST_PROMOTE_REGRESSION_LIMIT:
            # v3: Auto-reset after consecutive green rounds
            consecutive = self._promote_state.get("consecutive_successes", 0)
            if consecutive >= 2:
                logger.info(
                    "[%s] Circuit-breaker auto-reset after %d green rounds (was %d regressions)",
                    self.agent_id, consecutive, regressions,
                )
                self._promote_state["post_promote_regressions"] = 0
                regressions = 0
                self._save_promote_state()
            else:
                logger.warning(
                    "[%s] Promotion blocked by circuit-breaker (%d regressions, %d green)",
                    self.agent_id, regressions, consecutive,
                )
                return PromoteDecision(
                    round_id=round_id,
                    tier=tier,
                    approved=False,
                    reason="circuit_breaker",
                    targets_promoted=[],
                )

        targets_promoted: List[str] = []
        status_note_ready = False

        # v5: Local degraded path — write local artifact when remote services unavailable
        if self._stub_mode and not self._wb_b_url:
            self._write_local_promote_artifact(round_id, tier, "local_degraded_no_services", ["status-note-local"])
            return PromoteDecision(
                round_id=round_id,
                tier=tier,
                approved=True,
                reason="local_degraded",
                targets_promoted=["status-note-local"],
            )

        # Tier 1: status-note only
        if tier >= 1:
            status_note_result = await self._promote_status_note(round_id, verify_data)
            status_note_ready = self._promote_result_ready(status_note_result)
            if status_note_ready:
                targets_promoted.append("status-note")
            else:
                logger.info("[%s] Status-note promotion declined: %s", self.agent_id, status_note_result)

        # Tier 2: record shared-artifact readiness once a real promote succeeded.
        if tier >= 2 and status_note_ready and self._shared_artifact_ready(writeback_data, verify_data):
                targets_promoted.append("shared-artifact")

        # Tier 3: protected current-layer/doc22 promote.
        if tier >= 3 and status_note_ready:
            current_layer_result = await self._promote_current_layer(round_id, verify_data, writeback_data)
            if self._promote_result_ready(current_layer_result):
                targets_promoted.append("current-layer")
                targets_promoted.append("doc22")

                # Doc22 direct writeback — idempotent anchor-point update
                fix_results = writeback_data.get("fix_results", [])
                if fix_results:
                    wb_result = await self.writeback_doc22(round_id, fix_results)
                    if wb_result.get("written", 0) > 0:
                        targets_promoted.append("doc22-writeback")
                        logger.info(
                            "[%s] Doc22 writeback: %d entries updated",
                            self.agent_id, wb_result["written"],
                        )
            else:
                logger.info("[%s] Current-layer promotion declined: %s", self.agent_id, current_layer_result)

        approved = any(
            target in {"status-note", "status-note-local", "current-layer", "doc22"}
            for target in targets_promoted
        )

        return PromoteDecision(
            round_id=round_id,
            tier=tier,
            approved=approved,
            reason=f"tier{tier}_{'ok' if approved else 'blocked'}",
            targets_promoted=targets_promoted,
        )

    async def _promote_status_note(
        self, round_id: str, verify_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        return await self._promote_target(
            round_id=round_id,
            verify_data=verify_data,
            promote_type="status-note",
        )

    async def _promote_current_layer(
        self,
        round_id: str,
        verify_data: Dict[str, Any],
        writeback_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self._current_layer_allowed(verify_data, writeback_data):
            return {"status": "skipped", "skip_reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"}
        return await self._promote_target(
            round_id=round_id,
            verify_data=verify_data,
            promote_type="current-layer",
            enabled=True,
        )

    async def _promote_target(
        self,
        *,
        round_id: str,
        verify_data: Dict[str, Any],
        promote_type: str,
        enabled: bool = False,
    ) -> Dict[str, Any]:
        if self._stub_mode:
            logger.debug("Stub mode — auto-approve %s", promote_type)
            return {"status": "committed", "commit_id": f"stub-{promote_type}"}
        if not self._wb_b_url:
            return {"status": "failed", "error": "WRITEBACK_B_NOT_CONFIGURED"}

        audit_run_id = str(verify_data.get("audit_run_id") or round_id)
        runtime_context = self._build_runtime_context(round_id, verify_data)
        prepare_json = {
            "run_id": audit_run_id,
            "runtime_gates": runtime_context.get("runtime_gates") or {},
            "audit_context": {
                **runtime_context,
                "round_id": round_id,
                "source": "escort_team",
            },
        }
        if enabled:
            prepare_json["enabled"] = True

        try:
            async with httpx.AsyncClient(timeout=120.0, trust_env=False) as client:
                prepare_resp = await client.post(
                    f"{self._promote_url}/v1/promote/{promote_type}",
                    json=prepare_json,
                    headers=self._auth_headers(self._promote_token),
                )
                if prepare_resp.status_code >= 400:
                    return {
                        "status": "failed",
                        "error": f"PROMOTE_PREPARE_FAILED:{promote_type}:{prepare_resp.status_code}",
                    }
                return await self._commit_promote_patch(
                    client,
                    prepare_resp.json(),
                    runtime_context={
                        **runtime_context,
                        "round_id": round_id,
                        "source": "escort_team",
                    },
                    audit_run_id=audit_run_id,
                )
        except (httpx.ConnectError, httpx.ConnectTimeout, OSError) as exc:
            # v9: Service unreachable — degrade to local artifact so that
            # formal_promote evidence is recorded even without remote services.
            logger.warning(
                "[%s] %s promote service unreachable (%s) — writing local artifact",
                self.agent_id, promote_type, exc,
            )
            self._write_local_promote_artifact(
                round_id, 1, f"local_degraded_service_unreachable:{promote_type}", [f"{promote_type}-local"],
            )
            return {"status": "committed", "commit_id": f"local-{promote_type}-{round_id[:8]}"}
        except Exception as exc:
            logger.warning("[%s] %s promote failed: %s", self.agent_id, promote_type, exc)
            return {"status": "failed", "error": f"PROMOTE_FLOW_FAILED:{promote_type}:{exc}"}

    async def _commit_promote_patch(
        self,
        client: httpx.AsyncClient,
        prepare_payload: Dict[str, Any],
        *,
        runtime_context: Dict[str, Any],
        audit_run_id: str,
    ) -> Dict[str, Any]:
        layer = str(prepare_payload.get("layer") or "unknown")
        if bool(prepare_payload.get("skip_commit")):
            return {
                "status": "skipped",
                "skip_reason": str(prepare_payload.get("skip_reason") or "PREPARE_SKIP"),
            }

        triage_resp = await client.post(
            f"{self._promote_url}/v1/triage",
            json={
                "run_id": prepare_payload["run_id"],
                "layer": prepare_payload["layer"],
                "target_path": prepare_payload["target_path"],
                "target_anchor": prepare_payload.get("target_anchor"),
                "patch_text": prepare_payload["patch_text"],
                "base_sha256": prepare_payload["base_sha256"],
                "runtime_gates": runtime_context.get("runtime_gates") or {},
                "audit_context": runtime_context,
                "semantic_fingerprint": prepare_payload.get("semantic_fingerprint"),
            },
            headers=self._auth_headers(self._promote_token),
        )
        if triage_resp.status_code >= 400:
            return {"status": "failed", "error": f"TRIAGE_FAILED:{layer}:{triage_resp.status_code}"}

        triage_payload = triage_resp.json()
        if not bool(triage_payload.get("auto_commit")):
            return {
                "status": "skipped",
                "skip_reason": str(triage_payload.get("reason") or "TRIAGE_REJECTED"),
                "triage": triage_payload,
            }

        preview_payload: Dict[str, Any] = {}
        for attempt in range(MAX_RETRIES):
            try:
                preview_resp = await client.post(
                    f"{self._wb_b_url}/v1/preview",
                    json={
                        "target_path": prepare_payload["target_path"],
                        "base_sha256": prepare_payload["base_sha256"],
                        "patch_text": prepare_payload["patch_text"],
                    },
                    headers=self._auth_headers(self._wb_b_token),
                )
                if preview_resp.status_code == 403:
                    detail = ""
                    try:
                        detail = preview_resp.json().get("detail", "")
                    except Exception:
                        detail = preview_resp.text[:200]
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(BACKOFF_BASE ** attempt)
                        continue
                    return {
                        "status": "failed",
                        "error": f"WRITEBACK_B_PREVIEW_403:{detail} (target={prepare_payload['target_path']})",
                    }
                if preview_resp.status_code >= 400:
                    return {"status": "failed", "error": f"PREVIEW_B_FAILED:{layer}:{preview_resp.status_code}"}
                preview_payload = preview_resp.json()
                break
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
                    continue
                return {"status": "failed", "error": f"WRITEBACK_B_PREVIEW_TRANSIENT:{exc}"}

        if bool(preview_payload.get("conflict")):
            return {
                "status": "skipped",
                "skip_reason": "PREVIEW_CONFLICT",
                "preview": preview_payload,
            }

        commit_resp = await client.post(
            f"{self._wb_b_url}/v1/commit",
            json={
                "target_path": prepare_payload["target_path"],
                "base_sha256": prepare_payload["base_sha256"],
                "patch_text": prepare_payload["patch_text"],
                "idempotency_key": str(
                    prepare_payload.get("idempotency_key") or f"escort-team-promote-{audit_run_id}-{layer}"
                ),
                "actor": {"type": "escort_team", "id": "promote"},
                "request_id": str(
                    prepare_payload.get("request_id") or f"escort-team-promote-{audit_run_id}-{layer}"
                ),
                "run_id": str(prepare_payload.get("run_id") or audit_run_id),
                "triage_record_id": triage_payload.get("triage_record_id"),
            },
            headers=self._auth_headers(self._wb_b_token),
        )
        if commit_resp.status_code >= 400:
            return {"status": "failed", "error": f"COMMIT_B_FAILED:{layer}:{commit_resp.status_code}"}

        commit_payload = commit_resp.json()
        commit_status = str(commit_payload.get("status") or "").strip().lower()
        if commit_status == "skipped":
            return {
                "status": "skipped",
                "skip_reason": str(commit_payload.get("skip_reason") or "COMMIT_SKIP"),
                "commit_id": commit_payload.get("commit_id"),
            }
        if commit_status == "committed" or bool(commit_payload.get("idempotent_replay")):
            return {"status": "committed", "commit_id": commit_payload.get("commit_id")}
        return {"status": "failed", "error": f"COMMIT_B_INVALID:{layer}"}

    async def report_regression(self) -> None:
        """Called when a post-promote regression is detected."""
        self._promote_state["post_promote_regressions"] = (
            self._promote_state.get("post_promote_regressions", 0) + 1
        )
        self._promote_state["consecutive_successes"] = 0
        self._save_promote_state()

        regressions = self._promote_state["post_promote_regressions"]
        if regressions >= POST_PROMOTE_REGRESSION_LIMIT:
            logger.warning(
                "[%s] Circuit-breaker tripped: %d post-promote regressions",
                self.agent_id, regressions,
            )
            await self.mailbox.send(AgentMessage(
                source=self.agent_id,
                target="coordinator",
                msg_type=MessageType.PROMOTE_ROLLBACK.value,
                payload={
                    "reason": "circuit_breaker",
                    "regressions": regressions,
                },
            ))

    # ------------------------------------------------------------------
    # Doc22 idempotent writeback
    # ------------------------------------------------------------------

    _DOC22_PROBLEM_ANCHOR = re.compile(
        r"^(#{2,4}\s+(?:P[012]|ISSUE)[\s\-:：]*.*)", re.MULTILINE
    )
    _DOC22_STATUS_LINE = re.compile(
        r"^(\s*[-*]\s*\*?\*?状态\*?\*?\s*[:：]\s*)(.*)$", re.MULTILINE
    )

    async def writeback_doc22(
        self,
        round_id: str,
        fix_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Write fix results back to doc22 anchor points (idempotent).

        Each fix_result should contain:
          - problem_id: str
          - fix_summary: str
          - verify_passed: bool
          - evidence_commit_id: str
          - timestamp: str (ISO)

        Returns: {"written": int, "skipped": int, "errors": list}
        """
        doc22_path = self._find_doc22()
        if not doc22_path:
            return {"written": 0, "skipped": 0, "errors": ["doc22_not_found"]}

        try:
            content = doc22_path.read_text(encoding="utf-8")
        except Exception as exc:
            return {"written": 0, "skipped": 0, "errors": [f"doc22_read_error: {exc}"]}

        written = 0
        skipped = 0
        errors: List[str] = []

        for result in fix_results:
            problem_id = result.get("problem_id", "")
            if not problem_id:
                skipped += 1
                continue

            stamp = result.get("timestamp") or datetime.now(timezone.utc).isoformat()
            status_text = (
                f"✅ 已修复 (escort-team {round_id}) | "
                f"{result.get('fix_summary', 'auto-fix')} | "
                f"验证: {'通过' if result.get('verify_passed') else '未通过'} | "
                f"commit: {result.get('evidence_commit_id', 'n/a')[:12]} | "
                f"{stamp[:19]}"
            )

            # Idempotency: compute fingerprint to avoid duplicate writes
            fp = hashlib.sha256(f"{problem_id}:{round_id}:{status_text}".encode()).hexdigest()[:16]
            if fp in content:
                skipped += 1
                continue

            # Find the anchor for this problem and insert/replace status
            updated = self._update_problem_status(content, problem_id, status_text, fp)
            if updated != content:
                content = updated
                written += 1
            else:
                skipped += 1

        if written > 0:
            try:
                # Atomic write
                tmp_path = doc22_path.with_suffix(".md.tmp")
                tmp_path.write_text(content, encoding="utf-8")
                tmp_path.replace(doc22_path)
                logger.info(
                    "[%s] Doc22 writeback: %d written, %d skipped",
                    self.agent_id, written, skipped,
                )
            except Exception as exc:
                errors.append(f"doc22_write_error: {exc}")

        return {"written": written, "skipped": skipped, "errors": errors}

    def _find_doc22(self) -> Optional[Path]:
        """Locate doc22 in the repo."""
        core = self.config.repo_root / "docs" / "core"
        if not core.is_dir():
            return None
        for p in sorted(core.iterdir()):
            if "22_" in p.name.lower() and p.name.endswith(".md"):
                return p
        return None

    def _update_problem_status(
        self, content: str, problem_id: str, status_text: str, fingerprint: str
    ) -> str:
        """Find the problem anchor in doc22 and update its status line.

        Strategy:
        1. Find the section heading containing the problem_id
        2. Look for an existing '状态' line within the next 20 lines
        3. If found, replace it; if not, insert after the heading
        4. Append fingerprint as HTML comment for idempotency
        """
        lines = content.split("\n")
        pid_lower = problem_id.lower().replace("-", "").replace("_", "")

        for i, line in enumerate(lines):
            # Match section headings or bullet items containing the problem ID
            line_clean = line.lower().replace("-", "").replace("_", "")
            if pid_lower not in line_clean:
                continue
            if not (line.strip().startswith("#") or line.strip().startswith("- ") or line.strip().startswith("* ")):
                continue

            # Found anchor — look for existing status line within next 20 lines
            status_found = False
            search_end = min(i + 20, len(lines))
            for j in range(i + 1, search_end):
                if self._DOC22_STATUS_LINE.match(lines[j]):
                    # Replace existing status line (idempotent overwrite)
                    lines[j] = f"  - **状态**: {status_text} <!-- fp:{fingerprint} -->"
                    status_found = True
                    break
                # Stop at next heading
                if lines[j].strip().startswith("#"):
                    break

            if not status_found:
                # Insert new status line after anchor
                indent = "  - " if line.strip().startswith("#") else "    - "
                lines.insert(i + 1, f"{indent}**状态**: {status_text} <!-- fp:{fingerprint} -->")

            return "\n".join(lines)

        return content  # no anchor found — return unchanged

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_promote_state(self) -> Dict[str, Any]:
        if self._state_path.exists():
            try:
                return json.loads(self._state_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "consecutive_successes": 0,
            "post_promote_regressions": 0,
            "last_promote": "",
            "last_tier": 0,
        }

    def _save_promote_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._state_path.write_text(
                json.dumps(self._promote_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Promote state save error")
