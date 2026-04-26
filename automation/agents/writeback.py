"""WritebackAgent — guarded writeback with lease/fencing.

Wraps the existing ``writeback_service`` (ports 8092/8095),
providing:
* Lease acquisition before any write
* Atomic batch commit via the writeback service API
* Retry with exponential backoff on transient errors
* Full audit trail generation
* Automatic lease release on completion or failure

Does NOT reimplement the writeback logic — delegates to the
existing FastAPI service via HTTP.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import (
    AgentRole,
    PatchSet,
    VerifyResult,
    WritebackReceipt,
)

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 2.0  # seconds


class WritebackAgent(BaseAgent):
    """Executes guarded writeback via the writeback-service API."""

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.WRITEBACK, mailbox=mailbox, config=config)
        self._wb_url = self.config.service_urls.get("writeback_a", "http://127.0.0.1:8092")
        self._wb_token = self.config.service_tokens.get("writeback_a", "")
        self._wb_b_url = self.config.service_urls.get("writeback_b", "http://127.0.0.1:8095")
        self._wb_b_token = self.config.service_tokens.get("writeback_b", "")
        self._promote_prep_url = self.config.service_urls.get("promote_prep", "")
        self._promote_prep_token = self.config.service_tokens.get("promote_prep", "")

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        round_id = payload.get("round_id", "")
        verify_data = payload.get("verify", {})
        patches_raw = payload.get("patches", [])
        patches = [
            PatchSet.from_dict(p) if isinstance(p, dict) else p
            for p in patches_raw
        ]

        receipts: List[WritebackReceipt] = []
        errors: List[str] = []

        for ps in patches:
            try:
                receipt = await self._writeback_one(round_id, ps, verify_data)
                if receipt:
                    receipts.append(receipt)
            except Exception as exc:
                logger.exception(
                    "[%s] Writeback failed for %s", self.agent_id, ps.problem_id
                )
                errors.append(f"{ps.problem_id}: {exc}")

        logger.info(
            "[%s] Writeback complete: %d receipts, %d errors",
            self.agent_id, len(receipts), len(errors),
        )

        return {
            "findings": [r.to_dict() for r in receipts],
            "receipt_count": len(receipts),
            "errors": errors,
        }

    async def _writeback_one(
        self,
        round_id: str,
        patch_set: PatchSet,
        verify_data: Dict[str, Any],
    ) -> Optional[WritebackReceipt]:
        """Write back a single PatchSet via the writeback service."""
        real_patches = [
            p for p in patch_set.patches
            if not p.get("path", "").startswith("__analysis__")
        ]

        if not real_patches:
            logger.debug(
                "[%s] Skipping stub patches for %s",
                self.agent_id, patch_set.problem_id,
            )
            return None

        # v7 B2: SHA256 delta idempotency check.
        # If every new_text already matches what is on disk, the patch has
        # been applied before (or is a no-op).  Skip to avoid duplicate entries.
        if self._all_patches_already_applied(real_patches):
            logger.info(
                "[%s] SHA256 delta: all patches already applied for %s — skipping writeback",
                self.agent_id, patch_set.problem_id,
            )
            return None

        # Determine which writeback instance to use
        target_paths = [p["path"] for p in real_patches]
        wb_url, wb_token = self._select_writeback_instance(target_paths)

        # Step 1: Claim lease
        lease_id, fencing_token = await self._claim_lease(
            wb_url, wb_token, round_id, target_paths
        )
        if not lease_id or fencing_token in (None, ""):
            raise RuntimeError(f"lease_claim_failed:{target_paths}")

        try:
            preview_summary = await self._batch_preview(wb_url, wb_token, real_patches)
            triage_record_ids = await self._triage_patches(
                round_id,
                patch_set,
                verify_data,
                preview_summary,
                wb_url,
            )

            # Step 2: Batch commit
            commit_result = await self._batch_commit(
                wb_url,
                wb_token,
                round_id,
                patch_set.problem_id,
                real_patches,
                lease_id,
                fencing_token,
                triage_record_ids,
            )

            commits = commit_result.get("commits") or []
            commit_sha = ""
            if commits and isinstance(commits[0], dict):
                commit_sha = str(commits[0].get("commit_id") or "")

            return WritebackReceipt(
                round_id=round_id,
                problem_id=patch_set.problem_id,
                commit_sha=commit_sha,
                affected_files=target_paths,
                audit_trail_path="",
                lease_id=lease_id,
            )
        finally:
            # Step 3: Release lease
            await self._release_lease(wb_url, wb_token, lease_id)

    def _all_patches_already_applied(self, patches: List[Dict[str, Any]]) -> bool:
        """Return True if every patch's new_text already matches the file on disk.

        Based on the LiteLLM teamMemorySync push_delta() SHA256 idempotency
        pattern.  Prevents duplicate doc22 entries across retries.
        Patches without new_text (delete-ops) are always considered unapplied.
        """
        if not patches:
            return False
        for patch in patches:
            new_text = patch.get("new_text")
            if new_text is None:
                return False  # deletion patch — cannot skip safely
            target = self.config.repo_root / patch.get("path", "")
            try:
                current = target.read_text(encoding="utf-8")
            except OSError:
                return False  # file missing — not applied
            new_sha = hashlib.sha256(new_text.encode()).hexdigest()
            cur_sha = hashlib.sha256(current.encode()).hexdigest()
            if new_sha != cur_sha:
                return False
        return True

    def _select_writeback_instance(
        self, target_paths: List[str]
    ) -> tuple[str, str]:
        """Select Writeback-A or Writeback-B based on target paths.

        Writeback-B is used for docs/core/22_* (requires triage).
        Writeback-A is used for everything else.
        """
        for path in target_paths:
            if path.startswith("docs/core/22_"):
                return self._wb_b_url, self._wb_b_token
        return self._wb_url, self._wb_token

    async def _claim_lease(
        self,
        wb_url: str,
        wb_token: str,
        round_id: str,
        target_paths: List[str],
    ) -> Tuple[str, str]:
        """Claim a lease on target paths with retry."""
        for attempt in range(MAX_RETRIES):
            try:
                import httpx
                async with httpx.AsyncClient(timeout=30, trust_env=False) as client:
                    headers = {"Authorization": f"Bearer {wb_token}"} if wb_token else {}
                    resp = await client.post(
                        f"{wb_url}/v1/lease/claim",
                        json={
                            "round_id": round_id,
                            "target_paths": target_paths,
                            "lease_seconds": 300,
                        },
                        headers=headers,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        return data.get("lease_id", ""), data.get("fencing_token", "")
                    elif resp.status_code >= 500:
                        logger.warning("Lease claim 5xx (attempt %d): %s", attempt + 1, resp.status_code)
                        await asyncio.sleep(BACKOFF_BASE ** attempt)
                        continue
                    else:
                        logger.warning("Lease claim failed: %s %s", resp.status_code, resp.text[:200])
                        return "", ""
            except Exception as exc:
                logger.warning("Lease claim error (attempt %d): %s", attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
        return "", ""

    async def _batch_preview(
        self,
        wb_url: str,
        wb_token: str,
        patches: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        items = [
            {
                "target_path": patch["path"],
                "base_sha256": patch.get("before_sha", ""),
                "patch_text": patch.get("patch_text", ""),
            }
            for patch in patches
        ]

        import httpx

        async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
            headers = {"Authorization": f"Bearer {wb_token}"} if wb_token else {}
            resp = await client.post(
                f"{wb_url}/v1/batch-preview",
                json={"items": items},
                headers=headers,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"batch_preview_failed:{resp.status_code}:{resp.text[:200]}")
            payload = resp.json()
            conflicts = [
                item
                for item in payload.get("results", [])
                if isinstance(item, dict) and bool(item.get("conflict"))
            ]
            if conflicts:
                conflict_paths = ",".join(
                    str(item.get("relative_path") or item.get("target_path") or "")
                    for item in conflicts
                )
                raise RuntimeError(f"batch_preview_conflict:{conflict_paths}")
            return payload

    async def _triage_patches(
        self,
        round_id: str,
        patch_set: PatchSet,
        verify_data: Dict[str, Any],
        preview_summary: Dict[str, Any],
        wb_url: str,
    ) -> List[str]:
        if not self._promote_prep_url:
            raise RuntimeError("writeback_triage_unavailable:promote_prep_not_configured")

        runtime_gates = verify_data.get("runtime_gates")
        if not isinstance(runtime_gates, dict):
            runtime_gates = {}

        preview_by_path: Dict[str, Dict[str, Any]] = {}
        for item in preview_summary.get("results", []):
            if not isinstance(item, dict):
                continue
            for key in (item.get("relative_path"), item.get("target_path")):
                token = str(key or "").strip()
                if token:
                    preview_by_path[token] = item

        triage_record_ids: List[str] = []
        import httpx

        async with httpx.AsyncClient(timeout=120, trust_env=False) as client:
            headers = (
                {"Authorization": f"Bearer {self._promote_prep_token}"}
                if self._promote_prep_token else {}
            )
            for patch in patch_set.patches:
                target_path = str(patch.get("path") or "").strip()
                if not target_path or target_path.startswith("__analysis__"):
                    continue
                preview_item = preview_by_path.get(target_path, {})
                resp = await client.post(
                    f"{self._promote_prep_url}/v1/triage/writeback",
                    json={
                        "run_id": round_id,
                        "workflow_id": "escort_team",
                        "layer": "code-fix",
                        "target_path": target_path,
                        "patch_text": patch.get("patch_text", ""),
                        "base_sha256": patch.get("before_sha", ""),
                        "runtime_gates": runtime_gates,
                        "audit_context": {
                            "round_id": round_id,
                            "problem_id": patch_set.problem_id,
                            "source": "escort_team",
                            "writeback_url": wb_url,
                        },
                        "preview_summary": preview_item,
                        "metadata": {
                            "round_id": round_id,
                            "problem_id": patch_set.problem_id,
                            "lane_id": patch_set.lane_id,
                        },
                    },
                    headers=headers,
                )
                if resp.status_code >= 400:
                    raise RuntimeError(f"writeback_triage_failed:{target_path}:{resp.status_code}:{resp.text[:200]}")
                triage_payload = resp.json()
                if not bool(triage_payload.get("auto_commit")):
                    reason = str(triage_payload.get("reason") or "TRIAGE_BLOCKED")
                    raise RuntimeError(f"writeback_triage_blocked:{target_path}:{reason}")
                triage_record_id = str(triage_payload.get("triage_record_id") or "").strip()
                if not triage_record_id:
                    raise RuntimeError(f"writeback_triage_missing_record:{target_path}")
                triage_record_ids.append(triage_record_id)

        return triage_record_ids

    async def _batch_commit(
        self,
        wb_url: str,
        wb_token: str,
        round_id: str,
        problem_id: str,
        patches: List[Dict[str, str]],
        lease_id: str,
        fencing_token: str,
        triage_record_ids: List[str],
    ) -> Dict[str, Any]:
        """Commit patches via the writeback service batch endpoint with retry."""
        items = []
        for p in patches:
            items.append({
                "target_path": p["path"],
                "patch_text": p.get("patch_text", ""),
                "base_sha256": p.get("before_sha", ""),
            })

        for attempt in range(MAX_RETRIES):
            try:
                import httpx
                async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
                    headers = {"Authorization": f"Bearer {wb_token}"} if wb_token else {}
                    resp = await client.post(
                        f"{wb_url}/v1/batch-commit",
                        json={
                            "items": items,
                            "idempotency_key": f"{round_id}:{problem_id}:batch",
                            "lease_id": lease_id,
                            "fencing_token": fencing_token,
                            "run_id": round_id,
                            "actor": {"type": "escort_team", "id": "writeback"},
                            "request_id": f"req-{round_id}-{problem_id}-batch",
                            "triage_record_ids": triage_record_ids,
                        },
                        headers=headers,
                    )
                    if resp.status_code == 200:
                        return resp.json()
                    elif resp.status_code >= 500:
                        logger.warning("Batch commit 5xx (attempt %d): %s", attempt + 1, resp.status_code)
                        await asyncio.sleep(BACKOFF_BASE ** attempt)
                        continue
                    else:
                        logger.warning("Batch commit failed: %s %s", resp.status_code, resp.text[:200])
                        return {"error": resp.text[:200]}
            except Exception as exc:
                logger.warning("Batch commit error (attempt %d): %s", attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
        return {"error": "max retries exceeded"}

    async def _release_lease(
        self, wb_url: str, wb_token: str, lease_id: str
    ) -> None:
        """Release the lease after writeback with retry."""
        if not lease_id:
            return
        for attempt in range(MAX_RETRIES):
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
                    headers = {"Authorization": f"Bearer {wb_token}"} if wb_token else {}
                    resp = await client.post(
                        f"{wb_url}/v1/lease/release",
                        json={"lease_id": lease_id, "reason": "writeback_complete"},
                        headers=headers,
                    )
                    if resp.status_code < 500:
                        return
                    logger.warning("Lease release 5xx (attempt %d): %s", attempt + 1, resp.status_code)
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
            except Exception:
                logger.debug("Lease release error (attempt %d, non-fatal)", attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
