from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from automation.loop_controller.schemas import ProblemSpec


_PROMOTE_BENIGN_SKIP_REASONS = frozenset(
    {
        "RUN_ID_ALREADY_PRESENT",
        "SEMANTIC_FINGERPRINT_ALREADY_PRESENT",
        "CURRENT_LAYER_SEMANTIC_FINGERPRINT_ALREADY_PRESENT",
        "CURRENT_LAYER_NO_CHANGE",
        "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
    }
)


class IssueMeshGateway(Protocol):
    def run_audit(self, *, mode: str, max_workers: int) -> dict[str, Any]:
        ...

    def get_runtime_context(self) -> dict[str, Any]:
        ...

    def apply_fixes(
        self,
        *,
        problems: list[ProblemSpec],
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        ...

    def verify_round(self, *, round_id: str, changed_files: list[str]) -> dict[str, Any]:
        ...

    def promote_round(
        self,
        *,
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class HttpIssueMeshGatewayConfig:
    mesh_runner_base_url: str
    mesh_runner_token: str
    promote_prep_base_url: str
    promote_prep_token: str
    writeback_a_base_url: str
    writeback_a_token: str
    app_base_url: str
    internal_token: str
    writeback_b_base_url: str = ""
    writeback_b_token: str = ""
    timeout_seconds: float = 120.0


class HttpIssueMeshGateway:
    def __init__(self, cfg: HttpIssueMeshGatewayConfig) -> None:
        self._cfg = cfg
        self._client = httpx.Client(timeout=cfg.timeout_seconds, trust_env=False)

    def _auth_header(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"} if token else {}

    def run_audit(self, *, mode: str, max_workers: int) -> dict[str, Any]:
        payload = {
            "wait_for_completion": True,
            "max_workers": max_workers,
            "audit_scope": "current-layer",
            "run_label": f"autonomous-fix-{mode}",
        }
        response = self._client.post(
            f"{self._cfg.mesh_runner_base_url.rstrip('/')}/v1/runs",
            json=payload,
            headers=self._auth_header(self._cfg.mesh_runner_token),
        )
        response.raise_for_status()
        body = response.json()
        run_id = str(body.get("run_id") or "")
        bundle = body.get("bundle")
        if not isinstance(bundle, dict):
            bundle_resp = self._client.get(
                f"{self._cfg.mesh_runner_base_url.rstrip('/')}/v1/runs/{run_id}/bundle",
                headers=self._auth_header(self._cfg.mesh_runner_token),
            )
            bundle_resp.raise_for_status()
            bundle = bundle_resp.json().get("bundle") or {}
        return {"audit_run_id": run_id, "bundle": bundle, "artifact_fingerprints": {}}

    def get_runtime_context(self) -> dict[str, Any]:
        headers = {"X-Internal-Token": self._cfg.internal_token}
        response = self._client.get(
            f"{self._cfg.app_base_url.rstrip('/')}/api/v1/internal/audit/context",
            headers=headers,
        )
        response.raise_for_status()
        body = response.json()
        if isinstance(body, dict) and isinstance(body.get("data"), dict):
            return body["data"]
        return {}

    def apply_fixes(
        self,
        *,
        problems: list[ProblemSpec],
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        lease_state = dict(lease or {})
        refresh_lease = getattr(coordinator, "refresh", None)
        lease_id = str(lease_state.get("lease_id") or "") or None
        fence_token = lease_state.get("fencing_token")
        if problems and (lease_id is None or fence_token is None):
            return [
                {
                    "problem_id": problem.problem_id,
                    "outcome": "failed",
                    "patches_applied": [],
                    "error": "WRITEBACK_LEASE_REQUIRED",
                }
                for problem in problems
            ]
        runtime_gates = runtime_context.get("runtime_gates") or {}
        audit_context = {
            **runtime_context,
            "round_id": round_id,
            "source": "autonomous_fix_loop",
        }
        request_payload = {
            "source_run_id": audit_run_id,
            "fix_run_id": round_id,
            "max_fix_items": len(problems),
            "runtime_gates": runtime_gates,
            "audit_context": audit_context,
        }
        response = self._client.post(
            f"{self._cfg.promote_prep_base_url.rstrip('/')}/v1/triage/synthesize-patches",
            json=request_payload,
            headers=self._auth_header(self._cfg.promote_prep_token),
        )
        response.raise_for_status()
        body = response.json()
        patches = list(body.get("patches") or [])
        results: list[dict[str, Any]] = []
        for patch in patches:
            if not patch.get("valid"):
                results.append(
                    {
                        "problem_id": str(patch.get("issue_key") or "unknown"),
                        "outcome": "skipped",
                        "patches_applied": [],
                        "error": "INVALID_PATCH",
                    }
                )
                continue
            target_path = str(patch.get("target_path") or "")
            patch_text = str(patch.get("patch_text") or "")
            base_sha256 = str(patch.get("base_sha256") or "")
            problem_id = str(patch.get("issue_key") or patch.get("problem_id") or "unknown")
            preview = self._client.post(
                f"{self._cfg.writeback_a_base_url.rstrip('/')}/v1/preview",
                json={
                    "target_path": target_path,
                    "base_sha256": base_sha256,
                    "patch_text": patch_text,
                },
                headers=self._auth_header(self._cfg.writeback_a_token),
            )
            if preview.status_code >= 400:
                results.append(
                    {
                        "problem_id": str(patch.get("issue_key") or "unknown"),
                        "outcome": "failed",
                        "patches_applied": [],
                        "error": f"PREVIEW_FAILED:{preview.status_code}",
                    }
                )
                continue
            preview_body = preview.json()
            if bool(preview_body.get("conflict")):
                results.append(
                    {
                        "problem_id": problem_id,
                        "outcome": "failed",
                        "patches_applied": [],
                        "error": "PREVIEW_CONFLICT",
                    }
                )
                continue
            triage = self._client.post(
                f"{self._cfg.promote_prep_base_url.rstrip('/')}/v1/triage/writeback",
                json={
                    "run_id": round_id,
                    "workflow_id": "autonomous_fix_loop",
                    "layer": "code-fix",
                    "target_path": target_path,
                    "patch_text": patch_text,
                    "base_sha256": base_sha256,
                    "runtime_gates": runtime_gates,
                    "audit_context": {
                        **audit_context,
                        "problem_id": problem_id,
                    },
                    "preview_summary": preview_body,
                    "metadata": {
                        "round_id": round_id,
                        "problem_id": problem_id,
                    },
                },
                headers=self._auth_header(self._cfg.promote_prep_token),
            )
            if triage.status_code >= 400:
                results.append(
                    {
                        "problem_id": problem_id,
                        "outcome": "failed",
                        "patches_applied": [],
                        "error": f"TRIAGE_FAILED:{triage.status_code}",
                    }
                )
                continue
            triage_body = triage.json()
            if not bool(triage_body.get("auto_commit")):
                results.append(
                    {
                        "problem_id": problem_id,
                        "outcome": "skipped",
                        "patches_applied": [],
                        "error": f"TRIAGE_BLOCKED:{triage_body.get('reason')}",
                    }
                )
                continue
            if callable(refresh_lease):
                refreshed_lease = refresh_lease(lease=lease_state)
                if not isinstance(refreshed_lease, dict):
                    results.append(
                        {
                            "problem_id": problem_id,
                            "outcome": "failed",
                            "patches_applied": [],
                            "error": "WRITEBACK_LEASE_REFRESH_FAILED",
                        }
                    )
                    continue
                lease_state = {**lease_state, **refreshed_lease}
                lease_id = str(lease_state.get("lease_id") or "") or None
                fence_token = lease_state.get("fencing_token")
                if lease_id is None or fence_token is None:
                    results.append(
                        {
                            "problem_id": problem_id,
                            "outcome": "failed",
                            "patches_applied": [],
                            "error": "WRITEBACK_LEASE_REFRESH_FAILED",
                        }
                    )
                    continue
            commit_json: dict[str, Any] = {
                "target_path": target_path,
                "base_sha256": base_sha256,
                "patch_text": patch_text,
                "idempotency_key": f"code-fix:{round_id}:{target_path}",
                "actor": {"type": "autonomous_fix_loop", "id": "app-service"},
                "request_id": f"req-{round_id}-{target_path.replace('/', '-')}",
                "run_id": round_id,
                "triage_record_id": triage_body.get("triage_record_id"),
            }
            if lease_id is not None and fence_token is not None:
                commit_json["lease_id"] = lease_id
                commit_json["fencing_token"] = fence_token
            commit = self._client.post(
                f"{self._cfg.writeback_a_base_url.rstrip('/')}/v1/commit",
                json=commit_json,
                headers=self._auth_header(self._cfg.writeback_a_token),
            )
            if commit.status_code >= 400:
                results.append(
                    {
                        "problem_id": str(patch.get("issue_key") or "unknown"),
                        "outcome": "failed",
                        "patches_applied": [],
                        "error": f"COMMIT_FAILED:{commit.status_code}",
                    }
                )
                continue
            results.append(
                {
                    "problem_id": str(patch.get("issue_key") or "unknown"),
                    "outcome": "success",
                    "patches_applied": [target_path],
                    "patches_raw": [patch],
                }
            )
        return results

    def verify_round(self, *, round_id: str, changed_files: list[str]) -> dict[str, Any]:
        response = self._client.post(
            f"{self._cfg.promote_prep_base_url.rstrip('/')}/v1/triage/scoped-pytest",
            json={
                "fix_run_id": round_id,
                "changed_files": changed_files,
                "timeout_seconds": 120,
            },
            headers=self._auth_header(self._cfg.promote_prep_token),
        )
        response.raise_for_status()
        body = response.json()
        return {"all_green": bool(body.get("passed", False)), "scoped_pytest": body}

    def promote_round(
        self,
        *,
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        runtime_gates = runtime_context.get("runtime_gates") or {}
        shared_promote = runtime_gates.get("shared_artifact_promote") or {}
        if not self._cfg.writeback_b_base_url:
            return {"promoted": False, "reason": "WRITEBACK_B_NOT_CONFIGURED"}

        promoted_any = False
        skip_reasons: list[str] = []
        result = self._promote_one(
            promote_type="status-note",
            round_id=round_id,
            audit_run_id=audit_run_id,
            runtime_context=runtime_context,
            coordinator=coordinator,
            lease_seconds=lease_seconds,
        )
        if result.get("error"):
            return {"promoted": False, "reason": result["error"]}
        if result.get("committed"):
            promoted_any = True
        elif result.get("skipped"):
            skip_reason = str(result.get("reason") or "").strip()
            if skip_reason:
                skip_reasons.append(skip_reason)

        if not bool(shared_promote.get("allowed")):
            return {
                "promoted": False,
                "reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
                "status_note_committed": promoted_any,
            }

        result = self._promote_one(
            promote_type="current-layer",
            round_id=round_id,
            audit_run_id=audit_run_id,
            runtime_context=runtime_context,
            coordinator=coordinator,
            lease_seconds=lease_seconds,
        )
        if result.get("error"):
            return {"promoted": False, "reason": result["error"]}
        if result.get("committed"):
            promoted_any = True
        elif result.get("skipped"):
            skip_reason = str(result.get("reason") or "").strip()
            if skip_reason:
                skip_reasons.append(skip_reason)

        if promoted_any:
            return {"promoted": True, "reason": "READY"}

        non_benign = [reason for reason in skip_reasons if reason not in _PROMOTE_BENIGN_SKIP_REASONS]
        if non_benign:
            return {"promoted": False, "reason": non_benign[0]}

        return {"promoted": True, "reason": "READY_NOOP"}

    def _promote_one(
        self,
        *,
        promote_type: str,
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Execute a single promote sub-flow (status-note or current-layer)."""
        prepare_payload = {
            "run_id": audit_run_id,
            "runtime_gates": runtime_context.get("runtime_gates") or {},
            "audit_context": {
                **runtime_context,
                "round_id": round_id,
                "source": "autonomous_fix_loop",
            },
        }
        if promote_type == "current-layer":
            prepare_payload["enabled"] = True
        # Step 1: prepare payload from promote_prep
        prep_resp = self._client.post(
            f"{self._cfg.promote_prep_base_url.rstrip('/')}/v1/promote/{promote_type}",
            json=prepare_payload,
            headers=self._auth_header(self._cfg.promote_prep_token),
        )
        if prep_resp.status_code >= 400:
            return {"error": f"PROMOTE_PREPARE_FAILED:{promote_type}:{prep_resp.status_code}"}

        prep = prep_resp.json()
        if prep.get("skip_commit"):
            return {
                "skipped": True,
                "committed": False,
                "reason": str(prep.get("skip_reason") or "").strip() or "PREPARE_SKIP",
            }

        target_path = str(prep.get("target_path") or "")
        patch_text = str(prep.get("patch_text") or "")
        base_sha256 = str(prep.get("base_sha256") or "")
        claim = getattr(coordinator, "claim", None)
        refresh = getattr(coordinator, "refresh", None)
        assert_submit_allowed = getattr(coordinator, "assert_submit_allowed", None)
        release = getattr(coordinator, "release", None)
        promote_lease: dict[str, Any] | None = None
        if callable(claim):
            try:
                promote_lease = claim(
                    round_id=f"{round_id}:{promote_type}",
                    target_paths=[target_path],
                    lease_seconds=max(30, int(lease_seconds or 120)),
                )
            except Exception as exc:
                return {"error": f"PROMOTE_LEASE_CLAIM_FAILED:{promote_type}:{exc}"}
            if not isinstance(promote_lease, dict):
                return {"error": f"PROMOTE_LEASE_REQUIRED:{promote_type}"}
            if not str(promote_lease.get("lease_id") or "").strip() or promote_lease.get("fencing_token") is None:
                return {"error": f"PROMOTE_LEASE_REQUIRED:{promote_type}"}

        try:
            # Step 2: AI triage risk analysis
            triage_resp = self._client.post(
                f"{self._cfg.promote_prep_base_url.rstrip('/')}/v1/triage",
                json={
                    "run_id": str(prep.get("run_id") or audit_run_id),
                    "layer": str(prep.get("layer") or promote_type),
                    "target_path": target_path,
                    "target_anchor": prep.get("target_anchor"),
                    "patch_text": patch_text,
                    "base_sha256": base_sha256,
                    "runtime_gates": runtime_context.get("runtime_gates") or {},
                    "audit_context": {
                        **runtime_context,
                        "round_id": round_id,
                        "source": "autonomous_fix_loop",
                    },
                    "semantic_fingerprint": prep.get("semantic_fingerprint"),
                },
                headers=self._auth_header(self._cfg.promote_prep_token),
            )
            if triage_resp.status_code >= 400:
                return {"error": f"TRIAGE_FAILED:{promote_type}:{triage_resp.status_code}"}

            triage = triage_resp.json()
            if not triage.get("auto_commit"):
                return {
                    "skipped": True,
                    "committed": False,
                    "reason": str(triage.get("reason") or "TRIAGE_REJECTED"),
                }

            triage_record_id = str(triage.get("triage_record_id") or "")

            # Step 3: preview via writeback_b
            preview_resp = self._client.post(
                f"{self._cfg.writeback_b_base_url.rstrip('/')}/v1/preview",
                json={
                    "target_path": target_path,
                    "base_sha256": base_sha256,
                    "patch_text": patch_text,
                },
                headers=self._auth_header(self._cfg.writeback_b_token),
            )
            if preview_resp.status_code >= 400:
                return {"error": f"PREVIEW_B_FAILED:{promote_type}:{preview_resp.status_code}"}
            preview_payload = preview_resp.json()
            if bool(preview_payload.get("conflict")):
                return {"skipped": True, "committed": False, "reason": "PREVIEW_CONFLICT"}

            if promote_lease is not None and callable(refresh):
                try:
                    refreshed_lease = refresh(lease=promote_lease)
                except Exception as exc:
                    return {"error": f"PROMOTE_LEASE_REFRESH_FAILED:{promote_type}:{exc}"}
                if not isinstance(refreshed_lease, dict):
                    return {"error": f"PROMOTE_LEASE_REFRESH_FAILED:{promote_type}"}
                promote_lease = {**promote_lease, **refreshed_lease}
            if promote_lease is not None and callable(assert_submit_allowed):
                try:
                    assert_submit_allowed(
                        str(promote_lease.get("lease_id") or ""),
                        int(promote_lease.get("fencing_token")),
                        [target_path],
                    )
                except Exception as exc:
                    return {"error": f"PROMOTE_LEASE_CHECK_FAILED:{promote_type}:{exc}"}

            # Step 4: commit via writeback_b
            commit_json = {
                "target_path": target_path,
                "base_sha256": base_sha256,
                "patch_text": patch_text,
                "idempotency_key": str(prep.get("idempotency_key") or f"promote-{audit_run_id}-{promote_type}"),
                "actor": {"type": "autonomous_fix_loop", "id": "promote"},
                "request_id": str(prep.get("request_id") or f"promote-{audit_run_id}-{promote_type}"),
                "run_id": str(prep.get("run_id") or audit_run_id),
                "triage_record_id": triage_record_id,
            }
            if promote_lease is not None:
                commit_json["lease_id"] = str(promote_lease.get("lease_id") or "")
                commit_json["fencing_token"] = promote_lease.get("fencing_token")
            commit_resp = self._client.post(
                f"{self._cfg.writeback_b_base_url.rstrip('/')}/v1/commit",
                json=commit_json,
                headers=self._auth_header(self._cfg.writeback_b_token),
            )
            if commit_resp.status_code >= 400:
                return {"error": f"COMMIT_B_FAILED:{promote_type}:{commit_resp.status_code}"}
            commit_payload = commit_resp.json()
            if str(commit_payload.get("status") or "").lower() == "committed" or bool(commit_payload.get("idempotent_replay")):
                return {"committed": True, "commit_id": commit_payload.get("commit_id")}
            return {"error": f"COMMIT_B_INVALID:{promote_type}"}
        finally:
            if promote_lease is not None and callable(release):
                release(lease=promote_lease, reason=f"promote_{promote_type}_finished")

    def rollback_commits(self, *, commit_ids: list[str], round_id: str) -> list[dict[str, Any]]:
        """Rollback previously committed patches via writeback-A."""
        results: list[dict[str, Any]] = []
        for commit_id in reversed(commit_ids):
            if not commit_id:
                continue
            try:
                resp = self._client.post(
                    f"{self._cfg.writeback_a_base_url.rstrip('/')}/v1/rollback",
                    json={
                        "commit_id": commit_id,
                        "idempotency_key": f"rollback:{round_id}:{commit_id}",
                        "actor": {"type": "autonomous_fix_loop", "id": "rollback"},
                        "request_id": f"rollback-{round_id}-{commit_id}",
                        "run_id": round_id,
                    },
                    headers=self._auth_header(self._cfg.writeback_a_token),
                )
                results.append({
                    "commit_id": commit_id,
                    "status_code": resp.status_code,
                    "rolled_back": resp.status_code == 200,
                })
            except Exception:
                results.append({
                    "commit_id": commit_id,
                    "rolled_back": False,
                    "error": "ROLLBACK_NETWORK_ERROR",
                })
        return results
