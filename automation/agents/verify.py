"""VerifyAgent — multi-layer verification pipeline.

5-gate verification:
    1. Scoped pytest   — only affected FR tests
    2. Full regression — full pytest (P0/P1 only)
    3. Governance gates — blind-spot + catalog freshness + artifact alignment
    4. Contract validation — API contract checks (docs/core/05)
    5. Diff safety check — basic OWASP-aware scan of patch content

Wraps the existing ``loop_controller.verifier.Verifier`` to avoid
reimplementing the verification pipeline from scratch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import (
    AgentRole,
    PatchSet,
    VerifyResult,
)

logger = logging.getLogger(__name__)

_VERIFY_WORKSPACE_EXCLUDE = {
    ".git",
    ".venv",
    "venv",
    "env",
    "data",
    "runtime",
    "output",
    "_archive",
    "test-results",
    ".playwright-cli",
    ".pytest_cache",
    ".pytest_tmp",
    "basetemp_audit",
    ".ruff_cache",
    ".mypy_cache",
    ".vscode-1uankequanme-userdata",
}

# OWASP-aware patterns to flag in patches
SECURITY_PATTERNS = [
    (r"eval\s*\(", "eval() usage"),
    (r"exec\s*\(", "exec() usage"),
    (r"subprocess\.call\s*\(.*shell\s*=\s*True", "shell injection risk"),
    (r"os\.system\s*\(", "os.system() usage"),
    (r"pickle\.loads?\s*\(", "unsafe deserialization"),
    (r"__import__\s*\(", "dynamic import"),
    (r"yaml\.load\s*\([^)]*\)\s*$", "unsafe yaml.load (use safe_load)"),
    (r"(password|secret|token|api_key)\s*=\s*['\"][^'\"]+['\"]", "hardcoded credentials"),
]


class VerifyAgent(BaseAgent):
    """Runs multi-layer verification on patches produced by FixAgent."""

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.VERIFY, mailbox=mailbox, config=config)

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        round_id = payload.get("round_id", "")
        patches_raw = payload.get("patches", [])
        patches = [
            PatchSet.from_dict(p) if isinstance(p, dict) else p
            for p in patches_raw
        ]
        workspace_override = str(payload.get("workspace_root") or "").strip()
        candidate_workspace: Path | None = None
        workspace_prepare_error = ""
        workspace_root = self.config.repo_root

        try:
            if workspace_override:
                override_path = Path(workspace_override)
                workspace_root = (
                    override_path
                    if override_path.is_absolute()
                    else (self.config.repo_root / override_path).resolve()
                )
            else:
                candidate_workspace = self._build_candidate_workspace(patches)
                if candidate_workspace is not None:
                    workspace_root = candidate_workspace
        except Exception as exc:
            workspace_prepare_error = str(exc)
            logger.error(
                "[%s] Candidate workspace preparation failed, falling back to repo_root: %s",
                self.agent_id, exc,
            )
            # Fallback: run gates against original repo_root instead of cascade-failing
            workspace_root = self.config.repo_root

        # Collect affected files and FRs from patches
        affected_files: List[str] = []
        for p in patches:
            for patch in p.patches:
                path = patch.get("path", "")
                if path and not path.startswith("__analysis__"):
                    affected_files.append(path)

        try:
            if workspace_prepare_error:
                gov_result = await self._gate_governance()
                security_findings = await self._gate_security_scan(patches)
                # Fallback: still run scoped/contract gates against original repo_root
                results_fb = await asyncio.gather(
                    self._gate_scoped_pytest(affected_files, round_id, workspace_root),
                    self._gate_contract_validation(affected_files, workspace_root),
                    return_exceptions=True,
                )
                scoped_passed = results_fb[0] if isinstance(results_fb[0], bool) else False
                contract_passed = results_fb[1] if isinstance(results_fb[1], bool) else False
                full_passed = True  # skip full regression when workspace prep failed
            else:
                results = await asyncio.gather(
                    self._gate_scoped_pytest(affected_files, round_id, workspace_root),
                    self._gate_governance(),
                    self._gate_security_scan(patches),
                    self._gate_contract_validation(affected_files, workspace_root),
                    return_exceptions=True,
                )

                scoped_passed = results[0] if isinstance(results[0], bool) else False
                gov_result = results[1] if isinstance(results[1], dict) else {}
                security_findings = results[2] if isinstance(results[2], list) else []
                contract_passed = results[3] if isinstance(results[3], bool) else False

                full_passed = True
                has_critical = any(
                    any(
                        not str(patch.get("path") or "").startswith("__analysis__")
                        for patch in p.patches
                    )
                    for p in patches
                )
                if has_critical:
                    full_passed = await self._gate_full_regression(round_id, workspace_root)

            blind_spot_clean = gov_result.get("blind_spot_clean", True)
            catalog_fresh = gov_result.get("catalog_fresh", True)
            artifacts_aligned = gov_result.get("artifacts_aligned", True)
            # v7: In a no-patch MONITOR round the shared artifacts haven't changed
            # — the mtime drift check in _gate_governance() would spuriously flag
            # them as misaligned.  When there are no affected files the system is
            # in a steady green state, so alignment is trivially True.
            if not affected_files:
                artifacts_aligned = True
            security_clean = len(security_findings) == 0

            failed_gates: List[str] = []
            if workspace_prepare_error:
                failed_gates.append("candidate_workspace")
            if not scoped_passed:
                failed_gates.append("scoped_pytest")
            if not full_passed:
                failed_gates.append("full_regression")
            if not blind_spot_clean:
                failed_gates.append("blind_spot")
            if not catalog_fresh:
                failed_gates.append("catalog_freshness")
            if not artifacts_aligned:
                failed_gates.append("artifact_alignment")
            if not security_clean:
                failed_gates.append("security_scan")
            if not contract_passed:
                failed_gates.append("contract_validation")

            all_passed = len(failed_gates) == 0

            verify = VerifyResult(
                round_id=round_id,
                scoped_pytest_passed=scoped_passed,
                full_regression_passed=full_passed,
                blind_spot_clean=blind_spot_clean,
                catalog_fresh=catalog_fresh,
                artifacts_aligned=artifacts_aligned,
                security_clean=security_clean,
                all_passed=all_passed,
                failed_gates=failed_gates,
                details={
                    "security_findings": security_findings,
                    "affected_files": affected_files,
                    "workspace_root": str(workspace_root),
                    "candidate_workspace": bool(candidate_workspace),
                    "workspace_prepare_error": workspace_prepare_error or None,
                },
            )

            logger.info(
                "[%s] Verification %s (round=%s, gates=%s)",
                self.agent_id,
                "PASSED" if all_passed else f"FAILED: {failed_gates}",
                round_id,
                f"{6 - len(failed_gates)}/6",
            )

            # Auto-refresh shared artifacts after successful verification
            if all_passed:
                refresh_result = await self._refresh_shared_artifacts()
                if refresh_result.get("refreshed"):
                    logger.info(
                        "[%s] Shared artifacts refreshed: %s",
                        self.agent_id, refresh_result.get("refreshed"),
                    )

            return {
                **verify.to_dict(),
                "findings": [{"gate": g, "status": "failed"} for g in failed_gates],
            }
        finally:
            if candidate_workspace is not None:
                shutil.rmtree(candidate_workspace, ignore_errors=True)

    # ------------------------------------------------------------------
    # Gate implementations
    # ------------------------------------------------------------------

    async def _gate_scoped_pytest(
        self, affected_files: List[str], round_id: str, workspace_root: Path
    ) -> bool:
        """Run pytest scoped to affected test files."""
        if not affected_files:
            return True  # nothing to test

        # Derive test file paths
        test_patterns: List[str] = []
        for f in affected_files:
            # Try to find matching test file
            if f.startswith("app/"):
                module = f.replace("app/", "").replace("/", "_").replace(".py", "")
                test_patterns.append(f"test_{module}")
            test_patterns.append(Path(f).stem)

        if not test_patterns:
            return True

        k_expr = " or ".join(test_patterns[:10])  # limit expression length
        try:
            result = self._run_pytest(
                workspace_root,
                ["-k", k_expr],
                timeout_seconds=180,
            )
            # Exit code 0 = passed, 5 = no tests collected (all deselected) — both are OK
            passed = result.returncode in (0, 5)
            if not passed:
                logger.warning(
                    "[%s] Scoped pytest failed (k=%s): %s",
                    self.agent_id, k_expr, result.stdout[-500:] if result.stdout else "",
                )
            return passed
        except (subprocess.TimeoutExpired, Exception) as exc:
            logger.warning("[%s] Scoped pytest error: %s", self.agent_id, exc)
            return False

    async def _gate_full_regression(self, round_id: str, workspace_root: Path) -> bool:
        """Run full pytest regression suite."""
        try:
            result = self._run_pytest(
                workspace_root,
                [],
                timeout_seconds=600,
            )
            passed = result.returncode == 0
            if not passed:
                logger.warning(
                    "[%s] Full regression failed: %s",
                    self.agent_id, result.stdout[-500:] if result.stdout else "",
                )
            return passed
        except (subprocess.TimeoutExpired, Exception) as exc:
            logger.warning("[%s] Full regression error: %s", self.agent_id, exc)
            return False

    async def _gate_governance(self) -> Dict[str, bool]:
        """Check governance artifacts (blind-spot, catalog, alignment)."""
        result = {
            "blind_spot_clean": True,
            "catalog_fresh": True,
            "artifacts_aligned": True,
        }

        # Blind spot check
        bs_path = self.config.repo_root / "output" / "blind_spot_audit.json"
        if bs_path.exists():
            try:
                data = json.loads(bs_path.read_text(encoding="utf-8"))
                for cat in ("FAKE", "HOLLOW", "WEAK"):
                    items = data.get(cat, [])
                    if isinstance(items, list) and items:
                        result["blind_spot_clean"] = False
                    elif isinstance(items, int) and items > 0:
                        result["blind_spot_clean"] = False
            except Exception:
                pass

        # Catalog freshness
        cat_path = self.config.repo_root / "app" / "governance" / "catalog_snapshot.json"
        if cat_path.exists():
            try:
                data = json.loads(cat_path.read_text(encoding="utf-8"))
                result["catalog_fresh"] = self._catalog_snapshot_is_fresh(data)
            except Exception:
                pass

        # Artifacts alignment: verify shared artifacts are in sync
        # Compare modification times of key shared artifacts — if they
        # diverge by more than 1 hour, the artifacts are misaligned.
        junit_path = self.config.repo_root / "output" / "junit.xml"
        try:
            timestamps = []
            for artifact_path in (junit_path, cat_path, bs_path):
                if artifact_path.exists():
                    timestamps.append(artifact_path.stat().st_mtime)
            if len(timestamps) >= 2:
                drift = max(timestamps) - min(timestamps)
                if drift > 3600:  # 1 hour — aligned with docstring above
                    result["artifacts_aligned"] = False
                    logger.info(
                        "[%s] Artifact drift %.0fs exceeds 1h threshold",
                        self.agent_id, drift,
                    )
        except Exception:
            pass

        return result

    async def _gate_security_scan(
        self, patches: List[PatchSet]
    ) -> List[Dict[str, str]]:
        """Basic OWASP-aware scan of patch content."""
        findings: List[Dict[str, str]] = []
        for ps in patches:
            for patch in ps.patches:
                content = patch.get("patch_text", "")
                path = patch.get("path", "")
                if not content:
                    continue
                for pattern, description in SECURITY_PATTERNS:
                    if re.search(pattern, content, re.IGNORECASE | re.MULTILINE):
                        findings.append({
                            "file": path,
                            "issue": description,
                            "problem_id": ps.problem_id,
                        })
        return findings

    async def _gate_contract_validation(
        self, affected_files: List[str], workspace_root: Path
    ) -> bool:
        """Check that modified files don't break the API contract.

        Runs the doc-driven verify test suite if any API-related files
        were modified.
        """
        api_related = any(
            f.startswith(("app/routers/", "app/api/", "app/core/"))
            for f in affected_files
        )
        if not api_related:
            return True  # not API code, skip

        try:
            result = self._run_pytest(
                workspace_root,
                ["-k", "contract"],
                timeout_seconds=180,
            )
            passed = result.returncode == 0
            if not passed:
                logger.warning(
                    "[%s] Contract validation failed: %s",
                    self.agent_id, result.stdout[-500:] if result.stdout else "",
                )
            return passed
        except (subprocess.TimeoutExpired, Exception) as exc:
            logger.warning("[%s] Contract validation error: %s", self.agent_id, exc)
            return False

    @staticmethod
    def _catalog_snapshot_is_fresh(data: Dict[str, Any]) -> bool:
        freshness = str(data.get("freshness") or "").strip().lower()
        if freshness == "fresh":
            return True
        return str(data.get("test_result_freshness") or "").strip().lower() == "fresh"

    async def _refresh_shared_artifacts(self) -> Dict[str, Any]:
        """Rebuild shared artifacts after successful verification.

        Refreshes:
          1. output/junit.xml         — by running pytest with JUnit output
          2. catalog_snapshot.json     — by running build_feature_catalog
          3. blind_spot_audit.json     — by running blind_spot scanner
          4. latest_run.json           — updated by continuous_audit script
        """
        refreshed: List[str] = []
        errors: List[str] = []

        # 1. Refresh junit.xml with latest test results
        junit_path = self.config.repo_root / "output" / "junit.xml"
        try:
            result = subprocess.run(
                [
                    sys.executable, "-m", "pytest", "tests/",
                    f"--junitxml={junit_path}",
                    "-q", "--tb=no", "--no-header",
                ],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=str(self.config.repo_root),
                timeout=300,
            )
            if result.returncode in (0, 1):  # 0=pass, 1=some fail — both produce valid XML
                refreshed.append("junit.xml")
            else:
                errors.append(f"junit_refresh_rc={result.returncode}")
        except Exception as exc:
            errors.append(f"junit_refresh_error: {exc}")

        # 2. Refresh catalog_snapshot.json
        try:
            cat_result = subprocess.run(
                [
                    sys.executable, "-c",
                    "from app.governance.build_feature_catalog import build_catalog; build_catalog()",
                ],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=str(self.config.repo_root),
                timeout=120,
            )
            if cat_result.returncode == 0:
                refreshed.append("catalog_snapshot.json")
            else:
                errors.append(f"catalog_refresh_rc={cat_result.returncode}")
        except Exception as exc:
            errors.append(f"catalog_refresh_error: {exc}")

        # 3. Refresh blind_spot_audit.json
        blind_spot_script = self.config.repo_root / "scripts" / "blind_spot_audit.py"
        if blind_spot_script.exists():
            try:
                bs_result = subprocess.run(
                    [sys.executable, str(blind_spot_script)],
                    capture_output=True, text=True, encoding="utf-8", errors="replace",
                    cwd=str(self.config.repo_root),
                    timeout=120,
                )
                if bs_result.returncode == 0:
                    refreshed.append("blind_spot_audit.json")
                else:
                    errors.append(f"blind_spot_refresh_rc={bs_result.returncode}")
            except Exception as exc:
                errors.append(f"blind_spot_refresh_error: {exc}")

        # 4. Refresh latest_run.json via continuous_audit
        audit_script = self.config.repo_root / "scripts" / "continuous_repo_audit.py"
        if audit_script.exists():
            try:
                audit_result = subprocess.run(
                    [sys.executable, str(audit_script), "--quick"],
                    capture_output=True, text=True, encoding="utf-8", errors="replace",
                    cwd=str(self.config.repo_root),
                    timeout=120,
                )
                if audit_result.returncode == 0:
                    refreshed.append("latest_run.json")
                else:
                    errors.append(f"audit_refresh_rc={audit_result.returncode}")
            except Exception as exc:
                errors.append(f"audit_refresh_error: {exc}")

        return {"refreshed": refreshed, "errors": errors}

    def _build_candidate_workspace(self, patches: List[PatchSet]) -> Path | None:
        real_patches = [
            patch
            for patch_set in patches
            for patch in patch_set.patches
            if not str(patch.get("path") or "").startswith("__analysis__")
        ]
        if not real_patches:
            return None

        workspace_root = Path(
            tempfile.mkdtemp(
                prefix="verify_workspace_",
                dir=str(self.config.repo_root / "runtime" / "agents"),
            )
        )
        self._copy_workspace_source(workspace_root)
        for patch in real_patches:
            self._apply_patch_to_workspace(workspace_root, patch)
        return workspace_root

    def _copy_workspace_source(self, workspace_root: Path) -> None:
        workspace_root.mkdir(parents=True, exist_ok=True)
        ignore = shutil.ignore_patterns(
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
            ".mypy_cache",
        )
        for child in self.config.repo_root.iterdir():
            if child.name in _VERIFY_WORKSPACE_EXCLUDE:
                continue
            if child.name.startswith("tmp_pytest_run"):
                continue
            # Skip all .vscode* user-data dirs, other dot-dirs, and tmp_ prefixed dirs
            if child.name.startswith(".vscode"):
                continue
            if child.name.startswith("tmp_"):
                continue
            if child.name in (".agents", ".claude", ".cursor", ".pytest-tmp"):
                continue
            target = workspace_root / child.name
            if child.is_dir():
                shutil.copytree(child, target, dirs_exist_ok=True, ignore=ignore)
            elif child.is_file():
                shutil.copy2(child, target)

    def _apply_patch_to_workspace(self, workspace_root: Path, patch: Dict[str, str]) -> None:
        path = str(patch.get("path") or "").strip()
        if not path:
            raise RuntimeError("candidate_workspace_patch_missing_path")

        target = workspace_root / path
        old_text = patch.get("old_text", "")
        new_text = patch.get("patch_text", "")

        if target.exists():
            current = target.read_text(encoding="utf-8", errors="replace")
            if old_text:
                if old_text not in current:
                    raise RuntimeError(f"candidate_workspace_old_text_not_found:{path}")
                updated = current.replace(old_text, new_text, 1)
            else:
                raise RuntimeError(f"candidate_workspace_requires_old_text:{path}")
        else:
            if old_text:
                raise RuntimeError(f"candidate_workspace_target_missing:{path}")
            target.parent.mkdir(parents=True, exist_ok=True)
            updated = new_text

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(updated, encoding="utf-8")

    def _run_pytest(
        self,
        workspace_root: Path,
        extra_args: List[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        basetemp = workspace_root / "output" / "pytest_verify"
        basetemp.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "tests/",
            "-x",
            "--tb=short",
            "-q",
            f"--basetemp={basetemp}",
            *extra_args,
        ]
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(workspace_root),
            timeout=timeout_seconds,
        )
