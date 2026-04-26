"""DiscoveryAgent — multi-source problem detection.

Probes multiple data sources in parallel (fan-out), deduplicates and
prioritises findings, then emits ``ProblemSpec[]`` to the Coordinator.

Probe registry:
    AuditProbe          ← continuous_audit/latest_run.json
    TestFailureProbe    ← output/junit.xml
    BlindSpotProbe      ← output/blind_spot_audit.json
    CatalogDriftProbe   ← app/governance/catalog_snapshot.json
    CodeChangeDriftProbe← git diff (new)
    RuntimeHealthProbe  ← runtime health check (new)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import AgentRole, ProblemSpec, Severity
from .protocol import HandlingPath, ProblemStatus
from .doc25_probe import Doc25AngleProbe

logger = logging.getLogger(__name__)

_GOV_REGISTRY_SCOPE = [
    "app/governance/feature_registry.json",
    "scripts/continuous_repo_audit.py",
    "automation/mesh_runner/**",
    "github/automation/continuous_audit/**",
]

_GOV_MAPPING_SCOPE = [
    "app/governance/build_feature_catalog.py",
    "automation/loop_controller/**",
    "automation/promote_prep/**",
    "tests/test_loop_controller.py",
    "tests/test_promote_prep_service.py",
    "tests/test_mesh_runner_manifest_builder.py",
]


# ---------------------------------------------------------------------------
# Probe interface
# ---------------------------------------------------------------------------

class Probe(ABC):
    """Base class for all discovery probes."""

    name: str = "base"

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root


class ProbeError(Exception):
    """Wraps a probe failure with the probe name for diagnostics."""

    def __init__(self, probe_name: str, detail: str = ""):
        self.probe_name = probe_name
        super().__init__(f"Probe {probe_name}: {detail}")


# ---------------------------------------------------------------------------
# Probe implementations
# ---------------------------------------------------------------------------

class AuditProbe(Probe):
    """Read continuous audit latest_run.json for open findings."""

    name = "audit"

    async def scan(self) -> List[ProblemSpec]:
        path = self.repo_root / "github" / "automation" / "continuous_audit" / "latest_run.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            findings = data.get("findings", [])
            problems: List[ProblemSpec] = []
            for f in findings:
                problems.append(ProblemSpec(
                    problem_id=f.get("id", f.get("family", "unknown")),
                    source_probe=self.name,
                    severity=f.get("risk_level", Severity.P2.value),
                    family=f.get("family", ""),
                    title=f.get("title", f.get("summary", "")),
                    description=f.get("detail", ""),
                    affected_files=f.get("affected_files", []),
                    affected_frs=f.get("affected_frs", []),
                    suggested_approach=f.get("handling_path", "fix_code"),
                ))
            return problems
        except Exception as exc:
            logger.exception("AuditProbe scan error")
            raise ProbeError(self.name, f"scan failed: {exc}") from exc


class TestFailureProbe(Probe):
    """Parse junit.xml for test failures."""

    name = "test_failure"

    async def scan(self) -> List[ProblemSpec]:
        path = self.repo_root / "output" / "junit.xml"
        if not path.exists():
            return []
        try:
            tree = ET.parse(str(path))
            root = tree.getroot()
            problems: List[ProblemSpec] = []
            for tc in root.iter("testcase"):
                failure = tc.find("failure")
                error = tc.find("error")
                if failure is not None or error is not None:
                    name = tc.get("name", "unknown")
                    classname = tc.get("classname", "")
                    msg = ""
                    if failure is not None:
                        msg = failure.get("message", "")
                    elif error is not None:
                        msg = error.get("message", "")
                    problems.append(ProblemSpec(
                        problem_id=f"test-{hashlib.md5(f'{classname}.{name}'.encode()).hexdigest()[:8]}",
                        source_probe=self.name,
                        severity=Severity.P1.value,
                        title=f"Test failure: {classname}.{name}",
                        description=msg[:500],
                        affected_frs=self._extract_frs(name, classname),
                        suggested_approach="fix_code",
                    ))
            return problems
        except Exception as exc:
            logger.exception("TestFailureProbe scan error")
            raise ProbeError(self.name, f"scan failed: {exc}") from exc

    @staticmethod
    def _extract_frs(name: str, classname: str) -> List[str]:
        """Heuristic: extract FR tags from test name."""
        frs: List[str] = []
        combined = f"{classname}.{name}".lower()
        for i in range(1, 20):
            tag = f"fr{i:02d}"
            if tag in combined:
                frs.append(tag.upper())
        return frs


class BlindSpotProbe(Probe):
    """Check blind_spot_audit.json for coverage gaps."""

    name = "blind_spot"

    async def scan(self) -> List[ProblemSpec]:
        path = self.repo_root / "output" / "blind_spot_audit.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            problems: List[ProblemSpec] = []
            for category in ("FAKE", "HOLLOW", "WEAK"):
                items = data.get(category, [])
                if isinstance(items, list) and items:
                    for item in items:
                        item_str = item if isinstance(item, str) else json.dumps(item)
                        problems.append(ProblemSpec(
                            problem_id=f"blind-{category.lower()}-{hashlib.md5(item_str.encode()).hexdigest()[:8]}",
                            source_probe=self.name,
                            severity=Severity.P2.value,
                            title=f"Blind spot [{category}]: {item_str[:80]}",
                            description=item_str,
                            suggested_approach="fix_code",
                        ))
                elif isinstance(items, int) and items > 0:
                    problems.append(ProblemSpec(
                        problem_id=f"blind-{category.lower()}-count",
                        source_probe=self.name,
                        severity=Severity.P2.value,
                        title=f"Blind spot [{category}]: {items} items",
                        suggested_approach="fix_code",
                    ))
            return problems
        except Exception as exc:
            logger.exception("BlindSpotProbe scan error")
            raise ProbeError(self.name, f"scan failed: {exc}") from exc


class CatalogDriftProbe(Probe):
    """Detect stale or drifted feature catalog entries — v2: auto-rebuild."""

    name = "catalog_drift"

    @staticmethod
    def _catalog_freshness_label(data: Dict[str, Any]) -> str:
        freshness = str(data.get("freshness") or "").strip().lower()
        if freshness:
            return freshness
        junit_freshness = str(data.get("test_result_freshness") or "").strip().lower()
        if junit_freshness:
            return junit_freshness
        return "unknown"

    @classmethod
    def _catalog_is_fresh(cls, data: Dict[str, Any]) -> bool:
        return cls._catalog_freshness_label(data) == "fresh"

    async def scan(self) -> List[ProblemSpec]:
        path = self.repo_root / "app" / "governance" / "catalog_snapshot.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            freshness = self._catalog_freshness_label(data)
            if self._catalog_is_fresh(data):
                return []

            # v2: attempt auto-rebuild before reporting
            rebuilt = await self._attempt_rebuild()
            if rebuilt:
                # Re-check after rebuild
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    freshness = self._catalog_freshness_label(data)
                    if self._catalog_is_fresh(data):
                        logger.info("[catalog_drift] Auto-rebuild succeeded, catalog now fresh")
                        return []
                except Exception:
                    pass

            return [ProblemSpec(
                problem_id="catalog-stale",
                source_probe=self.name,
                severity=Severity.P2.value,
                title=f"Feature catalog is {freshness}",
                description=f"Catalog freshness: {freshness} (auto-rebuild {'attempted' if rebuilt else 'skipped'})",
                suggested_approach="execution_and_monitoring",
            )]
        except Exception as exc:
            logger.exception("CatalogDriftProbe scan error")
            raise ProbeError(self.name, f"scan failed: {exc}") from exc

    # v5: configurable rebuild timeout (was hardcoded 60s, too short for large repos)
    REBUILD_TIMEOUT_SECONDS = int(os.environ.get("CATALOG_REBUILD_TIMEOUT", "120"))

    async def _attempt_rebuild(self) -> bool:
        """Try to rebuild the catalog in a subprocess."""
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-c",
                "from app.governance.build_feature_catalog import build_catalog; build_catalog()",
                cwd=str(self.repo_root),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=self.REBUILD_TIMEOUT_SECONDS)
            if proc.returncode == 0:
                return True
            logger.warning("Catalog rebuild failed (rc=%d)", proc.returncode)
        except asyncio.TimeoutError:
            logger.warning("Catalog rebuild timed out (%ds)", self.REBUILD_TIMEOUT_SECONDS)
            if proc is not None:
                pid = getattr(proc, "pid", None)
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                if os.name == "nt" and isinstance(pid, int) and pid > 0:
                    try:
                        killer = await asyncio.create_subprocess_exec(
                            "taskkill",
                            "/PID",
                            str(pid),
                            "/T",
                            "/F",
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        await asyncio.wait_for(killer.wait(), timeout=5)
                    except Exception:
                        pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    pass
        except Exception:
            logger.debug("Catalog rebuild not available", exc_info=True)
        return False


class CodeChangeDriftProbe(Probe):
    """Detect uncommitted code changes that might indicate drift."""

    name = "code_change"

    async def scan(self) -> List[ProblemSpec]:
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", "--diff-filter=M"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=str(self.repo_root),
                timeout=30,
            )
            if result.returncode != 0:
                return []
            changed = [
                f for f in result.stdout.strip().split("\n")
                if f and (f.startswith("app/") or f.startswith("automation/"))
            ]
            if not changed:
                return []
            return [ProblemSpec(
                problem_id="code-drift",
                source_probe=self.name,
                severity=Severity.P3.value,
                title=f"Uncommitted changes in {len(changed)} files",
                description="\n".join(changed[:20]),
                affected_files=changed[:50],
                suggested_approach="manual_verify",
                current_status=ProblemStatus.REVIEW_REQUIRED.value,
                lane_id="repo_hygiene",
                task_family="repo-governance",
            )]
        except Exception:
            logger.debug("CodeChangeDriftProbe: git not available or error")
            return []


class RuntimeHealthProbe(Probe):
    """Check runtime service health indicators."""

    name = "runtime_health"

    async def scan(self) -> List[ProblemSpec]:
        state_path = self.repo_root / "runtime" / "loop_controller" / "state.json"
        if not state_path.exists():
            return []
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            problems: List[ProblemSpec] = []
            phase = data.get("phase", "")
            if phase == "BLOCKED":
                problems.append(ProblemSpec(
                    problem_id="runtime-blocked",
                    source_probe=self.name,
                    severity=Severity.P0.value,
                    title="Loop controller is BLOCKED",
                    description=f"Phase: {phase}, mode: {data.get('mode', 'unknown')}",
                    suggested_approach="fix_code",
                ))
            return problems
        except Exception:
            logger.debug("RuntimeHealthProbe: error reading state")
            return []


class ProviderHealthProbe(Probe):
    """Check AI provider (New API) readiness via 2-stage HTTP probe.

    Stage 1: GET /v1/models — verify at least one model available.
    Stage 2: POST /v1/chat/completions — smoke test with minimal input.

    Reference: loop_controller/controller.py ``_check_provider_readiness()``.
    """

    name = "provider_health"

    def __init__(self, repo_root: Path, *, new_api_url: str = "", new_api_token: str = ""):
        super().__init__(repo_root)
        self._url = new_api_url.rstrip("/") if new_api_url else ""
        self._token = new_api_token

    async def scan(self) -> List[ProblemSpec]:
        if not self._url:
            return []
        try:
            import httpx
        except ImportError:
            logger.debug("ProviderHealthProbe: httpx not installed")
            return []

        headers: Dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        try:
            async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
                # Stage 1: model list
                resp = await client.get(f"{self._url}/v1/models", headers=headers)
                if resp.status_code != 200:
                    return [self._problem(
                        f"Provider returned {resp.status_code} on /v1/models",
                        stage="models_list",
                    )]
                body = resp.json()
                models = body.get("data", [])
                if not models:
                    return [self._problem(
                        "Provider has no models available",
                        stage="models_empty",
                    )]

                # Stage 2: smoke test via chat completions
                first_model = models[0].get("id", "gpt-4")
                resp2 = await client.post(
                    f"{self._url}/v1/chat/completions",
                    json={
                        "model": first_model,
                        "messages": [{"role": "user", "content": "ping"}],
                        "max_tokens": 1,
                        "stream": False,
                    },
                    headers=headers,
                    timeout=30,
                )
                if resp2.status_code >= 500:
                    return [self._problem(
                        f"Provider smoke test returned {resp2.status_code}",
                        stage="smoke_test",
                    )]
                # Check if upstream returned valid content (non-empty choices)
                try:
                    body2 = resp2.json() if "json" in resp2.headers.get("content-type", "") else {}
                    choices = body2.get("choices", [])
                    if resp2.status_code == 200 and not choices:
                        # Gateway reachable but upstream channels returning empty
                        # — downgrade from P0 to P2 (gateway is up, upstreams degraded)
                        return [ProblemSpec(
                            problem_id="provider-upstream_degraded",
                            source_probe=self.name,
                            severity=Severity.P2.value,
                            family="infrastructure",
                            task_family="runtime-external",
                            lane_id="runtime_blocked",
                            title="AI Provider upstream channels degraded",
                            description="Gateway reachable and models listed, but upstream channels return empty completions",
                            suggested_approach="execution_and_monitoring",
                            current_status=ProblemStatus.BLOCKED.value,
                            blocker_type="external_dependency",
                            blocked_reason="AI gateway upstream channels temporarily returning empty responses",
                        )]
                except Exception:
                    pass

            return []
        except httpx.TimeoutException:
            return [self._problem("Provider connection timed out", stage="timeout")]
        except httpx.ConnectError:
            return [self._problem("Provider connection refused", stage="connect_error")]
        except Exception as exc:
            logger.debug("ProviderHealthProbe error: %s", exc)
            return [self._problem(f"Provider check error: {exc}", stage="error")]

    def _problem(self, description: str, stage: str = "") -> ProblemSpec:
        return ProblemSpec(
            problem_id=f"provider-{stage or 'down'}",
            source_probe=self.name,
            severity=Severity.P0.value,
            family="infrastructure",
            task_family="runtime-external",
            lane_id="runtime_blocked",
            title="AI Provider is not ready",
            description=description,
            suggested_approach="execution_and_monitoring",
            current_status=ProblemStatus.BLOCKED.value,
            blocker_type="external_dependency",
            blocked_reason="AI gateway temporarily unavailable or overloaded",
        )


class MeshAuditProbe(Probe):
    """Trigger a mesh audit run and convert findings to ProblemSpecs.

    3-step: POST /v1/runs → poll GET /v1/runs/{id} → GET /v1/runs/{id}/bundle.

    Reference: loop_controller/controller.py ``_do_audit()``.
    """

    name = "mesh_audit"
    POLL_INTERVAL = 5         # seconds between status polls
    MAX_POLL_SECONDS = 90     # Must fit within coordinator discovery timeout (120s)

    def __init__(self, repo_root: Path, *, mesh_runner_url: str = "", mesh_runner_token: str = ""):
        super().__init__(repo_root)
        self._url = mesh_runner_url.rstrip("/") if mesh_runner_url else ""
        self._token = mesh_runner_token

    async def scan(self) -> List[ProblemSpec]:
        if not self._url:
            return []
        try:
            import httpx
        except ImportError:
            logger.debug("MeshAuditProbe: httpx not installed")
            return []

        headers: Dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        try:
            async with httpx.AsyncClient(timeout=30, trust_env=False) as client:
                # Step 1: start audit run
                resp = await client.post(
                    f"{self._url}/v1/runs",
                    json={
                        "run_label": "escort-discovery",
                        "audit_scope": "current-layer",
                        "audit_context": {"source": "escort_team", "mode": "discovery"},
                    },
                    headers=headers,
                )
                if resp.status_code not in (200, 201):
                    logger.warning("MeshAuditProbe: POST /v1/runs returned %d", resp.status_code)
                    return []
                run_data = resp.json()
                run_id = run_data.get("run_id") or run_data.get("id", "")
                if not run_id:
                    return []

                # Step 2: poll until completion
                elapsed = 0
                while elapsed < self.MAX_POLL_SECONDS:
                    await asyncio.sleep(self.POLL_INTERVAL)
                    elapsed += self.POLL_INTERVAL
                    status_resp = await client.get(
                        f"{self._url}/v1/runs/{run_id}", headers=headers,
                    )
                    if status_resp.status_code != 200:
                        continue
                    status_data = status_resp.json()
                    run_status = status_data.get("status", "")
                    if run_status in ("completed", "partial", "failed"):
                        break
                else:
                    logger.warning("MeshAuditProbe: run %s timed out after %ds", run_id, elapsed)
                    return []

                # Step 3: fetch bundle
                bundle_resp = await client.get(
                    f"{self._url}/v1/runs/{run_id}/bundle", headers=headers,
                )
                if bundle_resp.status_code != 200:
                    return []
                bundle = bundle_resp.json()

            return self._bundle_to_problems(bundle)
        except httpx.TimeoutException:
            logger.warning("MeshAuditProbe: connection timed out")
            return []
        except httpx.ConnectError:
            logger.warning("MeshAuditProbe: mesh_runner connection refused")
            return []
        except Exception:
            logger.exception("MeshAuditProbe scan error")
            return []

    @staticmethod
    def _bundle_to_problems(bundle: Dict[str, Any]) -> List[ProblemSpec]:
        """Convert mesh audit bundle findings into ProblemSpecs."""
        findings = bundle.get("findings", [])
        problems: List[ProblemSpec] = []
        for f in findings:
            problems.append(ProblemSpec(
                problem_id=f.get("id", f.get("family", f"mesh-{len(problems)}")),
                source_probe="mesh_audit",
                severity=f.get("risk_level", Severity.P2.value),
                family=f.get("family", ""),
                title=f.get("title", f.get("summary", "")),
                description=f.get("detail", ""),
                affected_files=f.get("affected_files", []),
                affected_frs=f.get("affected_frs", []),
                suggested_approach=f.get("handling_path", "fix_code"),
                write_scope=f.get("write_scope", []),
            ))
        return problems


# ---------------------------------------------------------------------------
# Probe Registry
# ---------------------------------------------------------------------------

ALL_PROBES = [
    AuditProbe,
    TestFailureProbe,
    BlindSpotProbe,
    CatalogDriftProbe,
    CodeChangeDriftProbe,
    RuntimeHealthProbe,
    ProviderHealthProbe,
    MeshAuditProbe,
    Doc25AngleProbe,
]


# ---------------------------------------------------------------------------
# DiscoveryAgent
# ---------------------------------------------------------------------------

class DiscoveryAgent(BaseAgent):
    """Runs all probes in parallel, deduplicates, and returns ProblemSpec[]."""

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.DISCOVERY, mailbox=mailbox, config=config)
        urls = self.config.service_urls
        tokens = self.config.service_tokens
        self._probes: List[Probe] = [
            cls(self.config.repo_root)
            for cls in ALL_PROBES
            if cls not in (ProviderHealthProbe, MeshAuditProbe, Doc25AngleProbe)
        ]
        # HTTP-based probes need service URLs
        self._probes.append(ProviderHealthProbe(
            self.config.repo_root,
            new_api_url=urls.get("new_api", ""),
            new_api_token=tokens.get("new_api", ""),
        ))
        self._probes.append(MeshAuditProbe(
            self.config.repo_root,
            mesh_runner_url=urls.get("mesh_runner", ""),
            mesh_runner_token=tokens.get("mesh_runner", ""),
        ))
        # Doc25 probe only needs repo_root
        self._probes.append(Doc25AngleProbe(self.config.repo_root))

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Fan-out all probes in parallel, deduplicate, and return findings."""
        mode = payload.get("mode", "full")

        if mode == "incremental":
            # Only run fast probes
            probes = [p for p in self._probes if p.name in ("test_failure", "runtime_health")]
        else:
            probes = self._probes

        # Parallel fan-out (like claude-code-sourcemap's concurrent worker launch)
        results = await asyncio.gather(
            *(self._run_probe(p) for p in probes),
            return_exceptions=True,
        )

        all_problems: List[ProblemSpec] = []
        probe_errors = 0
        failed_probes: List[str] = []
        for i, r in enumerate(results):
            if isinstance(r, list):
                all_problems.extend(r)
            elif isinstance(r, Exception):
                probe_errors += 1
                probe_name = "unknown"
                if isinstance(r, ProbeError):
                    probe_name = r.probe_name
                    failed_probes.append(r.probe_name)
                else:
                    probe_name = probes[i].name if i < len(probes) else "unknown"
                    failed_probes.append(probe_name)
                logger.warning("Probe failed: %s", r)
                # v5: Convert probe infrastructure failures into problems
                # (instead of silently swallowing them)
                all_problems.append(ProblemSpec(
                    problem_id=f"probe-failure-{probe_name}",
                    source_probe="discovery",
                    severity=Severity.P2.value,
                    family="infrastructure",
                    title=f"Probe {probe_name} failed: {str(r)[:120]}",
                    description=f"Probe {probe_name} raised an exception during scan. "
                                f"This may indicate an infrastructure issue. Error: {str(r)[:300]}",
                    suggested_approach="execution_and_monitoring",
                ))

        # Deduplicate by problem_id
        seen: set = set()
        unique: List[ProblemSpec] = []
        for p in all_problems:
            if p.problem_id not in seen:
                seen.add(p.problem_id)
                unique.append(self._enrich_problem(p))

        # Sort by severity (P0 first)
        severity_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
        unique.sort(key=lambda x: severity_order.get(x.severity, 9))

        logger.info(
            "[%s] Discovery complete: %d problems from %d probes",
            self.agent_id, len(unique), len(probes),
        )

        return {
            "findings": [p.to_dict() for p in unique],
            "probe_count": len(probes),
            "total_raw": len(all_problems),
            "deduplicated": len(unique),
            "blocked": sum(1 for p in unique if p.current_status == ProblemStatus.BLOCKED.value),
            "review_required": sum(1 for p in unique if p.current_status == ProblemStatus.REVIEW_REQUIRED.value),
            "probe_errors": probe_errors,
            "failed_probes": failed_probes,
        }

    async def _run_probe(self, probe: Probe) -> List[ProblemSpec]:
        """Run a single probe with timeout protection.

        Exceptions propagate so the caller (with ``return_exceptions=True``)
        can count probe failures.  A ``ProbeError`` wraps the original
        exception and carries the probe name for diagnostics.
        """
        try:
            return await asyncio.wait_for(probe.scan(), timeout=self._probe_timeout_seconds(probe))
        except asyncio.TimeoutError:
            logger.warning("Probe %s timed out", probe.name)
            raise ProbeError(probe.name, "timeout")
        except Exception as exc:
            logger.exception("Probe %s error", probe.name)
            raise ProbeError(probe.name, str(exc)) from exc

    @staticmethod
    def _probe_timeout_seconds(probe: Probe) -> float:
        if probe.name == "doc25_angle":
            return 150.0
        if probe.name == "catalog_drift":
            return 90.0
        if probe.name == "mesh_audit":
            return 120.0
        return 60.0

    @staticmethod
    def _enrich_problem(problem: ProblemSpec) -> ProblemSpec:
        """Attach shard lane, handling path, blocked metadata, and doc25 angles to a finding."""
        family = (problem.family or "").lower()
        title = (problem.title or "").lower()
        source = (problem.source_probe or "").lower()

        if family == "issue-registry" or "issue-registry" in title or "issue_registry" in title:
            problem.task_family = "issue-registry"
            problem.lane_id = "gov_registry"
            problem.suggested_approach = HandlingPath.FIX_CODE.value
            # Preserve probe-set BLOCKED/REVIEW_REQUIRED status
            if problem.current_status not in (ProblemStatus.BLOCKED.value, ProblemStatus.REVIEW_REQUIRED.value):
                problem.current_status = ProblemStatus.ACTIVE.value
            problem.recommended_angles = [34, 35, 37]
            problem.analysis_angles = problem.analysis_angles or problem.recommended_angles
            if not problem.write_scope:
                problem.write_scope = list(_GOV_REGISTRY_SCOPE)
            return problem

        if source == "catalog_drift" or "feature marker" in title or "fr gate" in title:
            problem.task_family = "feature-governance"
            problem.lane_id = "gov_mapping"
            problem.suggested_approach = HandlingPath.FIX_THEN_REBUILD.value
            # Preserve probe-set BLOCKED/REVIEW_REQUIRED status
            if problem.current_status not in (ProblemStatus.BLOCKED.value, ProblemStatus.REVIEW_REQUIRED.value):
                problem.current_status = ProblemStatus.ACTIVE.value
            problem.recommended_angles = [32, 34, 35]
            problem.analysis_angles = problem.analysis_angles or problem.recommended_angles
            if not problem.write_scope:
                problem.write_scope = list(_GOV_MAPPING_SCOPE)
            return problem

        if source == "code_change":
            problem.task_family = "repo-governance"
            problem.lane_id = "repo_hygiene"
            problem.suggested_approach = HandlingPath.FREEZE_OR_ISOLATE.value
            problem.current_status = ProblemStatus.REVIEW_REQUIRED.value
            problem.blocker_type = "mixed_state"
            problem.blocked_reason = (
                "repository contains concurrent dirty changes; isolate or freeze before auto-writeback"
            )
            problem.recommended_angles = [6, 15, 39]
            problem.analysis_angles = problem.analysis_angles or problem.recommended_angles
            return problem

        if source in {"runtime_health", "provider_health"} or family == "infrastructure":
            problem.task_family = "runtime-external"
            problem.lane_id = "runtime_blocked"
            problem.suggested_approach = HandlingPath.EXTERNAL_DEPENDENCY.value
            problem.current_status = ProblemStatus.BLOCKED.value
            problem.blocker_type = "external_dependency"
            if not problem.blocked_reason:
                problem.blocked_reason = problem.description or problem.title or "external dependency unavailable"
            problem.recommended_angles = [9, 11, 22, 38]
            problem.analysis_angles = problem.analysis_angles or problem.recommended_angles
            return problem

        problem.task_family = problem.task_family or family or source or "general-fix"
        problem.lane_id = problem.lane_id or "general_fix"
        problem.current_status = problem.current_status or ProblemStatus.ACTIVE.value
        # Propagate doc25 recommended_angles to analysis_angles if not already set
        if problem.recommended_angles and not problem.analysis_angles:
            problem.analysis_angles = list(problem.recommended_angles)
        if not problem.write_scope and problem.affected_files:
            problem.write_scope = list(problem.affected_files)
        return problem
