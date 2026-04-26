from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    from scripts.github_automation_paths import (
        continuous_audit_dir,
        legacy_continuous_audit_dir,
        seed_dir_from_legacy,
    )
except ModuleNotFoundError:
    from github_automation_paths import (
        continuous_audit_dir,
        legacy_continuous_audit_dir,
        seed_dir_from_legacy,
    )


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = continuous_audit_dir(ROOT)
LEGACY_OUTPUT_DIR = legacy_continuous_audit_dir(ROOT)
HISTORY_DIR = OUTPUT_DIR / "history"
ISSUE_LEDGER = OUTPUT_DIR / "continuous_audit_issue_ledger.md"
LATEST_JSON = OUTPUT_DIR / "latest_run.json"
LOCK_FILE = OUTPUT_DIR / ".audit.lock"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAILED_NODE_RE = re.compile(r"^FAILED\s+(.+?)(?:\s+-\s+|$)", re.MULTILINE)
STYLE_LINK_RE = re.compile(r'<link rel="stylesheet" href="([^"]+)"')
COLLECT_ONLY_RE = re.compile(r"(?P<count>\d+)\s+tests\s+collected")


@dataclass
class CommandResult:
    name: str
    command: list[str]
    returncode: int
    duration_seconds: float
    failed_nodes: list[str] = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""


@dataclass
class Finding:
    issue_id: str
    severity: str
    kind: str
    title: str
    evidence: list[str]
    impact: str
    recommendation: str


@dataclass
class SharedArtifactStatus:
    audit_mode: str
    live_collect_only: int | None
    junit_exists: bool
    junit_tests: int | None
    catalog_exists: bool
    catalog_total_collected: int | None
    catalog_freshness: str | None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def ensure_dirs() -> None:
    seed_dir_from_legacy(OUTPUT_DIR, LEGACY_OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _read_lock_info(lock_path: Path | None = None) -> dict[str, str]:
    target = lock_path or LOCK_FILE
    try:
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    info: dict[str, str] = {}
    for line in raw.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        info[key.strip()] = value.strip()
    return info


def _pid_is_running(pid_value: str | None) -> bool:
    try:
        pid = int(str(pid_value or "").strip())
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if os.name == "nt":
        completed = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = (completed.stdout or "").strip()
        if not output or output.startswith("INFO:"):
            return False
        return f'"{pid}"' in output or f",{pid}," in output
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _reap_stale_lock(lock_info: dict[str, str] | None = None) -> bool:
    info = lock_info or _read_lock_info()
    if _pid_is_running(info.get("pid")):
        return False
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return True


@contextmanager
def audit_lock() -> Any:
    ensure_dirs()
    lock_info: dict[str, str] | None = None
    while True:
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            lock_info = _read_lock_info()
            if _reap_stale_lock(lock_info):
                lock_info = None
                continue
            yield False, lock_info
            return
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"pid={os.getpid()}\nstarted_at={now_utc().isoformat()}\n")
        yield True, None
    finally:
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass


def base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("MOCK_LLM", "true")
    env.setdefault("ENABLE_SCHEDULER", "false")
    env.setdefault("STRICT_REAL_DATA", "false")
    return env


def parse_failed_nodes(text: str) -> list[str]:
    out: list[str] = []
    for match in FAILED_NODE_RE.finditer(text):
        node = match.group(1).strip()
        if node and node not in out:
            out.append(node)
    return out


def parse_collect_only_count(text: str) -> int | None:
    match = COLLECT_ONLY_RE.search(text or "")
    if match is None:
        return None
    return int(match.group("count"))


def _read_junit_test_count(path: Path) -> int | None:
    if not path.exists():
        return None
    root = ET.parse(path).getroot()
    direct = root.attrib.get("tests")
    if direct is not None:
        try:
            return int(direct)
        except ValueError:
            pass
    total = 0
    found = False
    for suite in root.iter("testsuite"):
        value = suite.attrib.get("tests")
        if value is None:
            continue
        try:
            total += int(value)
            found = True
        except ValueError:
            continue
    return total if found else None


def load_shared_artifact_status(*, live_collect_only: int | None) -> SharedArtifactStatus:
    junit_path = ROOT / "output" / "junit.xml"
    catalog_path = ROOT / "app" / "governance" / "catalog_snapshot.json"
    catalog_payload = json.loads(catalog_path.read_text(encoding="utf-8")) if catalog_path.exists() else {}
    return SharedArtifactStatus(
        audit_mode="read_only",
        live_collect_only=live_collect_only,
        junit_exists=junit_path.exists(),
        junit_tests=_read_junit_test_count(junit_path),
        catalog_exists=catalog_path.exists(),
        catalog_total_collected=(catalog_payload.get("test_collection_summary") or {}).get("total_collected"),
        catalog_freshness=catalog_payload.get("test_result_freshness"),
    )


def run_command(name: str, command: list[str]) -> CommandResult:
    start = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=base_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    duration = time.monotonic() - start
    combined = (completed.stdout or "") + "\n" + (completed.stderr or "")
    return CommandResult(
        name=name,
        command=command,
        returncode=completed.returncode,
        duration_seconds=round(duration, 3),
        failed_nodes=parse_failed_nodes(combined),
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def load_registry_stats() -> dict[str, Any]:
    registry_path = ROOT / "app" / "governance" / "feature_registry.json"
    catalog_path = ROOT / "app" / "governance" / "catalog_snapshot.json"
    mismatch_path = ROOT / "app" / "governance" / "mismatch_report.json"

    registry = json.loads(registry_path.read_text(encoding="utf-8"))["features"]
    catalog = json.loads(catalog_path.read_text(encoding="utf-8")) if catalog_path.exists() else {}
    mismatch = json.loads(mismatch_path.read_text(encoding="utf-8")) if mismatch_path.exists() else {}

    def normalize(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def has_issue_marker(value: Any) -> bool:
        text = normalize(value)
        return any(marker in text for marker in ("⚠", "❌", "🔴"))

    warn_features = []
    for feat in registry:
        verdict_blob = " | ".join(
            [
                normalize(feat.get("code_verdict")),
                normalize(feat.get("test_verdict")),
                normalize(feat.get("page_verdict")),
                " ; ".join(feat.get("gaps") or []),
            ]
        )
        if (
            has_issue_marker(feat.get("code_verdict"))
            or has_issue_marker(feat.get("test_verdict"))
            or has_issue_marker(feat.get("page_verdict"))
            or (feat.get("gaps") or [])
        ):
            warn_features.append(feat)

    by_fr: dict[str, int] = {}
    for feat in warn_features:
        by_fr[feat["fr_id"]] = by_fr.get(feat["fr_id"], 0) + 1

    return {
        "registry_total": len(registry),
        "warn_features": len(warn_features),
        "warn_by_fr": dict(sorted(by_fr.items())),
        "catalog_status_summary": catalog.get("status_summary", {}),
        "mismatch_count": len(mismatch.get("mismatches", [])),
        "mismatch_titles": [item.get("feature_id", "") for item in mismatch.get("mismatches", [])],
    }


def load_page_alignment_stats() -> dict[str, Any]:
    registry = json.loads((ROOT / "app" / "governance" / "feature_registry.json").read_text(encoding="utf-8"))["features"]
    page_expectations = (ROOT / "scripts" / "doc_driven" / "page_expectations.py").read_text(encoding="utf-8")
    templates = sorted(path.name for path in (ROOT / "app" / "web" / "templates").glob("*.html"))

    registry_page_ids = {
        feat["feature_id"]
        for feat in registry
        if (
            ("page" in (feat.get("required_test_kinds") or []) and feat.get("runtime_page_path"))
            or feat["feature_id"].startswith("LEGACY-REPORT-")
            or (feat["feature_id"].startswith("OOS-MOCK-PAY-") and feat.get("runtime_page_path"))
        )
    }
    expectation_ids = set()
    for block in re.findall(r"fr_ids=\[(.*?)\]", page_expectations, re.S):
        expectation_ids.update(re.findall(r'"([^"]+)"', block))
    expected_templates = set(re.findall(r'template="([^"]+)"', page_expectations))
    missing_templates = sorted(template for template in templates if template not in expected_templates)

    features_selectors_empty = False
    feature_match = re.search(
        r'page_id="features".*?must_have_selectors=\[(.*?)\]',
        page_expectations,
        re.S,
    )
    if feature_match is not None:
        features_selectors_empty = feature_match.group(1).strip() == ""

    return {
        "registry_page_ids": sorted(registry_page_ids),
        "expectation_page_ids": sorted(expectation_ids),
        "only_in_registry": sorted(registry_page_ids - expectation_ids),
        "only_in_expectations": sorted(expectation_ids - registry_page_ids),
        "missing_templates": missing_templates,
        "features_selectors_empty": features_selectors_empty,
    }


class ProbeRuntime:
    def __init__(self) -> None:
        self._saved: dict[str, Any] = {}
        self._engine = None
        self._SessionLocal = None
        self._db_path = OUTPUT_DIR / f"probe_{uuid4().hex}.db"

    def __enter__(self) -> "ProbeRuntime":
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        import app.api.routes_auth as routes_auth
        import app.core.db as core_db
        import app.main as app_main
        from app.models import Base

        self._saved = {
            "core_engine": core_db.engine,
            "core_session": core_db.SessionLocal,
            "auth_session": routes_auth.SessionLocal,
            "app_engine": app_main.engine,
            "app_session": app_main.SessionLocal,
        }
        self._engine = create_engine(
            f"sqlite:///{self._db_path}",
            connect_args={"check_same_thread": False},
        )
        self._SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)
        Base.metadata.create_all(bind=self._engine)

        core_db.engine = self._engine
        core_db.SessionLocal = self._SessionLocal
        routes_auth.SessionLocal = self._SessionLocal
        app_main.engine = self._engine
        app_main.SessionLocal = self._SessionLocal

        def override_get_db() -> Any:
            db = self._SessionLocal()
            try:
                yield db
            finally:
                db.close()

        app_main.app.dependency_overrides[core_db.get_db] = override_get_db
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        import app.api.routes_auth as routes_auth
        import app.core.db as core_db
        import app.main as app_main

        app_main.app.dependency_overrides.clear()
        core_db.engine = self._saved["core_engine"]
        core_db.SessionLocal = self._saved["core_session"]
        routes_auth.SessionLocal = self._saved["auth_session"]
        app_main.engine = self._saved["app_engine"]
        app_main.SessionLocal = self._saved["app_session"]
        if self._engine is not None:
            self._engine.dispose()
        try:
            self._db_path.unlink()
        except FileNotFoundError:
            pass

    @property
    def SessionLocal(self) -> Any:
        return self._SessionLocal

    def client(self, *, raise_server_exceptions: bool = True) -> Any:
        from fastapi.testclient import TestClient
        import app.main as app_main

        return TestClient(
            app_main.app,
            base_url="http://localhost",
            raise_server_exceptions=raise_server_exceptions,
        )

    def create_user(
        self,
        *,
        email: str,
        password: str,
        role: str = "user",
        tier: str = "Free",
        email_verified: bool = True,
    ) -> None:
        from app.core.security import hash_password
        from app.models import User

        db = self._SessionLocal()
        try:
            user = User(
                email=email,
                password_hash=hash_password(password),
                role=role,
                tier=tier,
                email_verified=email_verified,
            )
            db.add(user)
            db.commit()
        finally:
            db.close()


def probe_features_css() -> Finding | None:
    with ProbeRuntime() as runtime:
        runtime.create_user(
            email="audit-admin@example.com",
            password="Password123",
            role="admin",
            tier="Pro",
        )
        with runtime.client() as client:
            login = client.post(
                "/auth/login",
                json={"email": "audit-admin@example.com", "password": "Password123"},
            )
            token = login.json()["data"]["access_token"]
            resp = client.get("/features", headers={"Authorization": f"Bearer {token}"})
            css_ref_missing = '/static/css/style.css' in resp.text
            asset = client.get("/static/css/style.css")
        if css_ref_missing and asset.status_code == 404:
            return Finding(
                issue_id="ISSUE-003",
                severity="P2",
                kind="page_rendering",
                title="/features references a missing stylesheet",
                evidence=[
                    "GET /features -> 200",
                    "HTML contains /static/css/style.css",
                    "GET /static/css/style.css -> 404",
                ],
                impact="The features page can load without its intended stylesheet.",
                recommendation="Point /features to an existing stylesheet or ship the referenced asset.",
            )
    return None


def probe_report_status_contract() -> Finding | None:
    with ProbeRuntime() as runtime:
        with runtime.client(raise_server_exceptions=False) as client:
            resp = client.get("/report/600519.SH/status")
        content_type = resp.headers.get("content-type", "")
        if resp.status_code == 404 and content_type.startswith("text/html"):
            return Finding(
                issue_id="ISSUE-013",
                severity="P2",
                kind="contract",
                title="/report/{stock_code}/status returns HTML errors although the page consumes JSON",
                evidence=[
                    "report_loading.html always calls response.json()",
                    "GET /report/600519.SH/status -> 404 text/html in an empty runtime",
                ],
                impact="The loading page cannot receive structured error details in failure paths.",
                recommendation="Treat the status route as JSON-first or move it under /api/ with JSON-only exception handling.",
            )
    return None


def probe_report_list_cutoff() -> Finding | None:
    from app.services.ssot_read_model import list_report_summaries_ssot
    from app.services.trade_calendar import latest_trade_date_str
    from tests.helpers_ssot import insert_report_bundle_ssot

    latest = latest_trade_date_str()
    with ProbeRuntime() as runtime:
        db = runtime.SessionLocal()
        try:
            insert_report_bundle_ssot(db, stock_code="600519.SH", stock_name="MOUTAI", trade_date="2026-03-06")
            old_payload = list_report_summaries_ssot(db, viewer_tier="Free", viewer_role=None)
        finally:
            db.close()

    with ProbeRuntime() as runtime:
        db = runtime.SessionLocal()
        try:
            insert_report_bundle_ssot(db, stock_code="600519.SH", stock_name="MOUTAI", trade_date=latest)
            latest_payload = list_report_summaries_ssot(db, viewer_tier="Free", viewer_role=None)
        finally:
            db.close()

    if old_payload["total"] == 0 and latest_payload["total"] == 1:
        return Finding(
            issue_id="ISSUE-002",
            severity="P1",
            kind="data_consistency",
            title="Report list visibility depends on global trade-calendar state instead of the current session data",
            evidence=[
                f"latest_trade_date_str() -> {latest}",
                "A seeded 2026-03-06 report yields total=0 for viewer_tier=Free",
                f"A seeded {latest} report yields total=1 for viewer_tier=Free",
            ],
            impact="The /reports page can appear empty even when the current runtime contains published reports.",
            recommendation="Bind viewer cutoff logic to the current runtime/session anchor instead of the module-level trade calendar engine.",
        )
    return None


def probe_html_500_behavior() -> Finding | None:
    from tests.helpers_ssot import insert_report_bundle_ssot

    with ProbeRuntime() as runtime:
        db = runtime.SessionLocal()
        try:
            report = insert_report_bundle_ssot(db, trade_date="2026-03-18")
            report_id = report.report_id
        finally:
            db.close()
        with runtime.client(raise_server_exceptions=False) as client:
            resp = client.get(f"/reports/{report_id}")
        content_type = resp.headers.get("content-type", "")
        if resp.status_code == 500 and content_type.startswith("application/json"):
            return Finding(
                issue_id="ISSUE-004",
                severity="P2",
                kind="page_rendering",
                title="HTML page exceptions currently fall through to JSON 500 responses",
                evidence=[
                    f"GET /reports/{report_id} -> 500",
                    f"content-type={content_type}",
                    "500.html exists but is not used by the generic exception handler",
                ],
                impact="Users can receive raw JSON envelopes instead of an HTML error page.",
                recommendation="Render 500.html for non-API HTML routes or remove the dead template.",
            )
    return None


def detect_detail_chain_issue(results: list[CommandResult]) -> Finding | None:
    combined_text = "\n".join((result.stdout or "") + "\n" + (result.stderr or "") for result in results)
    if "NameError: name 'trade_date' is not defined" not in combined_text:
        return None
    nodes = []
    for result in results:
        for node in result.failed_nodes:
            if "/reports/" in node or "report_detail" in node or "advanced" in node or "FR10_SITE_04" in node:
                nodes.append(node)
    if not nodes:
        nodes = [node for result in results for node in result.failed_nodes[:10]]
    return Finding(
        issue_id="ISSUE-001",
        severity="P1",
        kind="implementation",
        title="The shared report-detail read path crashes because _load_ssot_report_bundle uses undefined variables",
        evidence=(["NameError: name 'trade_date' is not defined"] + nodes[:8]),
        impact="Report detail pages, detail APIs, advanced APIs, and legacy /report/{stock_code} redirects are unstable.",
        recommendation="Fix the undefined-variable branch in _load_ssot_report_bundle and add regression tests for detail, advanced, and redirect flows.",
    )


def detect_page_alignment_issue() -> Finding | None:
    stats = load_page_alignment_stats()
    if not stats["only_in_registry"] and not stats["only_in_expectations"]:
        return None
    evidence = [
        f"registry page feature ids={len(stats['registry_page_ids'])}",
        f"page_expectations feature ids={len(stats['expectation_page_ids'])}",
    ]
    if stats["only_in_registry"]:
        evidence.append("only in registry: " + ", ".join(stats["only_in_registry"][:10]))
    if stats["only_in_expectations"]:
        evidence.append("only in page_expectations: " + ", ".join(stats["only_in_expectations"][:10]))
    return Finding(
        issue_id="ISSUE-005",
        severity="P2",
        kind="test_gate",
        title="The page expectation registry has drifted away from feature_registry.json",
        evidence=evidence,
        impact="Page-level verification no longer maps cleanly back to the active feature inventory.",
        recommendation="Normalize page_expectations.py to the real feature ids from feature_registry.json.",
    )


def detect_uncovered_templates_issue() -> Finding | None:
    stats = load_page_alignment_stats()
    interesting = {"privacy.html", "terms.html", "403.html", "404.html", "500.html"}
    missing = [name for name in stats["missing_templates"] if name in interesting]
    if not missing:
        return None
    return Finding(
        issue_id="ISSUE-006",
        severity="P2",
        kind="coverage_gap",
        title="Multiple formal info/error templates are not covered by page expectations",
        evidence=[
            "missing from page_expectations: " + ", ".join(missing),
            "report_error/loading/not_ready are present in page_expectations but still lack focused tests",
        ],
        impact="Info pages and error pages can regress without being caught by the current page gate.",
        recommendation="Add privacy/terms/403/404/500 and report transition pages to the formal page verification set.",
    )


def detect_features_gate_issue() -> Finding | None:
    stats = load_page_alignment_stats()
    if not stats["features_selectors_empty"]:
        return None
    return Finding(
        issue_id="ISSUE-007",
        severity="P3",
        kind="test_gate",
        title="The /features page is verified only at a shallow level",
        evidence=[
            "page_expectations.py has no must_have_selectors for page_id=features",
            "tests/test_features_page.py mainly checks 200/302/403 and light text assertions",
        ],
        impact="The page can lose structure or assets and still pass the current gate.",
        recommendation="Add DOM anchors for summary cards, filters, FR groups, and feature cards.",
    )


def detect_static_registry_issues(registry_stats: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    if registry_stats["warn_features"] > 0:
        top_frs = ", ".join(
            f"{fr}:{count}" for fr, count in list(registry_stats["warn_by_fr"].items())[:8]
        )
        findings.append(
            Finding(
                issue_id="ISSUE-REGISTRY",
                severity="P2",
                kind="static_audit",
                title="feature_registry.json still reports many features with gaps or warnings",
                evidence=[
                    f"warn_features={registry_stats['warn_features']}",
                    f"catalog_status_summary={registry_stats['catalog_status_summary']}",
                    f"warn_by_fr={top_frs}",
                ],
                impact="Even when the test subset is green, the registry still signals many unclosed implementation/test gaps.",
                recommendation="Use the registry warning clusters to prioritize the next remediation rounds.",
            )
        )
    return findings


def detect_blind_spot_findings(report: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    for issue in report.get("issues", []):
        findings.append(
            Finding(
                issue_id=issue["issue_id"],
                severity=issue["severity"],
                kind=issue["kind"],
                title=issue["title"],
                evidence=issue["evidence"],
                impact=(
                    "The current verification method can still miss real regressions "
                    "even when the product test subset looks green."
                ),
                recommendation=issue["recommendation"],
            )
        )
    return findings


def detect_shared_artifact_drift(shared_artifact_status: SharedArtifactStatus) -> list[Finding]:
    findings: list[Finding] = []
    evidence = [f"audit_mode={shared_artifact_status.audit_mode}"]
    if shared_artifact_status.live_collect_only is not None:
        evidence.append(f"live_collect_only={shared_artifact_status.live_collect_only}")
    if shared_artifact_status.junit_exists:
        evidence.append(f"junit_tests={shared_artifact_status.junit_tests}")
    else:
        evidence.append("junit_missing=true")
    if shared_artifact_status.catalog_exists:
        evidence.append(
            "catalog_total_collected="
            + (str(shared_artifact_status.catalog_total_collected) if shared_artifact_status.catalog_total_collected is not None else "null")
        )
        evidence.append(f"catalog_freshness={shared_artifact_status.catalog_freshness}")
    else:
        evidence.append("catalog_missing=true")

    counts = {
        value
        for value in (
            shared_artifact_status.live_collect_only,
            shared_artifact_status.junit_tests,
            shared_artifact_status.catalog_total_collected,
        )
        if value is not None
    }
    has_drift = len(counts) > 1
    missing_required = not shared_artifact_status.junit_exists or not shared_artifact_status.catalog_exists
    stale_catalog = shared_artifact_status.catalog_freshness not in (None, "fresh")
    if not (has_drift or missing_required or stale_catalog):
        return findings

    findings.append(
        Finding(
            issue_id="ISSUE-SHARED-ARTIFACT",
            severity="P1",
            kind="artifact_drift",
            title="shared artifacts are not on the same round as the live workspace",
            evidence=evidence,
            impact="The audit can only report the drift; it must not promote or rewrite shared artifacts on behalf of the controller.",
            recommendation="Have the controller run a single-writer post-fix promote after the runtime chain is stable.",
        )
    )
    return findings


def build_markdown(
    *,
    started_at: str,
    finished_at: str,
    command_results: list[CommandResult],
    findings: list[Finding],
    registry_stats: dict[str, Any],
    blind_spot_summary: dict[str, Any],
    history_json_name: str,
    shared_artifact_status: SharedArtifactStatus | None = None,
) -> str:
    total_failed_tests = sum(len(result.failed_nodes) for result in command_results)
    lines: list[str] = []
    lines.append("# Continuous Audit Issue Ledger")
    lines.append("")
    lines.append(f"- Started at: `{started_at}`")
    lines.append(f"- Finished at: `{finished_at}`")
    lines.append(f"- Root: `{ROOT}`")
    lines.append(f"- History JSON: `{history_json_name}`")
    lines.append("")
    lines.append("## Snapshot")
    lines.append("")
    if shared_artifact_status is not None:
        lines.append(f"- Shared artifact mode: `{shared_artifact_status.audit_mode}`")
        lines.append(f"- Live collect-only: `{shared_artifact_status.live_collect_only}`")
        lines.append(f"- Shared junit tests: `{shared_artifact_status.junit_tests}`")
        lines.append(
            "- Shared catalog total_collected/freshness: "
            f"`{shared_artifact_status.catalog_total_collected}` / `{shared_artifact_status.catalog_freshness}`"
        )
    lines.append(f"- Registry total features: `{registry_stats['registry_total']}`")
    lines.append(f"- Registry warning/gap features: `{registry_stats['warn_features']}`")
    lines.append(f"- Catalog status summary: `{registry_stats['catalog_status_summary']}`")
    lines.append(f"- Mismatch count: `{registry_stats['mismatch_count']}`")
    lines.append(f"- Failed test node count across suites: `{total_failed_tests}`")
    lines.append(
        "- Blind-spot summary: "
        f"fake/hollow/weak=`{blind_spot_summary['fake_count']}/{blind_spot_summary['hollow_count']}/{blind_spot_summary['weak_count']}`, "
        f"guarded=`{blind_spot_summary['guarded_assertions']}`, "
        f"missing_expectations=`{blind_spot_summary['missing_expectations']}`, "
        f"no_dom=`{blind_spot_summary['pages_without_dom']}`, "
        f"no_browser=`{blind_spot_summary['pages_without_browser']}`"
    )
    lines.append("")
    lines.append("## Command Results")
    lines.append("")
    for result in command_results:
        lines.append(
            f"- `{result.name}` -> rc=`{result.returncode}`, failed_nodes=`{len(result.failed_nodes)}`, duration=`{result.duration_seconds:.3f}s`"
        )
        if result.failed_nodes:
            for node in result.failed_nodes[:8]:
                lines.append(f"  - `{node}`")
    lines.append("")
    lines.append("## Active Findings")
    lines.append("")
    if not findings:
        lines.append("- No active findings were detected in this run.")
    else:
        for finding in findings:
            lines.append(f"### {finding.issue_id}")
            lines.append("")
            lines.append(f"- Severity: `{finding.severity}`")
            lines.append(f"- Kind: `{finding.kind}`")
            lines.append(f"- Title: {finding.title}")
            lines.append("- Evidence:")
            for item in finding.evidence:
                lines.append(f"  - {item}")
            lines.append(f"- Impact: {finding.impact}")
            lines.append(f"- Recommendation: {finding.recommendation}")
            lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This file is the single latest issue ledger for the scheduled audit.")
    lines.append("- Historical raw outputs are stored as JSON in `github/automation/continuous_audit/history/`.")
    return "\n".join(lines) + "\n"


def write_outputs(
    *,
    started_at: str,
    finished_at: str,
    command_results: list[CommandResult],
    findings: list[Finding],
    registry_stats: dict[str, Any],
    blind_spot_summary: dict[str, Any],
    shared_artifact_status: SharedArtifactStatus | None = None,
) -> None:
    stamp = utc_stamp()
    payload = {
        "status": "completed",
        "started_at": started_at,
        "finished_at": finished_at,
        "root": str(ROOT),
        "registry_stats": registry_stats,
        "blind_spot_summary": blind_spot_summary,
        "shared_artifact_status": asdict(shared_artifact_status) if shared_artifact_status is not None else {},
        "command_results": [asdict(result) for result in command_results],
        "findings": [asdict(finding) for finding in findings],
    }
    history_json = HISTORY_DIR / f"{stamp}.json"
    _atomic_write_json(history_json, payload)
    _atomic_write_json(LATEST_JSON, payload)
    markdown = build_markdown(
        started_at=started_at,
        finished_at=finished_at,
        command_results=command_results,
        findings=findings,
        registry_stats=registry_stats,
        blind_spot_summary=blind_spot_summary,
        history_json_name=history_json.name,
        shared_artifact_status=shared_artifact_status,
    )
    _atomic_write_text(ISSUE_LEDGER, markdown)


def write_skip_locked_outputs(*, started_at: str, finished_at: str, lock_info: dict[str, str] | None) -> None:
    stamp = utc_stamp()
    payload = {
        "status": "skipped_locked",
        "started_at": started_at,
        "finished_at": finished_at,
        "root": str(ROOT),
        "lock_info": lock_info or {},
        "registry_stats": {},
        "blind_spot_summary": {},
        "command_results": [],
        "findings": [],
    }
    history_json = HISTORY_DIR / f"{stamp}.json"
    _atomic_write_json(history_json, payload)
    preserve_completed_latest = False
    if LATEST_JSON.exists():
        try:
            current_latest = json.loads(LATEST_JSON.read_text(encoding="utf-8"))
            preserve_completed_latest = (current_latest.get("status") or "").lower() == "completed"
        except (json.JSONDecodeError, OSError):
            preserve_completed_latest = False
    if not preserve_completed_latest:
        _atomic_write_json(LATEST_JSON, payload)

    lines = [
        "# Continuous Audit Issue Ledger",
        "",
        "- Status: skipped because a previous audit run is still holding the lock.",
    ]
    pid_value = (lock_info or {}).get("pid")
    if pid_value:
        lines.append(f"- Lock PID: `{pid_value}`")
    started_value = (lock_info or {}).get("started_at")
    if started_value:
        lines.append(f"- Lock started_at: `{started_value}`")
    lines.append(f"- Historical raw output: `github/automation/continuous_audit/history/{history_json.name}`")
    if preserve_completed_latest:
        lines.append("- Latest machine-readable payload remains the last completed run: `github/automation/continuous_audit/latest_run.json`")
    else:
        lines.append("- Latest machine-readable payload: `github/automation/continuous_audit/latest_run.json`")
    lines.append("")
    _atomic_write_text(ISSUE_LEDGER, "\n".join(lines))


def main() -> int:
    started_at = now_utc().isoformat()
    ensure_dirs()

    with audit_lock() as (locked, lock_info):
        if not locked:
            finished_at = now_utc().isoformat()
            write_skip_locked_outputs(started_at=started_at, finished_at=finished_at, lock_info=lock_info)
            return 0

        command_results = [
            run_command(
                "collect_only_live",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests",
                    "--collect-only",
                    "-q",
                ],
            ),
            run_command("governance_alignment", [sys.executable, "-m", "pytest", "tests/test_governance_alignment.py", "-q"]),
            run_command(
                "page_matrix",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/test_doc_driven_verify.py",
                    "tests/test_frontend_backend_integration.py",
                    "tests/test_features_page.py",
                    "tests/test_fr10_site_dashboard.py",
                    "tests/test_gate_browser.py",
                    "tests/test_gate_browser_playwright.py",
                    "--tb=short",
                    "-q",
                ],
            ),
            run_command(
                "detail_contracts",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/test_fr00_authenticity_guard.py",
                    "tests/test_fr10_reports.py",
                    "tests/test_nfr03_contract.py",
                    "tests/test_nfr18_schema.py",
                    "--tb=short",
                    "-q",
                ],
            ),
        ]

        live_collect_only = parse_collect_only_count(command_results[0].stdout)
        shared_artifact_status = load_shared_artifact_status(live_collect_only=live_collect_only)
        registry_stats = load_registry_stats()
        from scripts.doc_driven.audit_blind_spots import audit_blind_spots

        blind_spot_report = audit_blind_spots()
        findings: list[Finding] = []

        for detector in (
            lambda: detect_detail_chain_issue(command_results),
            detect_page_alignment_issue,
            detect_uncovered_templates_issue,
            detect_features_gate_issue,
            probe_report_list_cutoff,
            probe_features_css,
            probe_html_500_behavior,
            probe_report_status_contract,
        ):
            finding = detector()
            if finding is not None:
                findings.append(finding)

        findings.extend(detect_static_registry_issues(registry_stats))
        findings.extend(detect_blind_spot_findings(blind_spot_report))
        findings.extend(detect_shared_artifact_drift(shared_artifact_status))
        findings.sort(key=lambda item: (item.severity, item.issue_id))

        finished_at = now_utc().isoformat()
        write_outputs(
            started_at=started_at,
            finished_at=finished_at,
            command_results=command_results,
            findings=findings,
            registry_stats=registry_stats,
            blind_spot_summary=blind_spot_report["summary"],
            shared_artifact_status=shared_artifact_status,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
