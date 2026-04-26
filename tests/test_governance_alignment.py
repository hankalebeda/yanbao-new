"""
Governance alignment gates for the registry, routes, and page expectations.
"""

from __future__ import annotations

import builtins
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from app.models import Report as _ReportModel

from app.governance.build_feature_catalog import scan_fastapi_routes
from scripts.doc_driven.page_expectations import PAGE_EXPECTATIONS

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "app" / "governance" / "feature_registry.json"
pytestmark = [pytest.mark.feature("FR10-FEATURE-01")]

if getattr(builtins, "Report", None) is None:
    builtins.Report = _ReportModel


def load_registry():
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture()
def client():
    import app.api.routes_governance as routes_governance
    from app.core.request_context import bind_request_id, reset_request_id

    async def _fake_require_admin(request: Request):
        token = (request.headers.get("Authorization") or "").strip()
        if token == "Bearer admin-token":
            return SimpleNamespace(role="admin")
        if token == "Bearer super-admin-token":
            return SimpleNamespace(role="super_admin")
        if token == "Bearer user-token":
            raise HTTPException(status_code=403, detail="FORBIDDEN")
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")

    app = FastAPI()
    app.dependency_overrides[routes_governance._require_admin] = _fake_require_admin

    @app.middleware("http")
    async def _request_id_middleware(request: Request, call_next):
        _, token = bind_request_id(request.headers.get("X-Request-ID"))
        try:
            return await call_next(request)
        finally:
            reset_request_id(token)

    app.include_router(routes_governance.features_router)
    app.include_router(routes_governance.governance_router)
    try:
        with TestClient(app, base_url="http://localhost") as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture()
def create_user():
    def _create_user(*, email="user@test.com", password="Password123", role="user", **_kwargs):
        return {"user": SimpleNamespace(email=email, role=role), "password": password}

    return _create_user


def _runtime_html_routes_from_main_file() -> set[str]:
    text = (ROOT / "app" / "main.py").read_text(encoding="utf-8-sig")
    routes = {match.group("path") for match in re.finditer(r'@app\.get\("(?P<path>/[^"]*)"', text)}
    excluded_routes = {
        "/favicon.ico",
        "/health",
        "/logout",
        "/reports/list",
        "/sim-dashboard",
        "/sim",
        "/sim/dashboard",
        "/watchlist",
        "/report/实时研报/{stock_code}",
        "/report/\\u5b9e\\u65f6\\u7814\\u62a5/{stock_code}",
        "/report/{legacy_path:path}",  # 纯跳转路由，无独立页面模板
    }
    excluded_prefixes = ("/api/", "/auth/", "/docs", "/openapi", "/redoc", "/static/", "/_test/")
    return {
        path
        for path in routes
        if path not in excluded_routes and not any(path.startswith(prefix) for prefix in excluded_prefixes)
    }


REQUIRED_FIELDS = {
    "feature_id",
    "title",
    "fr_id",
    "group",
    "visibility",
    "ssot_refs",
    "primary_api",
    "request_params",
    "default_example",
    "key_response_fields",
    "required_test_kinds",
    "owner_scope",
}

VALID_VISIBILITY = {"public", "admin", "internal", "out_of_ssot", "deprecated"}
VALID_OWNER = {"数据工程师", "研报生成工程师", "前端与体验", "测试与质量"}
VALID_FR_PATTERN = re.compile(r"^FR-\d{2}(-b)?$")
VALID_FEATURE_ID_PATTERN = re.compile(r"^(FR\d{2}[A-Z]?-|LEGACY-|OOS-)")


class TestRegistrySchema:
    def test_registry_file_exists(self):
        assert REGISTRY_PATH.exists(), "feature_registry.json does not exist"

    def test_registry_is_valid_json(self):
        data = load_registry()
        assert "features" in data
        assert isinstance(data["features"], list)

    def test_registry_has_features(self):
        data = load_registry()
        assert len(data["features"]) >= 90

    def test_each_feature_has_required_fields(self):
        data = load_registry()
        for feat in data["features"]:
            missing = REQUIRED_FIELDS - set(feat.keys())
            assert not missing, f"{feat.get('feature_id', '?')} missing fields: {missing}"

    def test_feature_id_format(self):
        data = load_registry()
        for feat in data["features"]:
            assert VALID_FEATURE_ID_PATTERN.match(feat["feature_id"]), f"invalid feature_id: {feat['feature_id']}"

    def test_visibility_enum(self):
        data = load_registry()
        for feat in data["features"]:
            assert feat["visibility"] in VALID_VISIBILITY

    def test_owner_scope_enum(self):
        data = load_registry()
        for feat in data["features"]:
            assert feat["owner_scope"] in VALID_OWNER

    def test_fr_id_format(self):
        data = load_registry()
        for feat in data["features"]:
            assert VALID_FR_PATTERN.match(feat["fr_id"]), f"invalid fr_id: {feat['fr_id']}"

    def test_no_duplicate_feature_ids(self):
        data = load_registry()
        ids = [f["feature_id"] for f in data["features"]]
        assert len(ids) == len(set(ids)), "duplicate feature_id detected"

    def test_primary_api_structure(self):
        data = load_registry()
        for feat in data["features"]:
            api = feat.get("primary_api", {})
            if feat["visibility"] not in ("out_of_ssot", "deprecated"):
                assert "method" in api
                assert "path" in api

    def test_retired_internal_cleanup_routes_are_tagged_in_registry(self):
        data = load_registry()
        by_id = {item["feature_id"]: item for item in data["features"]}
        for feature_id in ("FR09B-CLEAN-01", "FR09B-CLEAN-02"):
            assert by_id[feature_id].get("governance_flags") == ["RETIRED_ROUTE"]

    def test_registry_cleans_only_proven_stale_gap_copy(self):
        data = load_registry()
        by_id = {item["feature_id"]: item for item in data["features"]}

        cookie_probe = by_id["FR03-COOKIE-02"]
        assert "5分钟自动探测未实现(无cron/scheduler注册)" not in (cookie_probe.get("gaps") or [])
        assert "每5分钟自动探测已实现" in str(cookie_probe.get("code_verdict") or "")

        sim_liquidation = by_id["FR08-SIM-06"]
        assert sim_liquidation.get("gaps") == []
        assert "DELISTED_LIQUIDATED" in str(sim_liquidation.get("code_verdict") or "")
        assert "未实现" not in str(sim_liquidation.get("code_verdict") or "")

        sim_adj = by_id["FR08-SIM-07"]
        assert sim_adj.get("gaps") == []
        assert "前复权动态折算" in str(sim_adj.get("code_verdict") or "")
        assert "未实现" not in str(sim_adj.get("code_verdict") or "")

    def test_registry_current_public_contract_entries_follow_ssot(self):
        data = load_registry()
        by_id = {item["feature_id"]: item for item in data["features"]}

        prediction_stats = by_id["FR07-SETTLE-04"]
        assert "PredictionStatsPayload" in prediction_stats["spec_requirement"]
        assert prediction_stats.get("gaps") == []

        auth_me = by_id["FR09-AUTH-08"]
        assert "membership_level" in auth_me["spec_requirement"]
        assert "membership_expires_at" in auth_me["spec_requirement"]
        assert auth_me.get("gaps") == []

        platform_plans = by_id["FR09-AUTH-09"]
        assert "05未冻结此路由" not in platform_plans["spec_requirement"]
        assert platform_plans.get("gaps") == []

        platform_config = by_id["FR10-PLATFORM-01"]
        assert "PlatformConfigPayload" in platform_config["spec_requirement"]
        assert platform_config.get("gaps") == []

        platform_summary = by_id["FR10-PLATFORM-02"]
        assert "PlatformSummary" in platform_summary["spec_requirement"]
        assert platform_summary.get("gaps") == []


class TestRouteAlignment:
    @pytest.fixture(autouse=True)
    def _setup(self):
        os.environ.setdefault("MOCK_LLM", "true")
        os.environ.setdefault("ENABLE_SCHEDULER", "false")
        os.environ.setdefault("STRICT_REAL_DATA", "false")

    def _get_code_routes(self) -> set[str]:
        return scan_fastapi_routes()

    def test_registry_routes_exist_in_code(self):
        data = load_registry()
        code_routes = self._get_code_routes()
        missing = []
        for feat in data["features"]:
            if feat["visibility"] in ("deprecated", "out_of_ssot"):
                continue
            api = feat.get("primary_api", {})
            if not api.get("method"):
                continue
            route_key = f"{api['method']} {api['path']}"
            found = route_key in code_routes
            if not found and "{" in route_key:
                base = api["path"].split("{")[0]
                found = any(api["method"] in route and base in route for route in code_routes)
            if not found:
                missing.append(f"{feat['feature_id']}: {route_key}")
        assert not missing, "registry routes missing in code:\n" + "\n".join(missing)


class TestPageAlignment:
    @staticmethod
    def _registry_expectation_feature_ids():
        registry = load_registry()["features"]
        return {
            item["feature_id"]
            for item in registry
            if (
                ("page" in (item.get("required_test_kinds") or []) and item.get("runtime_page_path"))
                or item["feature_id"].startswith("LEGACY-REPORT-")
                or (item["feature_id"].startswith("OOS-MOCK-PAY-") and item.get("runtime_page_path"))
            )
        }

    def test_page_templates_exist(self):
        data = load_registry()
        template_dir = ROOT / "app" / "web" / "templates"
        templates = {p.stem for p in template_dir.glob("*.html")} if template_dir.exists() else set()
        page_map = {
            "/": "index",
            "/reports": "reports_list",
            "/login": "login",
            "/register": "register",
            "/subscribe": "subscribe",
            "/forgot-password": "forgot_password",
            "/reset-password": "reset_password",
            "/profile": "profile",
            "/admin": "admin",
            "/dashboard": "dashboard",
            "/portfolio/sim-dashboard": "sim_dashboard",
            "/features": "features",
            "/terms": "terms",
            "/privacy": "privacy",
        }

        missing = []
        for feat in data["features"]:
            if "page" not in feat.get("required_test_kinds", []):
                continue
            page = feat.get("runtime_page_path") or ""
            if not page:
                continue
            clean = page.split("{")[0].rstrip("/") or "/"
            template = page_map.get(clean)
            if template and template not in templates:
                missing.append(f"{feat['feature_id']}: {page} -> {template}.html")
        assert not missing, "missing page templates:\n" + "\n".join(missing)

    def test_every_runtime_html_route_has_page_expectation(self):
        runtime_routes = _runtime_html_routes_from_main_file()
        expectation_routes = {item.route for item in PAGE_EXPECTATIONS}
        missing = sorted(runtime_routes - expectation_routes)
        assert not missing, "runtime routes missing page expectations:\n" + "\n".join(missing)

    def test_page_expectation_feature_ids_align_with_registry_page_routes(self):
        registry_page_features = self._registry_expectation_feature_ids()
        expectation_feature_ids = {
            feature_id
            for item in PAGE_EXPECTATIONS
            for feature_id in item.fr_ids
        }
        missing = sorted(registry_page_features - expectation_feature_ids)
        unexpected = sorted(expectation_feature_ids - registry_page_features)
        assert not missing, "page_expectations missing registry page features:\n" + "\n".join(missing)
        assert not unexpected, "page_expectations contain non-registry feature ids:\n" + "\n".join(unexpected)

    def test_page_expectation_selectors_exist_in_bound_templates(self):
        template_dir = ROOT / "app" / "web" / "templates"
        missing = []
        for item in PAGE_EXPECTATIONS:
            template_path = template_dir / item.template
            text = template_path.read_text(encoding="utf-8") if template_path.exists() else ""
            for selector in item.must_have_selectors:
                if selector in {"html", "body"}:
                    continue
                probe = selector
                if selector.startswith("#"):
                    probe = f'id="{selector[1:]}"'
                elif selector.startswith("."):
                    probe = selector[1:]
                elif re.match(r"^\w+\[\w+=['\"].+['\"]\]$", selector):
                    probe = selector.split("=", 1)[-1].strip("]").strip("'").strip('"')
                elif "name='" in selector:
                    probe = selector.split("name='")[-1].split("'")[0]
                elif "type='" in selector:
                    probe = selector.split("type='")[-1].split("'")[0]
                if probe and probe not in text:
                    missing.append(f"{item.page_id}:{selector}")
        assert not missing, "page_expectations selectors missing in templates:\n" + "\n".join(missing)


class TestSSOTAlignment:
    def test_ssot_refs_non_empty_for_active_features(self):
        data = load_registry()
        missing = []
        for feat in data["features"]:
            if feat["visibility"] in ("out_of_ssot", "deprecated"):
                continue
            if not feat.get("ssot_refs"):
                missing.append(feat["feature_id"])
        assert not missing, "active features missing ssot_refs:\n" + "\n".join(missing)

    def test_ssot_docs_exist(self):
        docs = ROOT / "docs" / "core"
        for name in ["01_需求基线.md", "03_详细设计.md", "05_API与数据契约.md"]:
            assert (docs / name).exists(), f"missing SSOT doc: {name}"


class TestTestAlignment:
    def test_fr_test_files_exist(self):
        tests_dir = ROOT / "tests"
        test_files = {p.stem for p in tests_dir.glob("test_fr*.py")}

        data = load_registry()
        fr_ids = {feat["fr_id"] for feat in data["features"] if feat["visibility"] not in ("out_of_ssot", "deprecated")}
        missing = []
        for fr_id in sorted(fr_ids):
            num = fr_id.replace("FR-", "").replace("-", "")
            pattern = f"test_fr{num}"
            if not any(tf.startswith(pattern) for tf in test_files):
                missing.append(fr_id)
        assert not missing, "missing FR test files:\n" + "\n".join(missing)


class TestCatalogAPI:
    @staticmethod
    def _login(client, create_user, role="admin"):
        del client, create_user
        token = "super-admin-token" if role == "super_admin" else role
        return {"Authorization": f"Bearer {token}-token", "X-Request-ID": str(uuid4())}

    @staticmethod
    def _expected_latest_valid_junit_source() -> str | None:
        from app.governance.build_feature_catalog import (
            JUNIT_CANDIDATES,
            collect_pytest_nodes,
            count_junit_testcases,
            load_junit_results,
            split_test_collection_map,
        )

        test_nodes_raw = collect_pytest_nodes()
        _, meta = split_test_collection_map(test_nodes_raw)
        expected_total = int((meta or {}).get("total_collected") or 0)
        latest: tuple[float, Path] | None = None
        for path in JUNIT_CANDIDATES:
            if not path.exists():
                continue
            testcase_count = count_junit_testcases(path)
            if expected_total and testcase_count and testcase_count != expected_total:
                continue
            parsed = load_junit_results(path)
            if not parsed:
                continue
            mtime = path.stat().st_mtime
            if latest is None or mtime > latest[0]:
                latest = (mtime, path)
        if latest is None:
            return None
        try:
            return str(latest[1].relative_to(ROOT)).replace("\\", "/")
        except ValueError:
            return str(latest[1])

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_returns_200_for_admin(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog", headers=headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "total" in body["data"]
        assert body["data"]["total"] > 0
        assert isinstance(body["data"]["features"], list)

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_exposes_junit_freshness_and_latest_valid_source(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=live", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["test_result_freshness"] in {"fresh", "stale", "missing"}
        summary = data["test_collection_summary"]
        assert isinstance(summary, dict)
        assert isinstance(summary.get("total_collected"), int)
        assert summary["mapped_by_feature_marker"] >= 1
        assert (
            summary["mapped_by_feature_marker"]
            + summary["mapped_by_fr_inference"]
            + summary["unmapped"]
            == summary["total_collected"]
        )
        expected_source = self._expected_latest_valid_junit_source()
        if expected_source is None:
            assert data["test_result_source"] is None
            assert data["test_result_generated_at"] is None
            assert data["test_result_age_seconds"] is None
            assert data["test_result_freshness"] in {"missing", "stale"}
            return

        assert data["test_result_source"] == expected_source
        assert data["test_result_generated_at"] is not None
        assert isinstance(data["test_result_age_seconds"], int)
        assert data["test_result_age_seconds"] >= 0

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_exposes_test_collection_summary(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=snapshot", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        summary = data["test_collection_summary"]
        assert isinstance(summary, dict)
        assert isinstance(summary["total_collected"], int)
        assert "mapped_by_feature_marker" in summary
        assert "mapped_by_fr_inference" in summary
        assert "unmapped" in summary

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_governance_catalog_alias_returns_200_for_admin(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/governance/catalog", headers=headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["data"]["source"] == "live"
        assert isinstance(body["data"]["features"], list)

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_governance_catalog_snapshot_source_returns_snapshot(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/governance/catalog?source=snapshot", headers=headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["data"]["source"] == "snapshot"
        assert body["data"]["catalog_mode"] == "snapshot"
        summary = body["data"]["test_collection_summary"]
        assert isinstance(summary, dict)
        assert summary["mapped_by_feature_marker"] >= 1
        assert isinstance(body["data"]["features"], list)

    @pytest.mark.feature("FR10-FEATURE-01")
    @pytest.mark.parametrize("source", ["live", "snapshot"])
    def test_catalog_never_claims_pages_without_runtime_page_path(self, client, create_user, source):
        headers = self._login(client, create_user, "admin")
        resp = client.get(f"/api/v1/features/catalog?source={source}", headers=headers)
        assert resp.status_code == 200
        features = resp.json()["data"]["features"]
        offenders = sorted(
            item["feature_id"]
            for item in features
            if not item.get("runtime_page_path") and item.get("page_exists")
        )
        assert not offenders, "features without runtime_page_path must not claim page_exists=true:\n" + "\n".join(offenders)

    @pytest.mark.feature("FR10-FEATURE-01")
    @pytest.mark.feature("FR02-SCHED-02")
    @pytest.mark.feature("FR09-BILLING-03")
    @pytest.mark.feature("FR09B-CLEAN-01")
    @pytest.mark.feature("FR09B-CLEAN-02")
    def test_snapshot_catalog_retired_entries_still_out_of_ssot(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/governance/catalog?source=snapshot", headers=headers)
        assert resp.status_code == 200
        payload = resp.json()["data"]
        features = payload["features"]
        by_id = {item["feature_id"]: item for item in features}
        retired = by_id["FR02-SCHED-02"]
        fail_close = by_id["FR09-BILLING-03"]
        cleanup_report_clear = by_id["FR09B-CLEAN-01"]
        cleanup_stats_clear = by_id["FR09B-CLEAN-02"]
        assert retired["structural_status"] == "OUT_OF_SSOT"
        assert retired["route_exists"] is False
        assert retired["test_nodeids"] == []
        assert fail_close["structural_status"] == "OUT_OF_SSOT"
        assert fail_close["route_exists"] is False
        assert fail_close["test_nodeids"] == []
        assert cleanup_report_clear["structural_status"] == "OUT_OF_SSOT"
        assert cleanup_report_clear["route_exists"] is False
        assert cleanup_report_clear["test_nodeids"] == []
        assert cleanup_stats_clear["structural_status"] == "OUT_OF_SSOT"
        assert cleanup_stats_clear["route_exists"] is False
        assert cleanup_stats_clear["test_nodeids"] == []
        negative_summary = payload["negative_status_summary"]
        assert negative_summary["retired_route"] >= 3
        assert negative_summary["fail_close_route"] >= 1
        assert negative_summary["negative_total"] >= 4

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_surfaces_live_stale_reason(self, client, create_user, monkeypatch):
        import app.api.routes_governance as routes_governance

        monkeypatch.setattr(
            routes_governance,
            "get_live_governance_catalog",
            lambda force_refresh=False: {
                "catalog_mode": "live",
                "generated_at": "2026-03-23T09:00:00+00:00",
                "registry_generated_at": "2026-03-23T08:00:00+00:00",
                "test_result_source": None,
                "test_result_generated_at": None,
                "test_result_freshness": "stale",
                "test_result_age_seconds": None,
                "test_result_stale_reason": "testcase_count_mismatch:2!=1",
                "test_collection_summary": {
                    "total_collected": 2,
                    "mapped_by_feature_marker": 1,
                    "mapped_by_fr_inference": 0,
                    "unmapped": 1,
                },
                "status_summary": {"READY": 1},
                "features": [
                    {
                        "feature_id": "FR10-HOME-01",
                        "fr_id": "FR-10",
                        "title": "首页概览",
                        "visibility": "public",
                        "structural_status": "READY",
                        "mismatch_flags": [],
                        "route_exists": True,
                        "page_exists": True,
                        "test_nodeids": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
                        "last_test_status": "UNKNOWN",
                        "last_verified_at": None,
                    }
                ],
            },
        )

        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=live", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["test_result_freshness"] == "stale"
        assert data["test_result_stale_reason"] == "testcase_count_mismatch:2!=1"
        assert data["test_result_source"] is None
        assert data["test_collection_summary"] == {
            "total_collected": 2,
            "mapped_by_feature_marker": 1,
            "mapped_by_fr_inference": 0,
            "unmapped": 1,
        }
        assert data["latest_feature_verified_at"] is None
        assert data["negative_status_summary"]["negative_total"] == 0

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_preserves_explicit_null_latest_feature_timestamp_when_stale(
        self,
        client,
        create_user,
        monkeypatch,
    ):
        import app.api.routes_governance as routes_governance

        monkeypatch.setattr(
            routes_governance,
            "get_live_governance_catalog",
            lambda force_refresh=False: {
                "catalog_mode": "live",
                "generated_at": "2026-03-23T09:00:00+00:00",
                "registry_generated_at": "2026-03-23T08:00:00+00:00",
                "test_result_source": None,
                "test_result_generated_at": None,
                "test_result_freshness": "stale",
                "test_result_age_seconds": None,
                "test_result_stale_reason": "testcase_count_mismatch:2!=1",
                "latest_feature_verified_at": None,
                "test_collection_summary": {
                    "total_collected": 2,
                    "mapped_by_feature_marker": 1,
                    "mapped_by_fr_inference": 0,
                    "unmapped": 1,
                },
                "status_summary": {"READY": 1},
                "features": [
                    {
                        "feature_id": "FR10-HOME-01",
                        "fr_id": "FR-10",
                        "title": "home",
                        "visibility": "public",
                        "structural_status": "READY",
                        "mismatch_flags": [],
                        "route_exists": True,
                        "page_exists": True,
                        "test_nodeids": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
                        "last_test_status": "PASS",
                        "last_verified_at": "2026-03-22T08:15:00+00:00",
                    }
                ],
            },
        )

        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=live", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["test_result_freshness"] == "stale"
        assert data["latest_feature_verified_at"] is None

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_catalog_api_surfaces_doc22_audit_scope_summary(self, client, create_user, monkeypatch):
        import app.api.routes_governance as routes_governance

        monkeypatch.setattr(
            routes_governance,
            "get_live_governance_catalog",
            lambda force_refresh=False: {
                "catalog_mode": "live",
                "generated_at": "2026-03-23T09:00:00+00:00",
                "registry_generated_at": "2026-03-23T08:00:00+00:00",
                "status_summary": {"READY": 2, "OUT_OF_SSOT": 2},
                "features": [
                    {
                        "feature_id": "FR09-AUTH-05",
                        "fr_id": "FR-09",
                        "title": "OAuth 第三方登录",
                        "visibility": "public",
                        "structural_status": "READY",
                        "mismatch_flags": [],
                        "route_exists": True,
                        "page_exists": True,
                        "test_nodeids": [],
                        "last_test_status": "PASS",
                        "last_verified_at": "2026-03-22T08:15:00+00:00",
                        "primary_api": {"method": "GET", "path": "/auth/oauth/providers"},
                    },
                    {
                        "feature_id": "FR06-LLM-GEMINI-01",
                        "fr_id": "FR-06",
                        "title": "Gemini Provider 分析",
                        "visibility": "internal",
                        "structural_status": "READY",
                        "mismatch_flags": [],
                        "route_exists": True,
                        "page_exists": False,
                        "test_nodeids": [],
                        "last_test_status": "PASS",
                        "last_verified_at": "2026-03-22T08:15:00+00:00",
                        "primary_api": {"method": "POST", "path": "/api/v1/gemini/analyze"},
                    },
                    {
                        "feature_id": "OOS-MOCK-PAY-01",
                        "fr_id": "FR-09",
                        "title": "Mock 支付页",
                        "visibility": "out_of_ssot",
                        "structural_status": "OUT_OF_SSOT",
                        "mismatch_flags": ["out_of_ssot"],
                        "route_exists": False,
                        "page_exists": False,
                        "test_nodeids": [],
                        "last_test_status": "UNKNOWN",
                        "last_verified_at": None,
                        "primary_api": {"method": "GET", "path": "/billing/mock-pay/{order_id}"},
                    },
                    {
                        "feature_id": "FR02-SCHED-02",
                        "fr_id": "FR-02",
                        "title": "DAG 重触发",
                        "visibility": "admin",
                        "structural_status": "OUT_OF_SSOT",
                        "mismatch_flags": ["RETIRED_ROUTE"],
                        "route_exists": False,
                        "page_exists": False,
                        "test_nodeids": [],
                        "last_test_status": "UNKNOWN",
                        "last_verified_at": None,
                        "primary_api": {"method": "POST", "path": "/api/v1/admin/dag/retrigger"},
                    },
                ],
            },
        )

        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=live", headers=headers)
        assert resp.status_code == 200
        summary = resp.json()["data"]["audit_scope_summary"]
        assert summary["catalog_total"] == 4
        assert summary["governance_negative_total"] == 2
        assert summary["governance_eligible_total"] == 2
        assert summary["doc22_code_presence_total"] == 3
        assert summary["doc22_excluded_total"] == 3
        assert summary["doc22_active_total"] == 1
        assert summary["mock_pay_oos_total"] == 1
        assert summary["provider_policy_oos_total"] == 1
        assert summary["external_blocker_total"] == 1
        assert summary["governance_terminal_total"] == 1
        assert summary["bucket_feature_ids"]["external_blocker"] == ["FR09-AUTH-05"]
        assert summary["bucket_feature_ids"]["governance_terminal"] == ["FR02-SCHED-02"]

    @pytest.mark.feature("FR10-FEATURE-01")
    @pytest.mark.feature("FR02-SCHED-02")
    @pytest.mark.feature("FR09-BILLING-03")
    @pytest.mark.feature("FR09B-CLEAN-01")
    @pytest.mark.feature("FR09B-CLEAN-02")
    def test_live_catalog_out_of_ssot_entries_do_not_claim_routes_or_tests(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/features/catalog?source=live", headers=headers)
        assert resp.status_code == 200
        features = resp.json()["data"]["features"]
        by_id = {item["feature_id"]: item for item in features}
        retired = by_id["FR02-SCHED-02"]
        assert retired["structural_status"] == "OUT_OF_SSOT"
        assert retired["route_exists"] is False
        assert retired["test_nodeids"] == []
        fail_close = by_id["FR09-BILLING-03"]
        assert fail_close["structural_status"] == "OUT_OF_SSOT"
        assert fail_close["route_exists"] is False
        assert fail_close["test_nodeids"] == []
        cleanup_report_clear = by_id["FR09B-CLEAN-01"]
        cleanup_stats_clear = by_id["FR09B-CLEAN-02"]
        assert cleanup_report_clear["structural_status"] == "OUT_OF_SSOT"
        assert cleanup_report_clear["route_exists"] is False
        assert cleanup_report_clear["test_nodeids"] == []
        assert cleanup_stats_clear["structural_status"] == "OUT_OF_SSOT"
        assert cleanup_stats_clear["route_exists"] is False
        assert cleanup_stats_clear["test_nodeids"] == []

    @pytest.mark.feature("FR10-FEATURE-01")
    @pytest.mark.feature("FR02-SCHED-02")
    @pytest.mark.feature("FR09-BILLING-03")
    @pytest.mark.feature("FR09B-CLEAN-01")
    @pytest.mark.feature("FR09B-CLEAN-02")
    def test_retired_routes_never_report_pass_or_ready(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/api/v1/governance/catalog?source=snapshot", headers=headers)
        assert resp.status_code == 200
        features = resp.json()["data"]["features"]
        guarded = [
            item
            for item in features
            if any(flag in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"} for flag in (item.get("mismatch_flags") or []))
        ]
        assert guarded, "expected retired/fail-close governance entries in snapshot"
        for item in guarded:
            assert item["structural_status"] == "OUT_OF_SSOT"
            assert item["last_test_status"] == "UNKNOWN"
            assert item["test_nodeids"] == []


@pytest.mark.feature("FR10-FEATURE-01")
def test_live_catalog_uses_feature_timestamp_for_last_verified_at(monkeypatch):
    import app.services.governance_catalog_live as live_catalog

    monkeypatch.setattr(
        live_catalog,
        "load_registry",
        lambda: {
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                }
            ],
        },
    )
    monkeypatch.setattr(live_catalog, "scan_fastapi_routes", lambda: {"GET /api/v1/home"})
    monkeypatch.setattr(live_catalog, "scan_html_templates", lambda: set())
    monkeypatch.setattr(
        live_catalog,
        "collect_pytest_nodes",
        lambda: {
            "FR10-HOME-01": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
    )
    monkeypatch.setattr(
        live_catalog,
        "load_latest_junit_results_bundle",
        lambda expected_total=None: (
            {"tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )
    monkeypatch.setattr(
        live_catalog,
        "_latest_junit_node_verified_at",
        lambda junit_source=None: {
            "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "2026-03-23T08:15:00+00:00"
        },
    )

    catalog = live_catalog._build_live_catalog()

    feature = catalog["features"][0]
    assert feature["last_test_status"] == "PASS"
    assert feature["last_verified_at"] == "2026-03-23T08:15:00+00:00"
    assert catalog["latest_feature_verified_at"] == "2026-03-23T08:15:00+00:00"


@pytest.mark.feature("FR10-FEATURE-01")
def test_live_catalog_aggregates_all_test_nodes_and_surfaces_sixth_failure(monkeypatch):
    import app.services.governance_catalog_live as live_catalog

    nodeids = [f"tests/test_fr10_site_dashboard.py::test_case_{idx}" for idx in range(1, 7)]
    monkeypatch.setattr(
        live_catalog,
        "load_registry",
        lambda: {
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
    )
    monkeypatch.setattr(live_catalog, "scan_fastapi_routes", lambda: {"GET /api/v1/home"})
    monkeypatch.setattr(live_catalog, "scan_html_templates", lambda: set())
    monkeypatch.setattr(
        live_catalog,
        "collect_pytest_nodes",
        lambda: {
            "FR10-HOME-01": nodeids,
            "__meta__": {
                "total_collected": len(nodeids),
                "mapped_by_feature_marker": len(nodeids),
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
    )
    monkeypatch.setattr(
        live_catalog,
        "load_latest_junit_results_bundle",
        lambda expected_total=None: (
            {**{nodeid: "PASS" for nodeid in nodeids[:5]}, nodeids[5]: "FAIL"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )
    monkeypatch.setattr(live_catalog, "_latest_junit_node_verified_at", lambda junit_source=None: {})

    catalog = live_catalog._build_live_catalog()

    feature = catalog["features"][0]
    assert feature["test_nodeids"] == nodeids
    assert feature["last_test_status"] == "FAIL"
    assert feature["last_verified_at"] is None


@pytest.mark.feature("FR10-FEATURE-01")
def test_live_catalog_marks_zero_feature_marker_coverage_as_stale(monkeypatch):
    import app.services.governance_catalog_live as live_catalog

    monkeypatch.setattr(
        live_catalog,
        "load_registry",
        lambda: {
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
    )
    monkeypatch.setattr(live_catalog, "scan_fastapi_routes", lambda: {"GET /api/v1/home"})
    monkeypatch.setattr(live_catalog, "scan_html_templates", lambda: set())
    monkeypatch.setattr(
        live_catalog,
        "collect_pytest_nodes",
        lambda: {
            "FR-10": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 0,
                "mapped_by_fr_inference": 1,
                "unmapped": 0,
            },
        },
    )
    monkeypatch.setattr(
        live_catalog,
        "load_latest_junit_results_bundle",
        lambda expected_total=None: (
            {"tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )
    monkeypatch.setattr(live_catalog, "_latest_junit_node_verified_at", lambda junit_source=None: {})

    catalog = live_catalog._build_live_catalog()

    assert catalog["test_result_freshness"] == "stale"
    assert catalog["test_result_stale_reason"] == "feature_marker_coverage_zero"
    feature = catalog["features"][0]
    assert feature["last_test_status"] == "PASS"
    assert feature["last_verified_at"] is None


@pytest.mark.feature("FR10-FEATURE-01")
@pytest.mark.feature("FR09-BILLING-03")
def test_live_catalog_marks_fail_close_routes_out_of_ssot(monkeypatch):
    import app.services.governance_catalog_live as live_catalog

    monkeypatch.setattr(
        live_catalog,
        "load_registry",
        lambda: {
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR09-BILLING-03",
                    "fr_id": "FR-09",
                    "title": "订阅状态查询",
                    "visibility": "public",
                    "governance_flags": ["FAIL_CLOSE_ROUTE"],
                    "primary_api": {"method": "GET", "path": "/api/v1/membership/subscription/status"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
    )
    monkeypatch.setattr(live_catalog, "scan_fastapi_routes", lambda: {"GET /api/v1/membership/subscription/status"})
    monkeypatch.setattr(live_catalog, "scan_html_templates", lambda: set())
    monkeypatch.setattr(
        live_catalog,
        "collect_pytest_nodes",
        lambda: {
            "FR09-BILLING-03": ["tests/test_fr09_auth.py::test_fr09_subscription_status_route_is_retired"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
    )
    monkeypatch.setattr(
        live_catalog,
        "load_latest_junit_results_bundle",
        lambda expected_total=None: (
            {"tests/test_fr09_auth.py::test_fr09_subscription_status_route_is_retired": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    catalog = live_catalog._build_live_catalog()

    feature = catalog["features"][0]
    assert feature["structural_status"] == "OUT_OF_SSOT"
    assert feature["mismatch_flags"] == ["FAIL_CLOSE_ROUTE"]
    assert feature["route_exists"] is False
    assert feature["test_nodeids"] == []
    assert feature["last_test_status"] == "UNKNOWN"
    assert catalog["negative_status_summary"]["fail_close_route"] == 1


def test_catalog_api_returns_401_for_anonymous(client):
    resp = client.get("/api/v1/features/catalog")
    assert resp.status_code in (401, 403)
