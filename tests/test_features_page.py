"""Tests for the /features admin page."""

import builtins
from html.parser import HTMLParser
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient
from app.models import Report as _ReportModel
from app.services.governance_catalog_live import (
    get_live_governance_catalog as _default_get_live_governance_catalog,
)

pytestmark = [pytest.mark.feature("FR10-FEATURE-01")]
ROOT = Path(__file__).resolve().parents[1]
get_live_governance_catalog = _default_get_live_governance_catalog

if getattr(builtins, "Report", None) is None:
    builtins.Report = _ReportModel


def _fake_catalog():
    return {
        "generated_at": "2026-03-22T10:00:00+08:00",
        "catalog_mode": "live",
        "test_result_source": "output/junit.xml",
        "test_result_generated_at": "2026-03-22T09:50:00+08:00",
        "test_result_freshness": "fresh",
        "test_result_stale_reason": None,
        "latest_feature_verified_at": "2026-03-22T09:48:00+08:00",
        "negative_status_summary": {
            "negative_total": 0,
            "retired_route": 0,
            "fail_close_route": 0,
            "deprecated": 0,
            "out_of_ssot": 0,
        },
        "cache_ttl_seconds": 300,
        "total": 2,
        "denominator_summary": {
            "catalog_total": 2,
            "eligible_total": 2,
            "negative_total": 0,
            "ready_strict_count": 1,
            "ready_with_gaps_count": 1,
            "blocked_count": 0,
        },
        "feature_traceability_summary": {
            "feature_marker": 1,
            "fr_inference_fallback": 1,
            "unmapped": 0,
            "governance_excluded": 0,
        },
        "status_summary": {
            "READY": 1,
            "READY_WITH_GAPS": 1,
            "MISMATCH": 0,
            "OUT_OF_SSOT": 0,
        },
        "features": [
            {
                "feature_id": "FR10-HOME-01",
                "fr_id": "FR-10",
                "title": "首页概览",
                "structural_status": "READY",
                "visibility": "public",
                "primary_api": {"method": "GET", "path": "/api/v1/home"},
                "request_params": [{"name": "trade_date", "type": "string", "required": False, "description": "交易日"}],
                "default_example": {"curl": "curl http://localhost:8000/api/v1/home"},
                "key_response_fields": [{"name": "pool_size", "description": "股票池数量"}],
                "runtime_page_path": "/",
                "mismatch_flags": [],
                "last_test_status": "PASS",
                "last_verified_at": "2026-03-22T09:48:00+08:00",
                "test_nodeids": ["tests/test_doc_driven_verify.py::test_FR10_SITE_01_home_dom"],
                "ssot_refs": ["01§2.10", "05§11"],
                "test_traceability": {
                    "mapping_source": "feature_marker",
                    "mapping_note": "feature-level pytest markers map tests directly to this feature",
                    "exact_feature_node_count": 1,
                    "fr_inference_node_count": 0,
                },
            },
            {
                "feature_id": "FR10-FEATURE-01",
                "fr_id": "FR-10",
                "title": "页面与接口总览",
                "structural_status": "READY_WITH_GAPS",
                "visibility": "admin",
                "primary_api": {"method": "GET", "path": "/api/v1/features/catalog"},
                "request_params": [{"name": "fr_id", "type": "string", "required": False, "description": "按 FR 过滤"}],
                "default_example": {"curl": "curl http://localhost:8000/api/v1/features/catalog"},
                "key_response_fields": [{"name": "features", "description": "功能目录"}],
                "runtime_page_path": "/features",
                "mismatch_flags": [],
                "last_test_status": "UNKNOWN",
                "last_verified_at": None,
                "test_nodeids": ["tests/test_features_page.py::test_features_page_contains_fr_groups"],
                "ssot_refs": ["01§2.10", "05§11"],
                "test_traceability": {
                    "mapping_source": "fr_inference_fallback",
                    "mapping_note": "no direct feature marker found; tests are attached only through FR-level inference",
                    "exact_feature_node_count": 0,
                    "fr_inference_node_count": 1,
                },
            },
        ],
    }


class _FeaturePageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids: set[str] = set()
        self.stylesheets: list[str] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        attr_map = dict(attrs)
        if tag == "link" and attr_map.get("rel") == "stylesheet" and attr_map.get("href"):
            self.stylesheets.append(attr_map["href"])
        if attr_map.get("id"):
            self.ids.add(attr_map["id"])

    def handle_data(self, data):
        if data and data.strip():
            self.text_chunks.append(data.strip())


@pytest.fixture()
def create_user():
    def _create_user(*, email="user@test.com", password="Password123", role="user", **_kwargs):
        return {"user": SimpleNamespace(email=email, role=role), "password": password}

    return _create_user


@pytest.fixture()
def client():
    templates = Jinja2Templates(directory=str(ROOT / "app" / "web" / "templates"))
    templates.env.globals["api_base"] = "/api/v1"

    def _current_user(request: Request):
        token = (request.headers.get("Authorization") or "").strip()
        if token == "Bearer admin-token":
            return SimpleNamespace(role="admin", email="admin@test.com")
        if token == "Bearer user-token":
            return SimpleNamespace(role="user", email="user@test.com")
        return None

    app = FastAPI()
    app.mount("/static", StaticFiles(directory=str(ROOT / "app" / "web")), name="static")

    @app.get("/features", response_class=HTMLResponse)
    async def features_page(request: Request):
        from collections import defaultdict as _dd

        user = _current_user(request)
        if not user:
            return RedirectResponse(url="/login?next=/features", status_code=302)
        if (user.role or "").lower() not in {"admin", "super_admin"}:
            raise HTTPException(status_code=403, detail="闇€绠＄悊鍛樻潈闄?")
        catalog = get_live_governance_catalog()
        groups = _dd(list)
        for feat in catalog.get("features", []):
            groups[feat.get("fr_id", "UNKNOWN")].append(feat)
        groups = dict(sorted(groups.items(), key=lambda x: (x[0].replace("FR-", "").replace("-b", "z"), x[0])))
        status_summary = catalog.get("status_summary") or {}
        catalog_bridge = {
            "registry_total": catalog.get("total", 0),
            "ready_count": status_summary.get("READY", 0),
            "ready_with_gaps_count": status_summary.get("READY_WITH_GAPS", 0),
            "out_of_ssot_count": status_summary.get("OUT_OF_SSOT", 0),
            "progress_doc_path": "docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md",
            "fr10_name_bridge": [
                {"doc_id": "FR10-PAGE-01", "registry_ids": "FR10-HOME-01", "label": "home"},
                {"doc_id": "FR10-PAGE-02", "registry_ids": "FR10-LIST-01", "label": "list"},
                {"doc_id": "FR10-PAGE-03", "registry_ids": "FR10-DETAIL-01/02", "label": "detail"},
                {"doc_id": "FR10-PAGE-04~09", "registry_ids": "FR10-BOARD-01/02 + FR10-PLATFORM-01/02", "label": "dashboard"},
                {"doc_id": "FR10-PAGE-10", "registry_ids": "FR10-FEATURE-01", "label": "features"},
            ],
        }
        fr_names = {
            "FR-00": "鐪熷疄鎬х孩绾?",
            "FR-01": "鑲＄エ姹犵瓫閫?",
            "FR-02": "瀹氭椂璋冨害(DAG)",
            "FR-03": "Cookie涓庝細璇濈鐞?",
            "FR-04": "澶氭簮鏁版嵁閲囬泦",
            "FR-05": "甯傚満鐘舵€佹満",
            "FR-06": "鐮旀姤鐢熸垚",
            "FR-07": "棰勬祴缁撶畻涓庡洖鐏?",
            "FR-08": "妯℃嫙瀹炵洏杩借釜",
            "FR-09": "鍟嗕笟鍖栦笌鏉冪泭",
            "FR-09-b": "绯荤粺娓呯悊涓庡綊妗?",
            "FR-10": "瀹屾暣绔欑偣涓庣湅鏉?",
            "FR-11": "鐢ㄦ埛鍙嶉",
            "FR-12": "绠＄悊鍛樺悗鍙?",
            "FR-13": "涓氬姟浜嬩欢鎺ㄩ€?",
        }
        return templates.TemplateResponse(
            request,
            "features.html",
            {
                "current_user": user,
                "catalog": catalog,
                "groups": groups,
                "fr_names": fr_names,
                "catalog_bridge": catalog_bridge,
                "catalog_is_snapshot": str(catalog.get("catalog_mode") or "").lower() == "snapshot",
                "catalog_api_path": "/api/v1/features/catalog?source=live",
                "catalog_snapshot_api_path": "/api/v1/governance/catalog?source=snapshot",
                "admin_system_status_api_path": "/api/v1/admin/system-status",
                "health_api_path": "/health",
            },
        )

    with TestClient(app, base_url="http://localhost") as test_client:
        yield test_client


class TestFeaturesPage:
    @pytest.fixture(autouse=True)
    def _stub_catalog(self, monkeypatch):
        catalog = _fake_catalog()
        monkeypatch.setattr(sys.modules[__name__], "get_live_governance_catalog", lambda *args, **kwargs: catalog)

    @staticmethod
    def _login(client, create_user, role="admin"):
        del client, create_user
        return {"Authorization": f"Bearer {role}-token"}

    def test_features_page_200_for_admin(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        parser = _FeaturePageParser()
        parser.feed(resp.text)
        assert "catalog-bridge-summary" in parser.ids
        assert "/static/demo.css" in parser.stylesheets
        assert "/static/css/style.css" not in parser.stylesheets

    def test_features_page_stylesheet_is_real_asset(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        parser = _FeaturePageParser()
        parser.feed(resp.text)
        assert "/static/demo.css" in parser.stylesheets
        css = client.get("/static/demo.css")
        assert css.status_code == 200
        assert len(css.text.strip()) > 100

    def test_features_page_redirect_for_anonymous(self, client):
        resp = client.get("/features", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers.get("location", "")

    def test_features_page_403_for_user(self, client, create_user):
        headers = self._login(client, create_user, "user")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 403

    def test_features_page_contains_fr_groups(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        parser = _FeaturePageParser()
        parser.feed(resp.text)
        joined = " ".join(parser.text_chunks)
        joined_lower = joined.lower()
        assert "页面与接口总览" in joined
        assert "首页概览" in joined
        assert "待完善" in joined
        assert "catalog-bridge-summary" in parser.ids
        assert "fr10-name-bridge" in parser.ids
        assert "catalog-runtime-state" in parser.ids
        assert "catalog-sync-status" in parser.ids
        assert "catalog-runtime-anchor-strip" in parser.ids
        assert "catalog-runtime-truth-gaps" in parser.ids
        assert "catalog-test-freshness" in parser.ids
        assert "治理分母" in joined
        assert "测试映射" in joined
        assert "Runtime vs Governance" in joined
        assert "SSOT锚点" in joined
        summary_match = re.search(
            r'当前主统计口径.*?<strong>(\d+)</strong>',
            resp.text,
            flags=re.S,
        )
        assert summary_match is not None
        assert int(summary_match.group(1)) == 2
        assert "Negative bucket" in joined
        assert "2026-03-22T09:48:00+08:00" in joined
        assert "人工总表对照" in joined
        assert "22_" in joined
        assert "v7_" in joined
        assert "live catalog" not in joined_lower

    def test_features_page_uses_live_catalog_paths(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        parser = _FeaturePageParser()
        parser.feed(resp.text)
        assert "catalog-generated-at" in parser.ids
        assert re.search(r"/static/api-bridge\.js", resp.text)
        assert re.search(r"window\.__API_BASE__", resp.text)
        assert "页面数据来源" in " ".join(parser.text_chunks)

    def test_features_page_includes_runtime_refresh_script(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        required_patterns = (
            r"function\s+syncGovernanceCatalog",
            r"window\.ApiBridge",
            r"getFeaturesCatalog\('live'\)",
            r"getGovernanceCatalog\('snapshot'\)",
            r"getAdminSystemStatus\(\)",
            r"getHealthStatus\(\)",
            r"formatTruthGaps",
            r"catalog-runtime-truth-gaps",
            r"Promise\.allSettled",
            r"getSettledBridgeData",
            r"治理批次时间",
            r"最近 feature 验证",
            r"NORMAL:\s*'正常'",
            r"DEGRADED:\s*'已降级展示'",
        )
        for pattern in required_patterns:
            assert re.search(pattern, resp.text), pattern
        forbidden_patterns = (
            r"function\s+getSettledBridgeData",
            r"function\s+getBridgeData",
            r"readEnvelopeData",
            r"result\s*&&\s*result\.success\s*===\s*true\s*&&\s*result\.data",
            r"Object\.prototype\.hasOwnProperty\.call\(result,\s*'data'\)",
            r"status\s*===\s*['\"]fulfilled['\"]",
            r"fetch\(",
        )
        for pattern in forbidden_patterns:
            assert not re.search(pattern, resp.text), pattern

    def test_features_page_renders_search_and_filter_controls(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        assert 'id="featureSearch"' in resp.text
        assert 'data-filter="READY"' in resp.text
        assert 'data-vis="admin"' in resp.text

    def test_features_page_keeps_raw_test_nodeids_machine_readable_only(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        parser = _FeaturePageParser()
        parser.feed(resp.text)
        joined = " ".join(parser.text_chunks)
        assert "test_FR10_SITE_01_home_dom" not in joined
        assert "tests/test_features_page.py::test_features_page_contains_fr_groups" not in joined
        assert "01§2.10" not in joined
        assert "05§11" not in joined
        assert "已绑定 1 个测试节点" in joined
        assert "已绑定 2 个锚点" in joined
        assert 'data-test-count="1"' in resp.text
        assert "data-test-nodeids=" in resp.text
        assert "data-ssot-refs=" in resp.text

    def test_features_page_uses_canonical_denominator_and_traceability_data_attrs(self, client, create_user):
        headers = self._login(client, create_user, "admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        assert 'id="catalog-canonical-scope"' in resp.text
        assert 'data-catalog-total="2"' in resp.text
        assert 'data-eligible-total="2"' in resp.text
        assert 'data-negative-total="0"' in resp.text
        assert 'data-ready-with-gaps="1"' in resp.text
        assert 'id="catalog-traceability-summary"' in resp.text
        assert 'data-feature-marker="1"' in resp.text
        assert 'data-fr-fallback="1"' in resp.text
        assert 'data-mapping-source="feature_marker"' in resp.text
        assert 'data-mapping-source="fr_inference_fallback"' in resp.text


@pytest.mark.feature("FR02-SCHED-02")
def test_features_page_surfaces_retired_route_reason_without_claiming_ready(client, create_user, monkeypatch):
    catalog = _fake_catalog()
    catalog["features"].append(
        {
            "feature_id": "FR02-SCHED-02",
            "fr_id": "FR-02",
            "title": "DAG 重触发",
            "structural_status": "OUT_OF_SSOT",
            "visibility": "admin",
            "primary_api": {"method": "POST", "path": "/api/v1/admin/dag/retrigger"},
            "request_params": [],
            "runtime_page_path": "/admin",
            "mismatch_flags": ["RETIRED_ROUTE"],
            "last_test_status": "UNKNOWN",
            "last_verified_at": None,
        }
    )
    catalog["total"] = 3
    catalog["status_summary"]["OUT_OF_SSOT"] = 1
    catalog["negative_status_summary"]["negative_total"] = 1
    catalog["negative_status_summary"]["retired_route"] = 1
    catalog["denominator_summary"] = {
        "catalog_total": 3,
        "eligible_total": 2,
        "negative_total": 1,
        "ready_strict_count": 1,
        "ready_with_gaps_count": 1,
        "blocked_count": 0,
    }
    monkeypatch.setattr(sys.modules[__name__], "get_live_governance_catalog", lambda *args, **kwargs: catalog)

    headers = TestFeaturesPage._login(client, create_user, "admin")
    resp = client.get("/features", headers=headers)

    assert resp.status_code == 200
    parser = _FeaturePageParser()
    parser.feed(resp.text)
    joined = " ".join(parser.text_chunks)
    assert "退役路由，不计入 ready" in joined
    assert "测试状态" in joined
    assert "最近验证" in joined


def test_features_page_surfaces_governance_stale_reason(client, create_user, monkeypatch):
    catalog = _fake_catalog()
    catalog["test_result_freshness"] = "stale"
    catalog["test_result_stale_reason"] = "testcase_count_mismatch:1002!=1017"
    monkeypatch.setattr(sys.modules[__name__], "get_live_governance_catalog", lambda *args, **kwargs: catalog)

    headers = TestFeaturesPage._login(client, create_user, "admin")
    resp = client.get("/features", headers=headers)

    assert resp.status_code == 200
    parser = _FeaturePageParser()
    parser.feed(resp.text)
    joined = " ".join(parser.text_chunks)
    assert "testcase_count_mismatch:1002!=1017" in joined
    assert "stale" in joined


def test_features_page_does_not_backfill_feature_timestamp_when_catalog_is_stale(client, create_user, monkeypatch):
    catalog = _fake_catalog()
    catalog["test_result_freshness"] = "stale"
    catalog["test_result_source"] = None
    catalog["latest_feature_verified_at"] = None
    catalog["features"][0]["last_verified_at"] = "1999-01-01T00:00:00+00:00"
    monkeypatch.setattr(sys.modules[__name__], "get_live_governance_catalog", lambda *args, **kwargs: catalog)

    headers = TestFeaturesPage._login(client, create_user, "admin")
    resp = client.get("/features", headers=headers)

    assert resp.status_code == 200
    parser = _FeaturePageParser()
    parser.feed(resp.text)
    joined = " ".join(parser.text_chunks)
    assert "stale" in joined
    assert joined.count("1999-01-01T00:00:00+00:00") == 1


@pytest.mark.feature("FR02-SCHED-02")
def test_governance_builder_treats_retired_dag_route_as_out_of_ssot():
    from app.governance.build_feature_catalog import determine_status, scan_fastapi_routes

    feature = {
        "feature_id": "FR02-SCHED-02",
        "fr_id": "FR-02",
        "visibility": "admin",
        "primary_api": {"method": "POST", "path": "/api/v1/admin/dag/retrigger"},
        "required_test_kinds": ["api"],
    }

    status, flags = determine_status(
        feature,
        scan_fastapi_routes(),
        set(),
        {},
        {},
    )

    assert status == "OUT_OF_SSOT"
    assert "RETIRED_ROUTE" in flags
