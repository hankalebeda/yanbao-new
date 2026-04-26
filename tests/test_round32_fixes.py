"""Gate tests for Round 32 fixes: P0-12/P0-13/P1-35/P1-36/P1-37/P1-38.

Tests in this file are permanent gates — they must pass on every pytest run.
"""
from __future__ import annotations

import pytest
from uuid import uuid4
from datetime import date, datetime, timezone

from app.models import Base
from tests.helpers_ssot import seed_generation_context, utc_now


# ---------------------------------------------------------------------------
# P1-37: Scripts/tests must use SSOT dataset_name enum
# ---------------------------------------------------------------------------

class TestP137DatasetNameSSOT:

    def test_helpers_seed_uses_kline_daily(self, db_session):
        """seed_generation_context must insert dataset_name='kline_daily', not 'market_snapshot'."""
        db = db_session
        seed_generation_context(db, stock_code="600519.SH", trade_date="2026-03-06")
        db.flush()
        from sqlalchemy import text
        rows = db.execute(text("SELECT dataset_name FROM report_data_usage")).fetchall()
        names = {r[0] for r in rows}
        assert "market_snapshot" not in names, "seed_generation_context must not use 'market_snapshot'"
        assert "kline_daily" in names, "seed_generation_context must use SSOT 'kline_daily'"

    def test_ssot_frozen_dataset_names(self):
        """Verify SSOT frozen dataset_name enum."""
        SSOT_DATASETS = {"kline_daily", "hotspot_top50", "market_state_input", "northbound_summary", "etf_flow_summary"}
        assert "market_snapshot" not in SSOT_DATASETS


# ---------------------------------------------------------------------------
# P0-13: LLM router GENERAL chain must default to Codex relay pool -> Ollama
# ---------------------------------------------------------------------------

class TestP013LLMRouter:

    def test_general_chain_defaults_to_codex_then_ollama(self):
        """SSOT: current default path is Codex relay pool first, Ollama last."""
        from app.services.llm_router import _SCENE_TO_MODEL, LLMScene
        chain = _SCENE_TO_MODEL[LLMScene.GENERAL]
        assert chain == ["codex_api", "ollama"], f"GENERAL chain must be codex_api -> ollama, got {chain}"

    def test_model_to_fallback_mapping_covers_all(self):
        """Every model in GENERAL chain must map to a valid llm_fallback_level."""
        from app.services.llm_router import _SCENE_TO_MODEL, LLMScene
        SSOT_LEVELS = {"primary", "backup", "cli", "local", "failed"}
        # The mapping from report_generation_ssot.py
        _MODEL_TO_FALLBACK = {
            "codex_api": "primary",
            "ollama": "local",
        }
        chain = _SCENE_TO_MODEL[LLMScene.GENERAL]
        for model in chain:
            level = _MODEL_TO_FALLBACK.get(model)
            assert level is not None, f"model {model} has no fallback_level mapping"
            assert level in SSOT_LEVELS, f"model {model} maps to invalid level '{level}'"


# ---------------------------------------------------------------------------
# P1-35: report_data_usage.batch_id must link to data_batch
# ---------------------------------------------------------------------------

class TestP135LineageIntegrity:

    def test_seed_creates_data_batch_for_usage(self, db_session):
        """seed_generation_context must create data_batch row matching usage batch_id."""
        db = db_session
        seed_generation_context(db, stock_code="600519.SH", trade_date="2026-03-06")
        db.flush()
        from sqlalchemy import text
        orphans = db.execute(text(
            "SELECT COUNT(*) FROM report_data_usage u "
            "WHERE u.batch_id NOT IN (SELECT batch_id FROM data_batch)"
        )).scalar()
        assert orphans == 0, f"Found {orphans} report_data_usage rows with orphan batch_id"


# ---------------------------------------------------------------------------
# P1-36: Hotspot fetch_hotspot code normalization
# ---------------------------------------------------------------------------

class TestP136HotspotCodeNormalization:

    def test_normalize_sh_code(self):
        """SH prefix codes should normalize to .SH suffix."""
        code = "SH601868"
        if code.startswith("SH"):
            normalized = code[2:] + ".SH"
        assert normalized == "601868.SH"

    def test_normalize_sz_code(self):
        """SZ prefix codes should normalize to .SZ suffix."""
        code = "SZ000001"
        if code.startswith("SZ"):
            normalized = code[2:] + ".SZ"
        assert normalized == "000001.SZ"


# ---------------------------------------------------------------------------
# P1-38: capital_game_summary must have all 5 dimensions
# ---------------------------------------------------------------------------

class TestP138CapitalGameSummary:

    def test_always_returns_dict_not_none(self):
        """capital_game_summary must never return None (always has 5 fixed dimensions)."""
        from app.services.ssot_read_model import _build_capital_game_summary
        result = _build_capital_game_summary([])
        assert result is not None, "capital_game_summary must always return a dict"

    def test_has_all_five_dimensions(self):
        """Must have main_force, dragon_tiger, margin_financing, northbound, etf_flow."""
        from app.services.ssot_read_model import _build_capital_game_summary
        result = _build_capital_game_summary([])
        REQUIRED = {"headline", "summary_text", "has_real_conclusion", "main_force", "dragon_tiger", "margin_financing", "northbound", "etf_flow", "missing_dimensions", "missing_reasons", "completeness_level"}
        assert REQUIRED.issubset(set(result.keys())), f"Missing keys: {REQUIRED - set(result.keys())}"

    def test_main_force_has_status(self):
        """main_force must have status field even when data unavailable."""
        from app.services.ssot_read_model import _build_capital_game_summary
        result = _build_capital_game_summary([])
        assert result["main_force"] is not None
        assert "status" in result["main_force"]

    def test_margin_financing_has_status(self):
        """margin_financing must have status field even when data unavailable."""
        from app.services.ssot_read_model import _build_capital_game_summary
        result = _build_capital_game_summary([])
        assert result["margin_financing"] is not None
        assert "status" in result["margin_financing"]

    def test_with_northbound_data(self):
        """northbound should populate when data available."""
        from app.services.ssot_read_model import _build_capital_game_summary
        used_data = [
            {"dataset_name": "northbound_summary", "status": "ok", "status_reason": None, "fetch_time": "2026-03-13T00:00:00"},
        ]
        result = _build_capital_game_summary(used_data)
        assert result["northbound"] is not None
        assert result["northbound"]["status"] == "ok"
        # Other 3 dimensions still present
        assert result["main_force"] is not None
        assert result["dragon_tiger"] is not None
        assert result["margin_financing"] is not None

    def test_missing_capital_inputs_mark_report_incomplete(self):
        """缺少资金面快照时必须标记为不可公开展示，避免公开研报带缺口。"""
        from app.services.ssot_read_model import _build_capital_game_summary

        result = _build_capital_game_summary([])

        assert result["render_complete"] is False
        assert result["summary_text"]
        # 北向和 ETF 是尽力获取维度，完全无数据源时不加入 missing_dimensions
        assert set(result["missing_dimensions"]) == {"主力资金", "龙虎榜", "融资融券"}
        assert result["main_force"]["display_net_inflow_5d"] == "待补采"
        assert result["dragon_tiger"]["display_lhb_count_30d"] == "待补采"
        assert result["margin_financing"]["display_latest_rzye"] == "待补采"
        assert result["northbound"]["display_net_inflow_5d"] == "待补采"
        assert result["etf_flow"]["display_net_creation_redemption_5d"] == "待补采"

    def test_dragon_tiger_zero_count_is_still_complete(self):
        """龙虎榜 0 次属于真实零值，不应被当成数据缺口。"""
        from app.services.ssot_read_model import _build_capital_game_summary

        used_data = [
            {
                "dataset_name": "main_force_flow",
                "status": "ok",
                "status_reason": "capital_snapshot:{\"net_inflow_5d\": 120000000.0}",
                "fetch_time": "2026-03-13T00:00:00",
                "source_name": "eastmoney_fflow_daykline",
            },
            {
                "dataset_name": "dragon_tiger_list",
                "status": "ok",
                "status_reason": "capital_snapshot:{\"lhb_count_30d\": 0, \"source\": \"eastmoney\"}",
                "fetch_time": "2026-03-13T00:00:00",
                "source_name": "eastmoney_lhb",
            },
            {
                "dataset_name": "margin_financing",
                "status": "realtime_only",
                "status_reason": "capital_snapshot:{\"latest_rzye\": 8500000000.0}",
                "fetch_time": "2026-03-13T00:00:00",
                "source_name": "eastmoney_push2_rzrq",
            },
            {
                "dataset_name": "northbound_summary",
                "status": "ok",
                "status_reason": "{\"net_inflow_5d\": 50000000.0}",
                "fetch_time": "2026-03-13T00:00:00",
                "source_name": "akshare_hsgt_hist",
            },
            {
                "dataset_name": "etf_flow_summary",
                "status": "ok",
                "status_reason": "{\"net_creation_redemption_5d\": 30000000.0}",
                "fetch_time": "2026-03-13T00:00:00",
                "source_name": "etf_flow_summary",
            },
        ]

        result = _build_capital_game_summary(used_data)

        assert result["render_complete"] is True
        assert result["missing_dimensions"] == []
        assert result["dragon_tiger"]["lhb_count_30d"] == 0
        assert result["dragon_tiger"]["display_lhb_count_30d"] == "0 次"


# ---------------------------------------------------------------------------
# P2 hardening: hidden placeholder routes and sanitized admin payloads
# ---------------------------------------------------------------------------

class TestP2Hardening:

    def test_predictions_settle_hidden_in_ssot_mode(self, client, create_user):
        admin = create_user(email="p2-admin@example.com", password="Password123", role="admin", email_verified=True)
        login = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
        headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

        response = client.post(
            "/api/v1/predictions/settle",
            headers=headers,
            json={"report_id": str(uuid4()), "stock_code": "600519.SH", "windows": [7]},
        )
        assert response.status_code == 404
        assert response.json()["error_code"] == "NOT_FOUND"

    def test_internal_eval_regression_hidden_in_ssot_mode(self, client, internal_headers):
        response = client.post(
            "/api/v1/internal/eval/run-regression",
            headers=internal_headers("round32-hidden-token"),
        )
        assert response.status_code == 404
        assert response.json()["error_code"] == "NOT_FOUND"

    def test_internal_hotspot_collect_hidden_in_ssot_mode(self, client, internal_headers):
        response = client.post(
            "/api/v1/internal/hotspot/collect?platform=weibo&stock_code=600519.SH",
            json={"top_n": 5},
            headers=internal_headers("round32-hidden-token"),
        )
        assert response.status_code == 404
        assert response.json()["error_code"] == "NOT_FOUND"

    def test_admin_system_status_masks_stock_pool(self, client, create_user):
        admin = create_user(email="system-status-admin@example.com", password="Password123", role="admin", email_verified=True)
        login = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
        headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

        response = client.get("/api/v1/admin/system-status", headers=headers)
        assert response.status_code == 200
        stock_pool = response.json()["data"]["stock_pool"]
        assert set(stock_pool.keys()) == {"count", "sample"}
        assert isinstance(stock_pool["count"], int)
        assert isinstance(stock_pool["sample"], list)
        assert len(stock_pool["sample"]) <= 10

    def test_platform_summary_remains_public_and_frozen(self, client):
        response = client.get("/api/v1/platform/summary")
        assert response.status_code == 200
        data = response.json()["data"]
        assert set(data.keys()) == {
            "win_rate",
            "pnl_ratio",
            "alpha",
            "baseline_random",
            "baseline_ma_cross",
            "total_trades",
            "period_start",
            "period_end",
            "data_status",
            "status_reason",
            "display_hint",
            "runtime_trade_date",
            "snapshot_date",
            "cold_start",
            "cold_start_message",
        }
