from __future__ import annotations

import json

from scripts import rerun_router_reports


class _DummyResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _DummySession:
    def __init__(self):
        self.committed = 0
        self.rolled_back = 0

    def execute(self, stmt, params):
        del stmt
        assert params["trade_date"] == "2026-04-03"
        assert params["levels"] == ["local"]
        return _DummyResult([{"stock_code": "000001.SZ", "llm_fallback_level": "local"}])

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        return None


def test_rerun_router_reports_runs_single_stock(monkeypatch, capsys):
    session = _DummySession()
    monkeypatch.setattr(rerun_router_reports, "SessionLocal", lambda: session)
    monkeypatch.setattr(
        rerun_router_reports,
        "generate_report_ssot",
        lambda db, stock_code, trade_date, force_same_day_rebuild: {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "llm_fallback_level": "primary",
            "publish_status": "PUBLISHED",
            "confidence": 0.66,
        },
    )
    monkeypatch.setattr(
        rerun_router_reports,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "trade_date": "2026-04-03",
                "limit": 1,
                "include_failed": False,
                "disable_audit": True,
            },
        )(),
    )

    exit_code = rerun_router_reports.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["processed"] == 1
    assert payload["succeeded"] == 1
    assert payload["results"][0]["llm_fallback_level"] == "primary"
