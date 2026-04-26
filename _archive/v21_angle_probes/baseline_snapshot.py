"""V21 Baseline DB snapshot - read-only.

Writes output/v21_baseline_<ts>.json with all SSOT counters.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "app.db"
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)


def q(cur: sqlite3.Cursor, sql: str, *params):
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        if len(rows) == 1 and len(rows[0]) == 1:
            return rows[0][0]
        return [tuple(r) for r in rows]
    except Exception as e:
        return f"ERR:{e}"


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = OUT / f"v21_baseline_{ts}.json"

    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cur = conn.cursor()

    snap: dict = {"ts_utc": ts, "db": str(DB)}

    # report
    snap["report"] = {
        "total": q(cur, "SELECT COUNT(*) FROM report"),
        "is_deleted_0": q(cur, "SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL"),
        "published_1": q(cur, "SELECT COUNT(*) FROM report WHERE published=1"),
        "conflict_deleted_but_published": q(
            cur, "SELECT COUNT(*) FROM report WHERE is_deleted=1 AND published=1"
        ),
        "quality_flag_dist": q(
            cur,
            "SELECT quality_flag, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) GROUP BY quality_flag",
        ),
        "visible_non_ok": q(
            cur,
            "SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND quality_flag != 'ok'",
        ),
        "visible_ok_published": q(
            cur,
            "SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND quality_flag='ok' AND published=1",
        ),
        "by_action_visible": q(
            cur,
            "SELECT recommended_action, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) GROUP BY recommended_action",
        ),
    }

    # settlement_result
    snap["settlement_result"] = {
        "total": q(cur, "SELECT COUNT(*) FROM settlement_result"),
        "by_window": q(
            cur,
            "SELECT window_days, settlement_status, COUNT(*) FROM settlement_result GROUP BY window_days, settlement_status",
        ),
    }

    # kline_daily
    snap["kline_daily"] = {
        "rows": q(cur, "SELECT COUNT(*) FROM kline_daily"),
        "stocks": q(cur, "SELECT COUNT(DISTINCT stock_code) FROM kline_daily"),
        "latest_trade_day": q(cur, "SELECT MAX(trade_day) FROM kline_daily"),
    }

    # hotspot
    for t in ("hotspot_raw", "hotspot_normalized", "hotspot_top50", "market_hotspot_item_source"):
        snap[t] = {"rows": q(cur, f"SELECT COUNT(*) FROM {t}")}

    # market_state_cache
    snap["market_state_cache"] = {
        "rows": q(cur, "SELECT COUNT(*) FROM market_state_cache"),
        "date_range": q(cur, "SELECT MIN(trade_day), MAX(trade_day) FROM market_state_cache"),
    }

    # report_data_usage
    snap["report_data_usage"] = {
        "rows": q(cur, "SELECT COUNT(*) FROM report_data_usage"),
        "reports_with_usage": q(
            cur, "SELECT COUNT(DISTINCT report_id) FROM report_data_usage"
        ),
    }

    # required datasets coverage for visible ok reports
    required = ("kline_daily", "hotspot_top50", "northbound_summary", "etf_flow_summary", "market_state_input")
    missing_map = {}
    for ds in required:
        missing_map[ds] = q(
            cur,
            f"""
            SELECT COUNT(*) FROM report r
            WHERE (r.is_deleted=0 OR r.is_deleted IS NULL)
              AND r.published=1 AND r.quality_flag='ok'
              AND NOT EXISTS (
                SELECT 1 FROM report_data_usage u
                WHERE u.report_id=r.id AND u.dataset=? AND (u.status='ok' OR u.status IS NULL)
              )
            """,
            ds,
        )
    snap["visible_ok_missing_required"] = missing_map

    # stock_pool
    snap["stock_pool"] = {
        "rows": q(cur, "SELECT COUNT(*) FROM stock_pool"),
    }
    snap["stock_pool_refresh_task"] = {
        "recent": q(
            cur,
            "SELECT status, source, source_date, created_at FROM stock_pool_refresh_task ORDER BY id DESC LIMIT 3",
        ),
    }

    # empty tables
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    empties = []
    for t in tables:
        try:
            cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            if cnt == 0:
                empties.append(t)
        except Exception:
            pass
    snap["empty_tables"] = {"count": len(empties), "names": empties}

    conn.close()

    out_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] wrote {out_path}")
    print(json.dumps({
        "report.is_deleted_0": snap["report"]["is_deleted_0"],
        "report.published_1": snap["report"]["published_1"],
        "report.conflict": snap["report"]["conflict_deleted_but_published"],
        "report.visible_non_ok": snap["report"]["visible_non_ok"],
        "report.visible_ok_published": snap["report"]["visible_ok_published"],
        "settlement_result.total": snap["settlement_result"]["total"],
        "kline.stocks": snap["kline_daily"]["stocks"],
        "kline.latest": snap["kline_daily"]["latest_trade_day"],
        "visible_ok_missing_required": snap["visible_ok_missing_required"],
        "empty_tables.count": snap["empty_tables"]["count"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
