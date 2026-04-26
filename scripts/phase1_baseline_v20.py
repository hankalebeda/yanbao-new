"""v20 baseline snapshot — 研报真实可用率硬口径

硬定义:
- quality_flag='ok'
- llm_fallback_level='primary'
- published=1 AND is_deleted=0
- 必备 5 数据集存在 + status='ok'
- kline_daily(stock_code, trade_date) 有行
- market_state_cache(trade_date).market_state_degraded=0
- stock_master(stock_code) 存在

Output: output/baseline_v20_<timestamp>.json
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# NO_PROXY for LAN / offline DB access
os.environ.setdefault("NO_PROXY", "*")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402
from app.core.db import SessionLocal  # noqa: E402

REQUIRED_DATASETS = (
    "kline_daily",
    "hotspot_top50",
    "northbound_summary",
    "etf_flow_summary",
    "market_state_input",
)


def one(db, sql, **kw):
    row = db.execute(text(sql), kw).fetchone()
    return row[0] if row else None


def rows(db, sql, **kw):
    return [dict(r._mapping) for r in db.execute(text(sql), kw).fetchall()]


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = ROOT / "output" / f"baseline_v20_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    try:
        snap: dict = {
            "schema": "v20",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "hard_gate_definition": {
                "quality_flag": "ok",
                "llm_fallback_level": "primary",
                "published": 1,
                "is_deleted": 0,
                "required_datasets": list(REQUIRED_DATASETS),
                "required_dataset_status": "ok",
                "market_state_degraded": 0,
            },
        }

        # ---- reports global ----
        snap["reports"] = {
            "total": one(db, "SELECT COUNT(*) FROM report"),
            "is_deleted_1": one(db, "SELECT COUNT(*) FROM report WHERE is_deleted=1"),
            "is_deleted_0": one(db, "SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL"),
            "published_1": one(db, "SELECT COUNT(*) FROM report WHERE published=1"),
            "visible": one(
                db,
                "SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1",
            ),
            "conflict_deleted_and_published": one(
                db,
                "SELECT COUNT(*) FROM report WHERE is_deleted=1 AND published=1",
            ),
        }

        # by quality_flag (visible)
        snap["reports"]["by_quality_flag_visible"] = rows(
            db,
            """
            SELECT COALESCE(quality_flag,'null') AS quality_flag, COUNT(*) AS n
            FROM report
            WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1
            GROUP BY quality_flag
            ORDER BY n DESC
            """,
        )
        snap["reports"]["by_llm_fallback_level_visible"] = rows(
            db,
            """
            SELECT COALESCE(llm_fallback_level,'null') AS level, COUNT(*) AS n
            FROM report
            WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1
            GROUP BY llm_fallback_level
            ORDER BY n DESC
            """,
        )
        snap["reports"]["by_trade_date_recent"] = rows(
            db,
            """
            SELECT trade_date,
                   COUNT(*) AS n,
                   SUM(CASE WHEN quality_flag='ok' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN llm_fallback_level='primary' THEN 1 ELSE 0 END) AS primary_llm
            FROM report
            WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1
              AND trade_date >= '2026-03-01'
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 40
            """,
        )

        # ---- hard-gate pass rate (visible) ----
        hard_pass = one(
            db,
            """
            SELECT COUNT(*) FROM report r
            WHERE (r.is_deleted=0 OR r.is_deleted IS NULL)
              AND r.published=1
              AND r.quality_flag='ok'
              AND r.llm_fallback_level='primary'
              AND EXISTS (
                SELECT 1 FROM stock_master sm WHERE sm.stock_code = r.stock_code
              )
              AND EXISTS (
                SELECT 1 FROM kline_daily kd
                WHERE kd.stock_code = r.stock_code AND kd.trade_date = r.trade_date
              )
              AND NOT EXISTS (
                SELECT d FROM (
                  SELECT 'kline_daily' AS d UNION ALL
                  SELECT 'hotspot_top50' UNION ALL
                  SELECT 'northbound_summary' UNION ALL
                  SELECT 'etf_flow_summary' UNION ALL
                  SELECT 'market_state_input'
                ) req
                WHERE NOT EXISTS (
                  SELECT 1 FROM report_data_usage_link l
                  JOIN report_data_usage u ON u.usage_id = l.usage_id
                  WHERE l.report_id = r.report_id
                    AND u.dataset_name = req.d
                    AND u.status = 'ok'
                )
              )
            """,
        )
        snap["reports"]["hard_gate_pass_visible"] = hard_pass
        snap["reports"]["hard_gate_fail_visible"] = (snap["reports"]["visible"] or 0) - (hard_pass or 0)

        # ---- kline ----
        snap["kline"] = {
            "total_rows": one(db, "SELECT COUNT(*) FROM kline_daily"),
            "distinct_stocks": one(db, "SELECT COUNT(DISTINCT stock_code) FROM kline_daily"),
            "latest_date": str(one(db, "SELECT MAX(trade_date) FROM kline_daily") or ""),
            "recent": rows(
                db,
                """
                SELECT trade_date, COUNT(*) AS rows_n, COUNT(DISTINCT stock_code) AS stocks_n
                FROM kline_daily
                WHERE trade_date >= '2026-03-01'
                GROUP BY trade_date ORDER BY trade_date DESC LIMIT 40
                """,
            ),
        }
        # coverage ratio: stocks_in_pool vs kline on pool trade_date
        pool_total = one(db, "SELECT COUNT(*) FROM stock_master WHERE (is_delisted=0 OR is_delisted IS NULL)")
        snap["kline"]["stock_master_alive"] = pool_total

        # ---- hotspot ----
        snap["hotspot"] = {
            "raw": one(db, "SELECT COUNT(*) FROM hotspot_raw"),
            "normalized": one(db, "SELECT COUNT(*) FROM hotspot_normalized"),
            "top50": one(db, "SELECT COUNT(*) FROM hotspot_top50"),
        }

        # ---- northbound / etf / market_state ----
        snap["market_state"] = {
            "total": one(db, "SELECT COUNT(*) FROM market_state_cache"),
            "latest": str(one(db, "SELECT MAX(trade_date) FROM market_state_cache") or ""),
            "degraded_rows": one(db, "SELECT COUNT(*) FROM market_state_cache WHERE market_state_degraded=1"),
            "recent": rows(
                db,
                """
                SELECT trade_date, market_state, market_state_degraded, state_reason
                FROM market_state_cache
                WHERE trade_date >= '2026-03-01'
                ORDER BY trade_date DESC LIMIT 40
                """,
            ),
        }

        # report_data_usage distribution
        snap["report_data_usage"] = {
            "total": one(db, "SELECT COUNT(*) FROM report_data_usage"),
            "by_dataset": rows(
                db,
                """
                SELECT dataset_name,
                       SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok,
                       SUM(CASE WHEN status<>'ok' THEN 1 ELSE 0 END) AS non_ok
                FROM report_data_usage
                GROUP BY dataset_name
                ORDER BY ok DESC
                """,
            ),
        }

        # settlement
        snap["settlement"] = {
            "task_total": one(db, "SELECT COUNT(*) FROM settlement_task"),
            "result_total": one(db, "SELECT COUNT(*) FROM settlement_result"),
            "prediction_outcome": one(db, "SELECT COUNT(*) FROM prediction_outcome"),
        }

        # stock pool
        snap["stock_pool"] = {
            "snapshot_total": one(db, "SELECT COUNT(*) FROM stock_pool_snapshot"),
            "refresh_task_total": one(db, "SELECT COUNT(*) FROM stock_pool_refresh_task"),
            "latest_snapshots": rows(
                db,
                """
                SELECT trade_date, COUNT(*) AS n
                FROM stock_pool_snapshot
                GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10
                """,
            ),
        }

        # cookie_session
        snap["cookie_session"] = {
            "total": one(db, "SELECT COUNT(*) FROM cookie_session"),
            "by_status": rows(
                db,
                "SELECT COALESCE(status,'null') AS status, COUNT(*) AS n FROM cookie_session GROUP BY status",
            ),
        }

        # audit log volume
        snap["audit_log"] = {
            "total": one(db, "SELECT COUNT(*) FROM audit_log"),
        }

        # sample failing visible reports (up to 20)
        snap["reports"]["sample_hard_gate_fails"] = rows(
            db,
            """
            SELECT report_id, stock_code, trade_date, quality_flag, llm_fallback_level, publish_status
            FROM report
            WHERE (is_deleted=0 OR is_deleted IS NULL)
              AND published=1
              AND (quality_flag <> 'ok' OR llm_fallback_level <> 'primary' OR quality_flag IS NULL OR llm_fallback_level IS NULL)
            ORDER BY trade_date DESC, created_at DESC
            LIMIT 20
            """,
        )

        out_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"[ok] baseline written: {out_path}")
        # echo key numbers for quick terminal sanity
        r = snap["reports"]
        print(
            f"visible={r['visible']} | hard_gate_pass={r['hard_gate_pass_visible']} | "
            f"hard_gate_fail={r['hard_gate_fail_visible']} | "
            f"conflict(del&pub)={r['conflict_deleted_and_published']}"
        )
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
