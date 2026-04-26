import json
import os
import sys
import urllib.request as u

sys.path.insert(0, ".")

from sqlalchemy import text
from app.core.db import SessionLocal

os.environ.pop("http_proxy", None)
os.environ.pop("HTTP_PROXY", None)
u.install_opener(u.build_opener(u.ProxyHandler({})))

BASE = "http://127.0.0.1:8010"
HDR = {
    "Content-Type": "application/json",
    "X-Internal-Token": "kestra-internal-20260327",
}


def db_snapshot(tag: str) -> dict:
    db = SessionLocal()
    try:
        out = {"tag": tag}
        out["visible_total"] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0")).scalar()
        out["visible_ok"] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok'")).scalar()
        out["visible_non_ok"] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag<>'ok'")).scalar()
        out["missing_required_usage_visible"] = db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM report r
                WHERE r.is_deleted=0
                  AND (
                    SELECT COUNT(DISTINCT du.dataset_name)
                    FROM report_data_usage_link l
                    JOIN report_data_usage du ON du.usage_id=l.usage_id
                    WHERE l.report_id=r.report_id
                      AND du.dataset_name IN ('kline_daily','hotspot_top50','northbound_summary','etf_flow_summary','market_state_input')
                      AND lower(COALESCE(du.status,''))='ok'
                  ) < 5
                """
            )
        ).scalar()
        out["published_deleted_conflict"] = db.execute(
            text("SELECT COUNT(*) FROM report WHERE is_deleted=1 AND COALESCE(published,0)=1")
        ).scalar()
        return out
    finally:
        db.close()


def call_json(method: str, path: str, payload: dict | None = None, timeout: int = 120) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = u.Request(BASE + path, data=data, headers=HDR, method=method)
    with u.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


if __name__ == "__main__":
    before = db_snapshot("before")

    # 1) cleanup incomplete/non-ok bundles via internal endpoint
    cleanup = call_json(
        "POST",
        "/api/v1/internal/reports/cleanup-incomplete-all",
        {
            "batch_limit": 500,
            "max_batches": 20,
            "dry_run": False,
            "include_non_ok": True,
        },
        timeout=300,
    )

    # 2) strict SQL soft-delete for any still-visible non-ok reports
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE report
                SET is_deleted=1, deleted_at=CURRENT_TIMESTAMP, published=0
                WHERE is_deleted=0
                  AND quality_flag<>'ok'
                """
            )
        )
        db.commit()
    finally:
        db.close()

    # 3) strict SQL soft-delete for visible reports that miss required usage
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE report
                SET is_deleted=1, deleted_at=CURRENT_TIMESTAMP, published=0
                WHERE is_deleted=0
                  AND report_id IN (
                    SELECT r.report_id
                    FROM report r
                    WHERE r.is_deleted=0
                      AND (
                        SELECT COUNT(DISTINCT du.dataset_name)
                        FROM report_data_usage_link l
                        JOIN report_data_usage du ON du.usage_id=l.usage_id
                        WHERE l.report_id=r.report_id
                          AND du.dataset_name IN ('kline_daily','hotspot_top50','northbound_summary','etf_flow_summary','market_state_input')
                          AND lower(COALESCE(du.status,''))='ok'
                      ) < 5
                  )
                """
            )
        )
        db.commit()
    finally:
        db.close()

    after = db_snapshot("after")

    print(json.dumps({"before": before, "cleanup": cleanup, "after": after}, ensure_ascii=False, indent=2))
