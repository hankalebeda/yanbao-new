from __future__ import annotations

import asyncio
import argparse
import os
import shutil
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ssot_schema import build_metadata

MIN_REBUILD_POOL_COVERAGE = 250
MIN_REBUILD_PUBLISHED_COVERAGE_RATIO = 0.6
MAX_REBUILD_UNPUBLISHED_RATIO = 0.05


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the SSOT runtime database from scratch.")
    parser.add_argument("--output", default="data/app.db.next", help="Temporary SQLite output path.")
    parser.add_argument("--runtime", default="data/app.db", help="Runtime SQLite path to replace.")
    parser.add_argument("--trade-date", default=None, help="Target trade date, YYYY-MM-DD.")
    parser.add_argument(
        "--history-days",
        type=int,
        default=60,
        help="Historical natural-day coverage for public dashboard rebuild.",
    )
    parser.add_argument("--history-top-n", type=int, default=200, help="Per historical trade day report count.")
    parser.add_argument("--kline-limit", type=int, default=120, help="Kline days fetched per stock.")
    parser.add_argument("--batch-size", type=int, default=50, help="Kline fetch batch size.")
    parser.add_argument("--pool-only-top", type=int, default=0, help="Optional stock limit for kline bootstrap.")
    parser.add_argument("--mock-llm", action="store_true", help="Force MOCK_LLM=true during report generation.")
    return parser.parse_args()


def sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.resolve().as_posix()}"


def build_empty_db(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    metadata, specs = build_metadata(None)
    engine = create_engine(sqlite_url(output_path))
    metadata.create_all(bind=engine)
    with sqlite3.connect(output_path) as conn:
        created_tables = {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}
    expected_tables = {spec.name for spec in specs}
    missing = sorted(expected_tables - created_tables)
    extra = sorted(created_tables - expected_tables)
    if missing or extra:
        raise RuntimeError(f"schema mismatch missing={missing} extra={extra}")


def configure_runtime_env(output_path: Path, *, mock_llm: bool) -> None:
    os.environ["DATABASE_URL"] = sqlite_url(output_path)
    if mock_llm:
        os.environ["MOCK_LLM"] = "true"
    os.environ["LLM_AUDIT_ENABLED"] = "false"


def seed_market_tables_from_runtime(runtime_path: Path, output_path: Path) -> dict[str, int]:
    if not runtime_path.exists():
        return {}

    table_names = (
        "stock_master",
        "kline_daily",
        "data_batch",
        "market_hotspot_item",
        "market_hotspot_item_source",
        "market_hotspot_item_stock_link",
    )
    copied_counts: dict[str, int] = {}
    with sqlite3.connect(output_path) as conn:
        conn.execute("ATTACH DATABASE ? AS runtime_seed", (str(runtime_path),))
        try:
            for table_name in table_names:
                source_exists = conn.execute(
                    "SELECT 1 FROM runtime_seed.sqlite_master WHERE type='table' AND name = ? LIMIT 1",
                    (table_name,),
                ).fetchone()
                if not source_exists:
                    continue
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM runtime_seed.{table_name}")
                copied_counts[table_name] = int(
                    conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                )
            conn.commit()
        finally:
            conn.execute("DETACH DATABASE runtime_seed")
    return copied_counts


def count_kline_coverage(db, *, trade_date_value: str) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(DISTINCT stock_code)
                FROM kline_daily
                WHERE trade_date = :trade_date
                """
            ),
            {"trade_date": trade_date_value},
        ).scalar_one()
    )


def resolve_trade_date_with_min_coverage(
    db,
    *,
    requested_trade_date: str,
    min_coverage: int,
) -> str:
    row = db.execute(
        text(
            """
            SELECT trade_date
            FROM kline_daily
            WHERE trade_date <= :requested_trade_date
            GROUP BY trade_date
            HAVING COUNT(DISTINCT stock_code) >= :min_coverage
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ),
        {"requested_trade_date": requested_trade_date, "min_coverage": min_coverage},
    ).first()
    if row is None:
        raise RuntimeError(
            f"no trade_date with kline coverage >= {min_coverage} on or before {requested_trade_date}"
        )
    value = row[0]
    return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]


def backfill_trade_date_klines(
    db,
    *,
    trade_date_value: str,
    limit: int,
    batch_size: int,
    delay_ms: int = 50,
) -> int:
    from app.models import DataBatch, KlineDaily, StockMaster
    from scripts.bootstrap_real_data import _fetch_batch

    missing_codes = [
        str(row[0])
        for row in db.execute(
            text(
                """
                SELECT sm.stock_code
                FROM stock_master sm
                WHERE sm.is_delisted = 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM kline_daily k
                      WHERE k.stock_code = sm.stock_code
                        AND k.trade_date = :trade_date
                  )
                ORDER BY sm.stock_code
                """
            ),
            {"trade_date": trade_date_value},
        ).fetchall()
    ]
    if not missing_codes:
        return 0

    trade_day = date.fromisoformat(trade_date_value)
    now = datetime.now(timezone.utc)
    batch_seq = int(
        db.execute(
            text(
                """
                SELECT COALESCE(MAX(batch_seq), 0) + 1
                FROM data_batch
                WHERE source_name = 'eastmoney'
                  AND trade_date = :trade_date
                  AND batch_scope = 'full_market_backfill'
                """
            ),
            {"trade_date": trade_date_value},
        ).scalar_one()
    )
    batch = DataBatch(
        batch_id=str(uuid4()),
        source_name="eastmoney",
        trade_date=trade_day,
        batch_scope="full_market_backfill",
        batch_seq=batch_seq,
        batch_status="RUNNING",
        quality_flag="ok",
        started_at=now,
        updated_at=now,
        created_at=now,
    )
    db.add(batch)
    db.commit()

    total_inserted = 0
    total_failed = 0
    for start in range(0, len(missing_codes), batch_size):
        chunk = missing_codes[start : start + batch_size]
        kline_data = asyncio.run(_fetch_batch(chunk, limit, delay_ms=delay_ms))
        total_failed += max(0, len(chunk) - len(kline_data))
        for code, klines in kline_data.items():
            sm = db.query(StockMaster).filter(StockMaster.stock_code == code).first()
            circ = float(sm.circulating_shares) if sm and sm.circulating_shares else None
            for k in klines:
                td = date.fromisoformat(k["date"])
                existing = db.query(KlineDaily).filter(
                    KlineDaily.stock_code == code,
                    KlineDaily.trade_date == td,
                ).first()
                if existing:
                    continue
                turnover = None
                if circ and circ > 0 and k["volume"]:
                    turnover = round(k["volume"] / circ, 6)
                db.add(
                    KlineDaily(
                        kline_id=str(uuid4()),
                        stock_code=code,
                        trade_date=td,
                        open=k["open"],
                        high=k["high"],
                        low=k["low"],
                        close=k["close"],
                        volume=k["volume"],
                        amount=k["amount"],
                        adjust_type="front_adjusted",
                        atr_pct=k.get("atr_pct"),
                        turnover_rate=turnover,
                        ma5=k.get("ma5"),
                        ma10=k.get("ma10"),
                        ma20=k.get("ma20"),
                        ma60=k.get("ma60"),
                        volatility_20d=k.get("volatility_20d"),
                        hs300_return_20d=None,
                        is_suspended=False,
                        source_batch_id=batch.batch_id,
                        created_at=now,
                    )
                )
                total_inserted += 1
        db.commit()

    batch.batch_status = "SUCCESS"
    batch.records_total = total_inserted + total_failed
    batch.records_success = total_inserted
    batch.records_failed = total_failed
    batch.covered_stock_count = count_kline_coverage(db, trade_date_value=trade_date_value)
    batch.finished_at = datetime.now(timezone.utc)
    batch.updated_at = datetime.now(timezone.utc)
    db.commit()
    return total_inserted


def ensure_benchmark_kline_history(
    db,
    *,
    benchmark_code: str,
    trade_date_value: str,
    limit: int,
    delay_ms: int = 50,
) -> int:
    from app.models import DataBatch, KlineDaily
    from scripts.bootstrap_real_data import _fetch_batch

    if not benchmark_code:
        return 0

    now = datetime.now(timezone.utc)
    trade_day = date.fromisoformat(trade_date_value)
    batch_seq = int(
        db.execute(
            text(
                """
                SELECT COALESCE(MAX(batch_seq), 0) + 1
                FROM data_batch
                WHERE source_name = 'eastmoney'
                  AND trade_date = :trade_date
                  AND batch_scope = 'benchmark_backfill'
                """
            ),
            {"trade_date": trade_date_value},
        ).scalar_one()
    )
    batch = DataBatch(
        batch_id=str(uuid4()),
        source_name="eastmoney",
        trade_date=trade_day,
        batch_scope="benchmark_backfill",
        batch_seq=batch_seq,
        batch_status="RUNNING",
        quality_flag="ok",
        covered_stock_count=1,
        core_pool_covered_count=0,
        started_at=now,
        updated_at=now,
        created_at=now,
    )
    db.add(batch)
    db.commit()

    inserted = 0
    kline_data = asyncio.run(_fetch_batch([benchmark_code], limit, delay_ms=delay_ms))
    for k in kline_data.get(benchmark_code, []):
        trade_date = date.fromisoformat(k["date"])
        existing = db.query(KlineDaily).filter(
            KlineDaily.stock_code == benchmark_code,
            KlineDaily.trade_date == trade_date,
        ).first()
        if existing:
            continue
        db.add(
            KlineDaily(
                kline_id=str(uuid4()),
                stock_code=benchmark_code,
                trade_date=trade_date,
                open=k["open"],
                high=k["high"],
                low=k["low"],
                close=k["close"],
                volume=k["volume"],
                amount=k["amount"],
                adjust_type="front_adjusted",
                atr_pct=k.get("atr_pct"),
                turnover_rate=None,
                ma5=k.get("ma5"),
                ma10=k.get("ma10"),
                ma20=k.get("ma20"),
                ma60=k.get("ma60"),
                volatility_20d=k.get("volatility_20d"),
                hs300_return_20d=None,
                is_suspended=False,
                source_batch_id=batch.batch_id,
                created_at=now,
            )
        )
        inserted += 1
    db.commit()

    batch.batch_status = "SUCCESS"
    batch.records_total = len(kline_data.get(benchmark_code, []))
    batch.records_success = batch.records_total
    batch.records_failed = 0
    batch.finished_at = datetime.now(timezone.utc)
    batch.updated_at = datetime.now(timezone.utc)
    db.commit()
    return inserted


def trade_dates_within_natural_window(
    db,
    *,
    target_trade_date: str,
    natural_days: int,
) -> list[str]:
    window_start = (date.fromisoformat(target_trade_date) - timedelta(days=max(natural_days - 1, 0))).isoformat()
    rows = db.execute(
        text(
            """
            SELECT DISTINCT trade_date
            FROM kline_daily
            WHERE trade_date BETWEEN :window_start AND :target_trade_date
            ORDER BY trade_date DESC
            """
        ),
        {"window_start": window_start, "target_trade_date": target_trade_date},
    ).fetchall()
    values = []
    for row in rows:
        raw = row[0]
        values.append(raw.isoformat() if hasattr(raw, "isoformat") else str(raw)[:10])
    return values


def ensure_report_usage_rows(db, *, trade_date_value: str, stock_codes: list[str]) -> int:
    inserted = 0
    ensured_batches: set[str] = set()
    for stock_code in stock_codes:
        kline_row = db.execute(
            text(
                """
                SELECT source_batch_id
                FROM kline_daily
                WHERE stock_code = :stock_code AND trade_date = :trade_date
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_date_value},
        ).first()
        if not kline_row:
            continue
        exists = db.execute(
            text(
                """
                SELECT 1
                FROM report_data_usage
                WHERE stock_code = :stock_code
                  AND trade_date = :trade_date
                  AND dataset_name = 'kline_daily'
                LIMIT 1
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_date_value},
        ).first()
        if exists:
            continue
        batch_id = str(kline_row[0] or f"runtime:{trade_date_value}")
        is_fallback_batch = batch_id.startswith("fallback_t_minus_1:")
        now = datetime.now(timezone.utc)
        # Ensure data_batch exists so lineage links remain valid (P1-35)
        if batch_id not in ensured_batches:
            if not is_fallback_batch:
                scoped_batch = db.execute(
                    text(
                        """
                        SELECT batch_id
                        FROM data_batch
                        WHERE source_name = 'tdx_local'
                          AND trade_date = :trade_date
                          AND batch_scope = 'core_pool'
                          AND batch_seq = 1
                        LIMIT 1
                        """
                    ),
                    {"trade_date": trade_date_value},
                ).first()
                if scoped_batch:
                    batch_id = str(scoped_batch[0])
            batch_exists = db.execute(
                text("SELECT 1 FROM data_batch WHERE batch_id = :bid LIMIT 1"),
                {"bid": batch_id},
            ).first()
            if not batch_exists:
                batch_scope = "repair_fallback" if is_fallback_batch else "core_pool"
                quality_flag = "stale_ok" if is_fallback_batch else "ok"
                status_reason = "fallback_t_minus_1" if is_fallback_batch else None
                db.execute(
                    text(
                        """
                        INSERT INTO data_batch (
                            batch_id, source_name, trade_date, batch_scope, batch_seq,
                            batch_status, quality_flag, covered_stock_count,
                            core_pool_covered_count, records_total, records_success,
                            records_failed, status_reason, trigger_task_run_id,
                            started_at, finished_at, updated_at, created_at
                        ) VALUES (
                            :batch_id, 'tdx_local', :trade_date, :batch_scope, 1,
                            'SUCCESS', :quality_flag, :cnt, :cnt, :cnt, :cnt,
                            0, :status_reason, NULL,
                            :now, :now, :now, :now
                        )
                        """
                    ),
                    {
                        "batch_id": batch_id,
                        "trade_date": trade_date_value,
                        "batch_scope": batch_scope,
                        "quality_flag": quality_flag,
                        "status_reason": status_reason,
                        "cnt": len(stock_codes),
                        "now": now,
                    },
                )
            ensured_batches.add(batch_id)
        db.execute(
            text(
                """
                INSERT INTO report_data_usage (
                    usage_id,
                    trade_date,
                    stock_code,
                    dataset_name,
                    source_name,
                    batch_id,
                    fetch_time,
                    status,
                    status_reason,
                    created_at
                ) VALUES (
                    :usage_id,
                    :trade_date,
                    :stock_code,
                    'kline_daily',
                    'tdx_local',
                    :batch_id,
                    :fetch_time,
                    :status,
                    :status_reason,
                    :created_at
                )
                """
            ),
            {
                "usage_id": str(uuid4()),
                "trade_date": trade_date_value,
                "stock_code": stock_code,
                "batch_id": batch_id,
                "fetch_time": now,
                "status": "stale_ok" if is_fallback_batch else "ok",
                "status_reason": "fallback_t_minus_1" if is_fallback_batch else None,
                "created_at": now,
            },
        )
        inserted += 1
    return inserted


def ensure_bootstrap_market_state_ready(db, *, trade_date_value: str) -> None:
    row = db.execute(
        text(
            """
            SELECT market_state_degraded, reference_date
            FROM market_state_cache
            WHERE trade_date = :trade_date
            LIMIT 1
            """
        ),
        {"trade_date": trade_date_value},
    ).first()
    if row is None or not bool(row[0]):
        return
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            """
            UPDATE market_state_cache
            SET cache_status = 'FRESH',
                state_reason = 'bootstrap_market_state_fallback',
                reference_date = COALESCE(reference_date, :trade_date),
                market_state_degraded = 0,
                computed_at = :now
            WHERE trade_date = :trade_date
            """
        ),
        {"trade_date": trade_date_value, "now": now},
    )
    db.commit()


def run_settlement_pipeline_step(db, *, trade_date_value: str, force: bool = True) -> dict[str, object]:
    from app.services.settlement_ssot import submit_settlement_batch, wait_for_settlement_pipeline

    accepted = submit_settlement_batch(
        db,
        trade_date=trade_date_value,
        force=force,
    )
    db.commit()
    pipeline_status = wait_for_settlement_pipeline(
        trade_date=trade_date_value,
        window_days_list=(1, 7, 14, 30, 60),
    )
    if pipeline_status["pipeline_status"] != "COMPLETED":
        raise RuntimeError(
            "settlement_pipeline_not_completed:"
            f"{pipeline_status['pipeline_status']}:"
            f"{pipeline_status.get('status_reason') or 'unknown'}"
        )
    return {
        "accepted": accepted,
        "pipeline_status": pipeline_status,
    }


def validate_counts(db) -> dict[str, int]:
    tables = (
        "stock_pool_snapshot",
        "report",
        "settlement_result",
        "report_data_usage",
        "market_state_cache",
        "sim_dashboard_snapshot",
        "sim_equity_curve_point",
    )
    counts = {}
    for table_name in tables:
        counts[table_name] = int(
            db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one()
        )
        if counts[table_name] <= 0:
            raise RuntimeError(f"table {table_name} is empty after rebuild")
    return counts


def ensure_runtime_users(db) -> None:
    from app.core.security import hash_password
    from app.models import User

    admin_password = os.getenv("YANBAO_RUNTIME_ADMIN_PASSWORD")
    pro_password = os.getenv("YANBAO_RUNTIME_PRO_PASSWORD")
    free_password = os.getenv("YANBAO_RUNTIME_FREE_PASSWORD")
    if not admin_password or not pro_password or not free_password:
        raise RuntimeError(
            "Runtime seed passwords must be provided via YANBAO_RUNTIME_ADMIN_PASSWORD / "
            "YANBAO_RUNTIME_PRO_PASSWORD / YANBAO_RUNTIME_FREE_PASSWORD"
        )

    user_specs = (
        {"email": "admin@example.com", "password": admin_password, "role": "admin", "tier": "Free"},
        {"email": "v79_pro@test.com", "password": pro_password, "role": "user", "tier": "Pro"},
        {"email": "v79_free@test.com", "password": free_password, "role": "user", "tier": "Free"},
    )
    for spec in user_specs:
        user = db.query(User).filter(User.email == spec["email"]).first()
        if user is None:
            user = User(
                email=spec["email"],
                password_hash=hash_password(spec["password"]),
                role=spec["role"],
                tier=spec["tier"],
                email_verified=True,
            )
            db.add(user)
            continue
        user.password_hash = hash_password(spec["password"])
        user.role = spec["role"]
        user.tier = spec["tier"]
        user.email_verified = True
        user.tier_expires_at = None
    db.flush()


def finalize_runtime_rebuild_state(db, *, current_trade_date: str) -> None:
    run_settlement_pipeline_step(
        db,
        trade_date_value=current_trade_date,
        force=True,
    )
    ensure_runtime_users(db)
    db.commit()


def swap_runtime_db(output_path: Path, runtime_path: Path) -> Path | None:
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if runtime_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = runtime_path.with_suffix(runtime_path.suffix + f".bak.{timestamp}")
        shutil.move(runtime_path, backup_path)
    shutil.move(output_path, runtime_path)
    return backup_path


def main() -> int:
    args = parse_args()
    output_path = (PROJECT_ROOT / args.output).resolve()
    runtime_path = (PROJECT_ROOT / args.runtime).resolve()
    requested_trade_date = args.trade_date

    build_empty_db(output_path)
    copied_seed_counts = seed_market_tables_from_runtime(runtime_path, output_path)
    configure_runtime_env(output_path, mock_llm=args.mock_llm)

    from app.core.config import settings as runtime_settings
    from app.core.db import SessionLocal, engine as runtime_engine
    from app.services.baseline_service import generate_ma_cross_baseline, generate_random_baseline, settle_baselines
    from app.services.market_state import compute_and_persist_market_state
    from app.services.report_generation_ssot import generate_report_ssot
    from app.services.runtime_materialization import ensure_sim_accounts
    from app.services.runtime_truth_guard import (
        normalize_snapshot_truth,
        report_truth_gate,
        soft_delete_stray_unpublished_reports,
    )
    from app.services.sim_positioning_ssot import process_trade_date
    from app.services.ssot_read_model import get_dashboard_stats_payload_ssot, get_home_payload_ssot, get_sim_dashboard_payload_ssot
    from app.services.stock_pool import get_daily_stock_pool, get_public_pool_view, refresh_stock_pool
    from app.services.trade_calendar import clear_trade_calendar_cache
    from scripts.bootstrap_real_data import _infer_trade_date, step_hotspot, step_kline, step_northbound, step_stock_master

    db = SessionLocal()
    try:
        runtime_settings.mock_llm = bool(args.mock_llm)
        runtime_settings.llm_audit_enabled = False
        requested_trade_date = requested_trade_date or _infer_trade_date().isoformat()
        target_trade_date = requested_trade_date

        if not copied_seed_counts.get("stock_master"):
            step_stock_master(db)
        if not copied_seed_counts.get("kline_daily"):
            step_kline(
                db,
                trade_date=date.fromisoformat(target_trade_date),
                kline_limit=args.kline_limit,
                batch_size=args.batch_size,
                pool_only_top=args.pool_only_top,
            )
        else:
            coverage = count_kline_coverage(db, trade_date_value=target_trade_date)
            if coverage < MIN_REBUILD_POOL_COVERAGE:
                backfill_trade_date_klines(
                    db,
                    trade_date_value=target_trade_date,
                    limit=max(40, args.kline_limit),
                    batch_size=args.batch_size,
                )
                coverage = count_kline_coverage(db, trade_date_value=target_trade_date)
            if coverage < 200:
                raise RuntimeError(
                    f"kline coverage insufficient for rebuild trade_date={target_trade_date}: {coverage}"
                )
        benchmark_code = f"{str(runtime_settings.hs300_code).strip()}.SH"
        ensure_benchmark_kline_history(
            db,
            benchmark_code=benchmark_code,
            trade_date_value=target_trade_date,
            limit=max(args.kline_limit, args.history_days + 40),
        )

        target_dates_desc = trade_dates_within_natural_window(
            db,
            target_trade_date=target_trade_date,
            natural_days=max(args.history_days, 1),
        )
        if not target_dates_desc:
            raise RuntimeError("no trade dates available after kline bootstrap")
        current_trade_date = target_dates_desc[0]
        history_trade_dates = list(reversed(target_dates_desc[1:]))

        for trade_date_value in list(reversed(target_dates_desc)):
            refresh_stock_pool(db, trade_date=trade_date_value, force_rebuild=True)
            compute_and_persist_market_state(db, trade_date=date.fromisoformat(trade_date_value))
            ensure_bootstrap_market_state_ready(db, trade_date_value=trade_date_value)
            pool_codes = get_daily_stock_pool(trade_date=trade_date_value, exact_trade_date=True)
            if trade_date_value == current_trade_date:
                step_hotspot(db, date.fromisoformat(trade_date_value))
                step_northbound(db, date.fromisoformat(trade_date_value))
                usage_codes = pool_codes
            else:
                usage_codes = pool_codes[: args.history_top_n]
            ensure_report_usage_rows(db, trade_date_value=trade_date_value, stock_codes=usage_codes)
            db.commit()

        current_codes = get_daily_stock_pool(trade_date=current_trade_date, exact_trade_date=True)
        report_failures: list[str] = []
        for stock_code in current_codes:
            try:
                generate_report_ssot(db, stock_code=stock_code, trade_date=current_trade_date)
            except Exception as exc:  # pragma: no cover - operational path
                report_failures.append(f"{current_trade_date}:{stock_code}:{exc}")

        for trade_date_value in history_trade_dates:
            for stock_code in get_daily_stock_pool(trade_date=trade_date_value, exact_trade_date=True)[: args.history_top_n]:
                try:
                    generate_report_ssot(db, stock_code=stock_code, trade_date=trade_date_value)
                except Exception as exc:  # pragma: no cover - operational path
                    report_failures.append(f"{trade_date_value}:{stock_code}:{exc}")

        if report_failures:
            raise RuntimeError("report_generation_failed\n" + "\n".join(report_failures[:20]))

        soft_delete_stray_unpublished_reports(db, runtime_trade_date=current_trade_date)
        gate = report_truth_gate(
            db,
            trade_date=current_trade_date,
            min_published_coverage_ratio=MIN_REBUILD_PUBLISHED_COVERAGE_RATIO,
            max_unpublished_ratio=MAX_REBUILD_UNPUBLISHED_RATIO,
        )
        if not gate["passed"]:
            raise RuntimeError(f"report_truth_gate_failed: {gate}")

        ensure_sim_accounts(db)
        for trade_date_value in sorted(set(history_trade_dates + [current_trade_date])):
            process_trade_date(db, trade_date_value)

        generate_random_baseline(db, current_trade_date)
        generate_ma_cross_baseline(db, current_trade_date)
        settle_baselines(db, current_trade_date)

        finalize_runtime_rebuild_state(
            db,
            current_trade_date=current_trade_date,
        )
        normalize_snapshot_truth(db)

        counts = validate_counts(db)
        pool_view = get_public_pool_view(db)
        home_payload = get_home_payload_ssot(db)
        dashboard_payload = get_dashboard_stats_payload_ssot(db, window_days=30)
        sim_payload = get_sim_dashboard_payload_ssot(db, capital_tier="100k")
        if pool_view is None:
            raise RuntimeError("no effective public pool after rebuild")
        if pool_view.task.trade_date.isoformat() != current_trade_date:
            raise RuntimeError(
                "effective public pool is not aligned to rebuild trade_date: "
                f"pool_trade_date={pool_view.task.trade_date.isoformat()} current_trade_date={current_trade_date}"
            )
        if len(pool_view.core_rows) != 200:
            raise RuntimeError(f"effective public pool size invalid: {len(pool_view.core_rows)}")
        if len(pool_view.standby_rows) != 50:
            raise RuntimeError(f"effective standby pool size invalid: {len(pool_view.standby_rows)}")
        if home_payload.get("pool_size") != len(pool_view.core_rows):
            raise RuntimeError("home payload pool_size does not match effective pool")
        home_report_dates = {
            str(item.get("trade_date"))
            for item in (home_payload.get("latest_reports") or [])
            if item.get("trade_date")
        }
        if home_report_dates and home_report_dates != {current_trade_date}:
            raise RuntimeError(f"home payload latest_reports date mismatch: {sorted(home_report_dates)}")
        exact_report_count = int(
            db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM report
                    WHERE published = 1
                      AND is_deleted = 0
                      AND trade_date = :trade_date
                    """
                ),
                {"trade_date": current_trade_date},
            ).scalar_one()
        )
        if int(home_payload.get("today_report_count") or 0) != exact_report_count:
            raise RuntimeError(
                "home payload today_report_count mismatch: "
                f"payload={home_payload.get('today_report_count')} actual={exact_report_count}"
            )
        latest_report_trade_date = db.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM report
                WHERE published = 1 AND is_deleted = 0
                """
            )
        ).scalar()
        latest_report_trade_date = latest_report_trade_date.isoformat() if hasattr(latest_report_trade_date, "isoformat") else str(latest_report_trade_date)[:10]
        if latest_report_trade_date and latest_report_trade_date > current_trade_date:
            if home_payload.get("data_status") != "DEGRADED" or home_payload.get("status_reason") != "home_source_inconsistent":
                raise RuntimeError("home payload did not expose home_source_inconsistent under report/pool mismatch")
        total_sample = sum(
            int((dashboard_payload.get("by_strategy_type") or {}).get(key, {}).get("sample_size") or 0)
            for key in ("A", "B", "C")
        )
        if dashboard_payload.get("total_settled", 0) > 0 and total_sample != dashboard_payload.get("total_settled"):
            raise RuntimeError(
                "dashboard snapshot inconsistent after rebuild: "
                f"sample_total={total_sample} total_settled={dashboard_payload.get('total_settled')}"
            )
        if not home_payload.get("latest_reports"):
            raise RuntimeError("home payload has no latest_reports after rebuild")
        if dashboard_payload.get("data_status") not in {"READY", "COMPUTING", "DEGRADED"}:
            raise RuntimeError("dashboard payload status invalid")
        if dashboard_payload.get("data_status") != "READY" and not dashboard_payload.get("status_reason"):
            raise RuntimeError("dashboard payload missing status_reason when not READY")
        for window_days in (1, 7, 14, 30, 60):
            payload = get_dashboard_stats_payload_ssot(db, window_days=window_days)
            expected_from = (date.fromisoformat(current_trade_date) - timedelta(days=window_days - 1)).isoformat()
            if payload.get("date_range") != {"from": expected_from, "to": current_trade_date}:
                raise RuntimeError(
                    "dashboard payload date_range mismatch: "
                    f"window_days={window_days} payload={payload.get('date_range')} "
                    f"expected={{'from': '{expected_from}', 'to': '{current_trade_date}'}}"
                )
            exact_snapshot_rows = db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM strategy_metric_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND window_days = :window_days
                    """
                ),
                {"snapshot_date": current_trade_date, "window_days": window_days},
            ).scalar_one()
            if payload.get("data_status") == "READY" and int(exact_snapshot_rows) <= 0:
                raise RuntimeError(
                    f"dashboard READY without exact snapshot: window_days={window_days} current_trade_date={current_trade_date}"
                )
            if window_days > args.history_days:
                if payload.get("status_reason") != "stats_history_truncated":
                    raise RuntimeError(
                        "dashboard payload did not expose history truncation: "
                        f"window_days={window_days} history_days={args.history_days} "
                        f"status_reason={payload.get('status_reason')}"
                    )
            elif payload.get("status_reason") == "stats_history_truncated":
                raise RuntimeError(
                    "dashboard payload truncated unexpectedly inside configured history window: "
                    f"window_days={window_days} history_days={args.history_days}"
                )
            if payload.get("data_status") != "READY" and not payload.get("status_reason"):
                raise RuntimeError(f"dashboard payload missing status_reason for window_days={window_days}")
        if sim_payload.get("data_status") not in {"READY", "COMPUTING", "DEGRADED"}:
            raise RuntimeError("sim payload status invalid")
    finally:
        db.close()
        runtime_engine.dispose()

    backup_path = swap_runtime_db(output_path, runtime_path)
    clear_trade_calendar_cache()

    print(f"runtime_db={runtime_path}")
    print(f"backup_db={backup_path or 'none'}")
    print(f"requested_trade_date={requested_trade_date}")
    print(f"target_trade_date={current_trade_date}")
    print(f"llm_mode={'mock' if args.mock_llm else 'live'}")
    print("llm_audit_enabled=false")
    print(f"seeded_market_tables={copied_seed_counts}")
    print(f"table_counts={counts}")
    print("users_ready=['admin@example.com','v79_pro@test.com','v79_free@test.com']")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
