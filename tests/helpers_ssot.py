from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from app.models import Base, Report
from app.services.trade_calendar import latest_trade_date_str


_REQUIRED_REPORT_USAGE_ROWS = (
    ("kline_daily", "tdx_local"),
    ("hotspot_top50", "eastmoney"),
    ("northbound_summary", "eastmoney"),
    # etf_flow_summary is supplementary (fetcher optional); not inserted by default fixture
    ("main_force_flow", "eastmoney_fflow_daykline"),
    ("dragon_tiger_list", "eastmoney_lhb"),
    ("margin_financing", "eastmoney_push2_rzrq"),
    ("stock_profile", "eastmoney_push2_stock_get"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_refresh_snapshot_binding(
    db,
    *,
    task_id: str,
    trade_day: date,
    stock_code: str,
    pool_version: int,
) -> None:
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    task_table = Base.metadata.tables["stock_pool_refresh_task"]
    existing = db.execute(
        snapshot_table.select().where(
            snapshot_table.c.refresh_task_id == task_id,
            snapshot_table.c.trade_date == trade_day,
            snapshot_table.c.stock_code == stock_code,
        )
    ).mappings().first()
    if existing:
        if existing.get("pool_version") != pool_version:
            db.execute(
                snapshot_table.update()
                .where(snapshot_table.c.pool_snapshot_id == existing["pool_snapshot_id"])
                .values(pool_version=pool_version)
            )
        return

    core_snapshot_count = db.execute(
        snapshot_table.select()
        .with_only_columns(snapshot_table.c.pool_snapshot_id)
        .where(
            snapshot_table.c.refresh_task_id == task_id,
            snapshot_table.c.trade_date == trade_day,
            snapshot_table.c.pool_role == "core",
        )
    ).mappings().all()
    rank_no = len(core_snapshot_count) + 1
    now = utc_now()
    db.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=task_id,
            trade_date=trade_day,
            pool_version=pool_version,
            stock_code=stock_code,
            pool_role="core",
            rank_no=rank_no,
            score=max(1.0, 100.0 - float(rank_no - 1)),
            is_suspended=False,
            created_at=now,
        )
    )
    db.execute(
        task_table.update()
        .where(task_table.c.task_id == task_id)
        .values(
            core_pool_size=rank_no,
            updated_at=now,
        )
    )


def _ensure_refresh_task(
    db,
    *,
    trade_day: date,
    stock_code: str | None = None,
    pool_version: int | None = None,
) -> str:
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    task_table = Base.metadata.tables["stock_pool_refresh_task"]
    now = utc_now()
    if stock_code:
        snapshot_filters = [
            snapshot_table.c.trade_date == trade_day,
            snapshot_table.c.stock_code == stock_code,
        ]
        if pool_version is not None:
            snapshot_filters.append(snapshot_table.c.pool_version == pool_version)
        row = db.execute(
            snapshot_table.select().where(*snapshot_filters)
        ).mappings().first()
        if row and row.get("refresh_task_id"):
            return str(row["refresh_task_id"])
    if pool_version is not None:
        row = db.execute(
            task_table.select().where(
                task_table.c.trade_date == trade_day,
                task_table.c.pool_version == pool_version,
            )
        ).mappings().first()
        if row and row.get("task_id"):
            return str(row["task_id"])
    fallback_row = db.execute(
        task_table.select().where(task_table.c.trade_date == trade_day)
    ).mappings().first()
    if fallback_row and fallback_row.get("task_id"):
        task_id = str(fallback_row["task_id"])
        effective_pool_version = int(
            pool_version if pool_version is not None else (fallback_row.get("pool_version") or 1)
        )
        if pool_version is not None and fallback_row.get("pool_version") != pool_version:
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    pool_version=pool_version,
                    updated_at=now,
                )
            )
            db.execute(
                snapshot_table.update()
                .where(snapshot_table.c.refresh_task_id == task_id)
                .values(pool_version=pool_version)
            )
            effective_pool_version = int(pool_version)
        if stock_code:
            _ensure_refresh_snapshot_binding(
                db,
                task_id=task_id,
                trade_day=trade_day,
                stock_code=stock_code,
                pool_version=effective_pool_version,
            )
        return task_id

    task_id = str(uuid4())
    effective_pool_version = pool_version if pool_version is not None else 1
    db.execute(
        task_table.insert().values(
            task_id=task_id,
            trade_date=trade_day,
            status="COMPLETED",
            pool_version=effective_pool_version,
            fallback_from=None,
            filter_params_json={"target_pool_size": 1 if stock_code else 0},
            core_pool_size=1 if stock_code else 0,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    if stock_code:
        db.execute(
            snapshot_table.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=trade_day,
                pool_version=effective_pool_version,
                stock_code=stock_code,
                pool_role="core",
                rank_no=1,
                score=100.0,
                is_suspended=False,
                created_at=now,
            )
        )
    # Add a standby row so the pool passes runtime validation
    db.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=task_id,
            trade_date=trade_day,
            pool_version=effective_pool_version,
            stock_code="990001.SH",
            pool_role="standby",
            rank_no=1,
            score=50.0,
            is_suspended=False,
            created_at=now,
        )
    )
    return task_id


def _ensure_market_state_cache(
    db,
    *,
    trade_day: date,
    market_state: str = "BULL",
    market_state_degraded: bool = False,
) -> None:
    cache_table = Base.metadata.tables["market_state_cache"]
    existing = db.execute(
        cache_table.select().where(cache_table.c.trade_date == trade_day)
    ).mappings().first()

    now = utc_now()
    kline_batch_id = _ensure_data_batch(
        db,
        batch_id=str(uuid4()),
        trade_day=trade_day,
        source_name="tdx_local",
        batch_scope="core_pool",
    )
    hotspot_batch_id = _ensure_data_batch(
        db,
        batch_id=str(uuid4()),
        trade_day=trade_day,
        source_name="eastmoney",
        batch_scope="full_market",
    )
    values = dict(
        market_state=market_state,
        cache_status="FRESH",
        state_reason="market ok" if not market_state_degraded else "market_state_degraded=true",
        reference_date=trade_day,
        market_state_degraded=market_state_degraded,
        a_type_pct=0.4,
        b_type_pct=0.3,
        c_type_pct=0.2,
        kline_batch_id=kline_batch_id,
        hotspot_batch_id=hotspot_batch_id,
        computed_at=now,
    )
    if existing:
        db.execute(
            cache_table.update()
            .where(cache_table.c.trade_date == trade_day)
            .values(**values)
        )
        return
    db.execute(
        cache_table.insert().values(
            trade_date=trade_day,
            created_at=now,
            **values,
        )
    )


def _ensure_data_batch(
    db,
    *,
    batch_id: str,
    trade_day: date,
    source_name: str = "tdx_local",
    batch_scope: str = "core_pool",
    batch_seq: int = 1,
) -> str:
    table = Base.metadata.tables["data_batch"]
    existing = db.execute(
        table.select().where(table.c.batch_id == batch_id)
    ).mappings().first()
    if existing:
        return str(existing["batch_id"])

    existing_by_scope = db.execute(
        table.select().where(
            table.c.source_name == source_name,
            table.c.trade_date == trade_day,
            table.c.batch_scope == batch_scope,
            table.c.batch_seq == batch_seq,
        )
    ).mappings().first()
    if existing_by_scope and existing_by_scope.get("batch_id"):
        return str(existing_by_scope["batch_id"])

    now = utc_now()
    db.execute(
        table.insert().values(
            batch_id=batch_id,
            source_name=source_name,
            trade_date=trade_day,
            batch_scope=batch_scope,
            batch_seq=batch_seq,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    return batch_id


def _runtime_trade_date_str() -> str:
    return latest_trade_date_str()


def _load_refresh_pool_version(db, *, refresh_task_id: str) -> int:
    task_table = Base.metadata.tables["stock_pool_refresh_task"]
    row = db.execute(
        task_table.select().where(task_table.c.task_id == refresh_task_id)
    ).mappings().first()
    if not row or row.get("pool_version") is None:
        raise AssertionError(f"refresh_task_id missing pool_version: {refresh_task_id}")
    return int(row["pool_version"])


def _load_parent_batch_ids(db, *, child_batch_id: str) -> set[str]:
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    rows = db.execute(
        lineage_table.select().where(
            lineage_table.c.child_batch_id == child_batch_id,
            lineage_table.c.lineage_role == "MERGED_FROM",
        )
    ).mappings().all()
    return {str(row["parent_batch_id"]) for row in rows if row.get("parent_batch_id")}


def _ensure_market_state_input_usage_fixture(
    db,
    *,
    stock_code: str,
    report_trade_day: date | None = None,
    market_state_row: dict,
) -> dict:
    usage_table = Base.metadata.tables["report_data_usage"]
    batch_table = Base.metadata.tables["data_batch"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    market_state_trade_date = market_state_row["trade_date"]
    usage_trade_date = report_trade_day or market_state_trade_date
    now = market_state_row.get("computed_at") or utc_now()
    desired_parent_batch_ids = {
        str(batch_id)
        for batch_id in (
            market_state_row.get("kline_batch_id"),
            market_state_row.get("hotspot_batch_id"),
        )
        if batch_id
    }
    derived_batch_rows = db.execute(
        batch_table.select().where(
            batch_table.c.source_name == "market_state_cache",
            batch_table.c.trade_date == market_state_trade_date,
            batch_table.c.batch_scope == "market_state_derived",
        )
    ).mappings().all()
    derived_batch = None
    for candidate in derived_batch_rows:
        if _load_parent_batch_ids(db, child_batch_id=str(candidate["batch_id"])) == desired_parent_batch_ids:
            derived_batch = candidate
            break
    quality_flag = "degraded" if market_state_row.get("market_state_degraded") else "ok"
    status_reason = market_state_row.get("state_reason") if market_state_row.get("market_state_degraded") else None
    if derived_batch:
        batch_id = str(derived_batch["batch_id"])
        db.execute(
            batch_table.update()
            .where(batch_table.c.batch_id == batch_id)
            .values(
                batch_status="SUCCESS",
                quality_flag=quality_flag,
                covered_stock_count=1,
                core_pool_covered_count=1,
                records_total=1,
                records_success=1,
                records_failed=0,
                status_reason=status_reason,
                finished_at=now,
                updated_at=now,
            )
        )
    else:
        batch_id = str(uuid4())
        next_seq_row = db.execute(
            batch_table.select()
            .with_only_columns(batch_table.c.batch_seq)
            .where(
                batch_table.c.source_name == "market_state_cache",
                batch_table.c.trade_date == market_state_trade_date,
                batch_table.c.batch_scope == "market_state_derived",
            )
            .order_by(batch_table.c.batch_seq.desc())
        ).mappings().first()
        next_seq = int(next_seq_row["batch_seq"]) + 1 if next_seq_row and next_seq_row.get("batch_seq") is not None else 1
        db.execute(
            batch_table.insert().values(
                batch_id=batch_id,
                source_name="market_state_cache",
                trade_date=market_state_trade_date,
                batch_scope="market_state_derived",
                batch_seq=next_seq,
                batch_status="SUCCESS",
                quality_flag=quality_flag,
                covered_stock_count=1,
                core_pool_covered_count=1,
                records_total=1,
                records_success=1,
                records_failed=0,
                status_reason=status_reason,
                trigger_task_run_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
    for parent_batch_id in sorted(desired_parent_batch_ids):
        exists = db.execute(
            lineage_table.select().where(
                lineage_table.c.child_batch_id == batch_id,
                lineage_table.c.parent_batch_id == parent_batch_id,
                lineage_table.c.lineage_role == "MERGED_FROM",
            )
        ).mappings().first()
        if exists:
            continue
        db.execute(
            lineage_table.insert().values(
                batch_lineage_id=str(uuid4()),
                child_batch_id=batch_id,
                parent_batch_id=parent_batch_id,
                lineage_role="MERGED_FROM",
                created_at=now,
            )
        )
    usage_row = db.execute(
        usage_table.select().where(
            usage_table.c.trade_date == usage_trade_date,
            usage_table.c.stock_code == stock_code,
            usage_table.c.dataset_name == "market_state_input",
            usage_table.c.source_name == "market_state_cache",
            usage_table.c.batch_id == batch_id,
        )
    ).mappings().first()
    if usage_row:
        db.execute(
            usage_table.update()
            .where(usage_table.c.usage_id == usage_row["usage_id"])
            .values(
                fetch_time=now,
                status="degraded" if market_state_row.get("market_state_degraded") else "ok",
                status_reason=status_reason,
            )
        )
        usage_row["fetch_time"] = now
        usage_row["status"] = "degraded" if market_state_row.get("market_state_degraded") else "ok"
        usage_row["status_reason"] = status_reason
        return dict(usage_row)
    usage_id = str(uuid4())
    values = dict(
        usage_id=usage_id,
        trade_date=usage_trade_date,
        stock_code=stock_code,
        dataset_name="market_state_input",
        source_name="market_state_cache",
        batch_id=batch_id,
        fetch_time=now,
        status="degraded" if market_state_row.get("market_state_degraded") else "ok",
        status_reason=status_reason,
        created_at=now,
    )
    db.execute(usage_table.insert().values(**values))
    return values


def _ensure_required_generation_usage_fixtures(
    db,
    *,
    stock_code: str,
    trade_day: date,
    batch_id: str,
    now: datetime,
) -> list[dict]:
    usage_table = Base.metadata.tables["report_data_usage"]
    rows: list[dict] = []

    for offset, (dataset_name, source_name) in enumerate(_REQUIRED_REPORT_USAGE_ROWS):
        fetch_time = now + timedelta(microseconds=offset)
        existing = db.execute(
            usage_table.select().where(
                usage_table.c.trade_date == trade_day,
                usage_table.c.stock_code == stock_code,
                usage_table.c.dataset_name == dataset_name,
                usage_table.c.source_name == source_name,
            )
        ).mappings().first()
        if existing:
            status_reason = None
            if dataset_name == "main_force_flow":
                status_reason = "capital_snapshot:{\"net_inflow_5d\":123456.0,\"history_records\":30,\"history_end_date\":\"2026-03-06\"}"
            elif dataset_name == "dragon_tiger_list":
                status_reason = "capital_snapshot:{\"lhb_count_30d\":3,\"source\":\"eastmoney_lhb\"}"
            elif dataset_name == "margin_financing":
                status_reason = "capital_snapshot:{\"latest_rzye\":5000000000.0,\"rzye_delta_5d\":88888.0,\"history_records\":20}"
            elif dataset_name == "stock_profile":
                status_reason = "profile_snapshot:{\"pe_ttm\":21.4,\"pb\":7.2,\"roe_pct\":28.1,\"total_mv\":1760000000000.0,\"industry\":\"白酒\",\"region\":\"贵州\",\"list_date\":\"2001-08-27\"}"
            db.execute(
                usage_table.update()
                .where(usage_table.c.usage_id == existing["usage_id"])
                .values(
                    batch_id=batch_id,
                    fetch_time=fetch_time,
                    status="ok",
                    status_reason=status_reason,
                )
            )
            existing.update(
                batch_id=batch_id,
                fetch_time=fetch_time,
                status="ok",
                status_reason=status_reason,
            )
            rows.append(dict(existing))
            continue

        values = {
            "usage_id": str(uuid4()),
            "trade_date": trade_day,
            "stock_code": stock_code,
            "dataset_name": dataset_name,
            "source_name": source_name,
            "batch_id": batch_id,
            "fetch_time": fetch_time,
            "status": "ok",
            "status_reason": None,
            "created_at": now,
        }
        if dataset_name == "main_force_flow":
            values["status_reason"] = "capital_snapshot:{\"net_inflow_5d\":123456.0,\"history_records\":30,\"history_end_date\":\"2026-03-06\"}"
        elif dataset_name == "dragon_tiger_list":
            values["status_reason"] = "capital_snapshot:{\"lhb_count_30d\":3,\"source\":\"eastmoney_lhb\"}"
        elif dataset_name == "margin_financing":
            values["status_reason"] = "capital_snapshot:{\"latest_rzye\":5000000000.0,\"rzye_delta_5d\":88888.0,\"history_records\":20}"
        elif dataset_name == "stock_profile":
            values["status_reason"] = "profile_snapshot:{\"pe_ttm\":21.4,\"pb\":7.2,\"roe_pct\":28.1,\"total_mv\":1760000000000.0,\"industry\":\"白酒\",\"region\":\"贵州\",\"list_date\":\"2001-08-27\"}"
        db.execute(usage_table.insert().values(**values))
        rows.append(values)

    return rows


def _runtime_trade_date() -> date:
    return date.fromisoformat(_runtime_trade_date_str())


def _default_list_date() -> str:
    return (_runtime_trade_date() - timedelta(days=365 * 16)).isoformat()


def insert_stock_master(
    db,
    *,
    stock_code: str,
    stock_name: str,
    industry: str = "WhiteLiquor",
    exchange: str = "SH",
    is_suspended: bool = False,
    is_delisted: bool = False,
    is_st: bool = False,
    circulating_shares: float = 1_000_000_000,
    list_date: str | None = None,
) -> None:
    table = Base.metadata.tables["stock_master"]
    exists = db.execute(table.select().where(table.c.stock_code == stock_code)).first()
    if exists:
        return
    actual_list_date = list_date or _default_list_date()
    db.execute(
        table.insert().values(
            stock_code=stock_code,
            stock_name=stock_name,
            exchange=exchange,
            industry=industry,
            list_date=date.fromisoformat(actual_list_date),
            circulating_shares=circulating_shares,
            is_st=is_st,
            is_suspended=is_suspended,
            is_delisted=is_delisted,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
    )


def insert_kline(
    db,
    *,
    stock_code: str,
    trade_date: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: float = 100_000,
    amount: float | None = None,
    atr_pct: float = 0.03,
    turnover_rate: float = 0.02,
    ma5: float | None = None,
    ma10: float | None = None,
    ma20: float | None = None,
    ma60: float | None = None,
    volatility_20d: float = 0.02,
    hs300_return_20d: float = 0.01,
    is_suspended: bool = False,
    batch_id: str | None = None,
) -> None:
    table = Base.metadata.tables["kline_daily"]
    amount = amount if amount is not None else close_price * volume
    batch_id = batch_id or str(uuid4())
    trade_day = date.fromisoformat(trade_date)
    values = dict(
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume,
        amount=amount,
        adjust_type="front_adjusted",
        atr_pct=atr_pct,
        turnover_rate=turnover_rate,
        ma5=ma5 if ma5 is not None else close_price * 0.99,
        ma10=ma10 if ma10 is not None else close_price * 0.98,
        ma20=ma20 if ma20 is not None else close_price * 0.96,
        ma60=ma60 if ma60 is not None else close_price * 0.9,
        volatility_20d=volatility_20d,
        hs300_return_20d=hs300_return_20d,
        is_suspended=is_suspended,
        source_batch_id=batch_id,
        created_at=utc_now(),
    )
    existing = db.execute(
        table.select().where(
            table.c.stock_code == stock_code,
            table.c.trade_date == trade_day,
        )
    ).mappings().first()
    if existing:
        db.execute(
            table.update()
            .where(table.c.kline_id == existing["kline_id"])
            .values(**values)
        )
        return
    db.execute(
        table.insert().values(
            kline_id=str(uuid4()),
            stock_code=stock_code,
            trade_date=trade_day,
            **values,
        )
    )


def seed_generation_context(
    db,
    *,
    stock_code: str = "600519.SH",
    stock_name: str = "MOUTAI",
    trade_date: str | None = None,
    close_price: float = 120.0,
    atr_pct: float = 0.03,
    ma20: float = 116.0,
    volatility_20d: float = 0.02,
    market_state: str = "BULL",
    market_state_degraded: bool = False,
    is_suspended: bool = False,
    event_type: str | None = None,
    pool_version: int = 1,
) -> None:
    now = utc_now()
    batch_id = str(uuid4())
    refresh_task_id = str(uuid4())
    trade_date = trade_date or _runtime_trade_date_str()
    trade_day = date.fromisoformat(trade_date)

    insert_stock_master(
        db,
        stock_code=stock_code,
        stock_name=stock_name,
        is_suspended=is_suspended,
    )
    insert_kline(
        db,
        stock_code=stock_code,
        trade_date=trade_date,
        open_price=close_price * 0.99,
        high_price=close_price * 1.03,
        low_price=close_price * 0.97,
        close_price=close_price,
        atr_pct=atr_pct,
        ma20=ma20,
        volatility_20d=volatility_20d,
        is_suspended=is_suspended,
        batch_id=batch_id,
    )

    db.execute(
        Base.metadata.tables["data_batch"].insert().values(
            batch_id=batch_id,
            source_name="tdx_local",
            trade_date=trade_day,
            batch_scope="core_pool",
            batch_seq=1,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    _ensure_required_generation_usage_fixtures(
        db,
        stock_code=stock_code,
        trade_day=trade_day,
        batch_id=batch_id,
        now=now,
    )
    db.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=refresh_task_id,
            trade_date=trade_day,
            status="COMPLETED",
            pool_version=pool_version,
            fallback_from=None,
            filter_params_json={"target_pool_size": 1},
            core_pool_size=1,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=refresh_task_id,
            trade_date=trade_day,
            pool_version=pool_version,
            stock_code=stock_code,
            pool_role="core",
            rank_no=1,
            score=88.5,
            is_suspended=is_suspended,
            created_at=now,
        )
    )
    db.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=refresh_task_id,
            trade_date=trade_day,
            pool_version=pool_version,
            stock_code="990001.SH",
            pool_role="standby",
            rank_no=1,
            score=50.0,
            is_suspended=False,
            created_at=now,
        )
    )
    db.execute(
        Base.metadata.tables["market_state_cache"].insert().values(
            trade_date=trade_day,
            market_state=market_state,
            cache_status="FRESH",
            state_reason="market ok" if not market_state_degraded else "market_state_degraded=true",
            reference_date=trade_day,
            market_state_degraded=market_state_degraded,
            a_type_pct=0.4,
            b_type_pct=0.4,
            c_type_pct=0.2,
            kline_batch_id=batch_id,
            hotspot_batch_id=batch_id,
            computed_at=now,
            created_at=now,
        )
    )

    if event_type:
        hotspot_id = str(uuid4())
        db.execute(
            Base.metadata.tables["market_hotspot_item"].insert().values(
                hotspot_item_id=hotspot_id,
                batch_id=batch_id,
                source_name="weibo",
                merged_rank=1,
                source_rank=1,
                topic_title=f"{stock_name} event",
                news_event_type=event_type,
                hotspot_tags_json=[event_type],
                source_url="https://example.com/hotspot",
                fetch_time=now,
                quality_flag="ok",
                created_at=now,
            )
        )
        db.execute(
            Base.metadata.tables["market_hotspot_item_stock_link"].insert().values(
                hotspot_item_stock_link_id=str(uuid4()),
                hotspot_item_id=hotspot_id,
                stock_code=stock_code,
                relation_role="subject",
                match_confidence=0.99,
                created_at=now,
            )
        )

    db.commit()


def insert_report_bundle_ssot(
    db,
    *,
    stock_code: str = "600519.SH",
    stock_name: str = "MOUTAI",
    trade_date: str | None = None,
    report_id: str | None = None,
    recommendation: str = "BUY",
    confidence: float = 0.78,
    strategy_type: str = "A",
    market_state: str = "BULL",
    quality_flag: str = "ok",
    published: bool = True,
    review_flag: str = "APPROVED",
    signal_entry_price: float = 123.45,
    atr_pct: float = 0.032,
    atr_multiplier: float = 1.5,
    stop_loss: float = 117.28,
    target_price: float = 138.88,
    stop_loss_calc_mode: str = "atr_multiplier",
    trade_instructions: dict[str, dict[str, object]] | None = None,
    ensure_pool_snapshot: bool = True,
    pool_version: int | None = None,
) -> Report:
    trade_date = trade_date or _runtime_trade_date_str()
    report_id = report_id or str(uuid4())
    task_id = str(uuid4())
    batch_id = str(uuid4())
    now = utc_now()
    trade_day = date.fromisoformat(trade_date)
    idempotency_key = f"daily:{stock_code}:{trade_date}"
    refresh_task_id = _ensure_refresh_task(
        db,
        trade_day=trade_day,
        stock_code=stock_code if ensure_pool_snapshot else None,
        pool_version=pool_version,
    )

    insert_stock_master(db, stock_code=stock_code, stock_name=stock_name)
    _ensure_market_state_cache(
        db,
        trade_day=trade_day,
        market_state=market_state,
    )
    market_state_row = db.execute(
        Base.metadata.tables["market_state_cache"]
        .select()
        .where(Base.metadata.tables["market_state_cache"].c.trade_date <= trade_day)
        .order_by(Base.metadata.tables["market_state_cache"].c.trade_date.desc())
    ).mappings().first()
    market_state_trade_date = (market_state_row or {}).get("trade_date") or trade_day
    market_state_reference_date = (market_state_row or {}).get("reference_date") or market_state_trade_date
    pool_version = _load_refresh_pool_version(db, refresh_task_id=refresh_task_id)
    kline_table = Base.metadata.tables["kline_daily"]
    existing_kline = db.execute(
        kline_table.select().where(
            kline_table.c.stock_code == stock_code,
            kline_table.c.trade_date == trade_day,
        )
    ).first()
    if existing_kline is None:
        insert_kline(
            db,
            stock_code=stock_code,
            trade_date=trade_date,
            open_price=signal_entry_price,
            high_price=max(signal_entry_price, target_price),
            low_price=min(signal_entry_price, stop_loss),
            close_price=signal_entry_price,
            atr_pct=atr_pct,
        )
    batch_id = _ensure_data_batch(db, batch_id=batch_id, trade_day=trade_day)
    db.execute(
        Base.metadata.tables["report_generation_task"].insert().values(
            task_id=task_id,
            trade_date=trade_day,
            stock_code=stock_code,
            idempotency_key=idempotency_key,
            generation_seq=1,
            status="Completed",
            retry_count=0,
            quality_flag=quality_flag,
            status_reason=None,
            llm_fallback_level="primary",
            risk_audit_status="completed",
            risk_audit_skip_reason=None,
            market_state_trade_date=market_state_trade_date,
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )

    report = Report(
        report_id=report_id,
        generation_task_id=task_id,
        trade_date=trade_day,
        stock_code=stock_code,
        stock_name_snapshot=stock_name,
        pool_version=pool_version,
        idempotency_key=idempotency_key,
        generation_seq=1,
        published=published,
        publish_status="PUBLISHED" if published else "DRAFT_GENERATED",
        published_at=now if published else None,
        recommendation=recommendation,
        confidence=confidence,
        quality_flag=quality_flag,
        status_reason=None,
        llm_fallback_level="primary",
        strategy_type=strategy_type,
        market_state=market_state,
        market_state_reference_date=market_state_reference_date,
        market_state_degraded=bool((market_state_row or {}).get("market_state_degraded")),
        market_state_reason_snapshot=(market_state_row or {}).get("state_reason") or "market ok",
        market_state_trade_date=market_state_trade_date,
        conclusion_text="Bullish setup",
        reasoning_chain_md="step-1\nstep-2",
        prior_stats_snapshot={"recent_3m_accuracy": 0.61},
        risk_audit_status="completed",
        risk_audit_skip_reason=None,
        review_flag=review_flag,
        failure_category=None,
        negative_feedback_count=0,
        reviewed_by=None,
        reviewed_at=None,
        is_deleted=False,
        deleted_at=None,
        superseded_by_report_id=None,
        created_at=now,
        updated_at=now,
    )
    db.add(report)
    db.flush()

    db.execute(
        Base.metadata.tables["report_citation"].insert().values(
            citation_id=str(uuid4()),
            report_id=report_id,
            citation_order=1,
            source_name="market_data",
            source_url="https://example.com/market",
            fetch_time=now,
            title="Market snapshot",
            excerpt="Price and volume stable",
            created_at=now,
        )
    )
    db.execute(
        Base.metadata.tables["instruction_card"].insert().values(
            instruction_card_id=str(uuid4()),
            report_id=report_id,
            signal_entry_price=signal_entry_price,
            atr_pct=atr_pct,
            atr_multiplier=atr_multiplier,
            stop_loss=stop_loss,
            target_price=target_price,
            stop_loss_calc_mode=stop_loss_calc_mode,
            created_at=now,
        )
    )

    required_usage_rows = _ensure_required_generation_usage_fixtures(
        db,
        stock_code=stock_code,
        trade_day=trade_day,
        batch_id=batch_id,
        now=now,
    )
    market_state_usage = _ensure_market_state_input_usage_fixture(
        db,
        stock_code=stock_code,
        report_trade_day=trade_day,
        market_state_row=dict(market_state_row or {}),
    )
    link_table = Base.metadata.tables["report_data_usage_link"]
    usage_ids = [str(row["usage_id"]) for row in required_usage_rows]
    usage_ids.append(str(market_state_usage["usage_id"]))
    for link_index, usage_id in enumerate(usage_ids):
        db.execute(
            link_table.insert().values(
                report_data_usage_link_id=str(uuid4()),
                report_id=report_id,
                usage_id=usage_id,
                created_at=now + timedelta(microseconds=link_index),
            )
        )

    trade_instructions = trade_instructions or {
        "10k": {"status": "EXECUTE", "position_ratio": 0.1, "skip_reason": None},
        "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
        "500k": {"status": "EXECUTE", "position_ratio": 0.3, "skip_reason": None},
    }
    for capital_tier in ("10k", "100k", "500k"):
        current = trade_instructions.get(capital_tier) or {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": None}
        db.execute(
            Base.metadata.tables["sim_trade_instruction"].insert().values(
                trade_instruction_id=str(uuid4()),
                report_id=report_id,
                capital_tier=capital_tier,
                status=current["status"],
                position_ratio=current["position_ratio"],
                skip_reason=current.get("skip_reason"),
                created_at=now,
            )
        )

    db.commit()
    db.refresh(report)
    return report


def insert_sim_account(
    db,
    *,
    capital_tier: str,
    initial_cash: float,
    cash_available: float,
    total_asset: float,
    peak_total_asset: float,
    max_drawdown_pct: float,
    drawdown_state: str,
    drawdown_state_factor: float,
    active_position_count: int = 0,
    last_reconciled_trade_date: str | None = None,
) -> None:
    table = Base.metadata.tables["sim_account"]
    now = utc_now()
    existing = db.execute(
        table.select().where(table.c.capital_tier == capital_tier)
    ).mappings().first()
    values = dict(
        capital_tier=capital_tier,
        initial_cash=initial_cash,
        cash_available=cash_available,
        total_asset=total_asset,
        peak_total_asset=peak_total_asset,
        max_drawdown_pct=max_drawdown_pct,
        drawdown_state=drawdown_state,
        drawdown_state_factor=drawdown_state_factor,
        active_position_count=active_position_count,
        last_reconciled_trade_date=date.fromisoformat(last_reconciled_trade_date) if last_reconciled_trade_date else None,
        updated_at=now,
    )
    if existing:
        db.execute(
            table.update()
            .where(table.c.capital_tier == capital_tier)
            .values(**values)
        )
    else:
        db.execute(
            table.insert().values(
                **values,
                created_at=now,
            )
        )
    db.commit()


def insert_open_position(
    db,
    *,
    report_id: str,
    stock_code: str,
    capital_tier: str,
    signal_date: str,
    entry_date: str,
    actual_entry_price: float,
    signal_entry_price: float,
    position_ratio: float,
    shares: int,
    atr_pct_snapshot: float = 0.03,
    atr_multiplier_snapshot: float = 1.5,
    stop_loss_price: float = 90.0,
    target_price: float = 110.0,
) -> str:
    position_id = str(uuid4())
    now = utc_now()
    account_exists = db.execute(
        Base.metadata.tables["sim_account"]
        .select()
        .where(Base.metadata.tables["sim_account"].c.capital_tier == capital_tier)
    ).first()
    if not account_exists:
        initial_cash = 100_000.0
        insert_sim_account(
            db,
            capital_tier=capital_tier,
            initial_cash=initial_cash,
            cash_available=initial_cash,
            total_asset=initial_cash,
            peak_total_asset=initial_cash,
            max_drawdown_pct=0.0,
            drawdown_state="NORMAL",
            drawdown_state_factor=1.0,
            active_position_count=0,
            last_reconciled_trade_date=entry_date,
        )
    db.execute(
        Base.metadata.tables["sim_position"].insert().values(
            position_id=position_id,
            report_id=report_id,
            stock_code=stock_code,
            capital_tier=capital_tier,
            position_status="OPEN",
            signal_date=date.fromisoformat(signal_date),
            entry_date=date.fromisoformat(entry_date),
            actual_entry_price=actual_entry_price,
            signal_entry_price=signal_entry_price,
            position_ratio=position_ratio,
            shares=shares,
            atr_pct_snapshot=atr_pct_snapshot,
            atr_multiplier_snapshot=atr_multiplier_snapshot,
            stop_loss_price=stop_loss_price,
            target_price=target_price,
            exit_date=None,
            exit_price=None,
            holding_days=(date.today() - date.fromisoformat(entry_date)).days,
            net_return_pct=None,
            commission_total=0,
            stamp_duty=0,
            slippage_total=0,
            take_profit_pending_t1=False,
            stop_loss_pending_t1=False,
            suspended_pending=False,
            limit_locked_pending=False,
            skip_reason=None,
            status_reason=None,
            created_at=now,
            updated_at=now,
        )
    )
    db.commit()
    return position_id


def age_report_generation_task(
    db,
    *,
    stock_code: str,
    trade_date: str,
    status: str = "Pending",
    updated_hours_ago: int = 80,
) -> str:
    task_id = str(uuid4())
    when = utc_now() - timedelta(hours=updated_hours_ago)
    trade_day = date.fromisoformat(trade_date)
    insert_stock_master(db, stock_code=stock_code, stock_name=stock_code)
    _ensure_market_state_cache(db, trade_day=trade_day)
    refresh_task_id = _ensure_refresh_task(db, trade_day=trade_day, stock_code=stock_code)
    db.execute(
        Base.metadata.tables["report_generation_task"].insert().values(
            task_id=task_id,
            trade_date=trade_day,
            stock_code=stock_code,
            idempotency_key=f"daily:{stock_code}:{trade_date}",
            generation_seq=1,
            status=status,
            retry_count=0,
            quality_flag="ok",
            status_reason=None,
            llm_fallback_level="primary",
            risk_audit_status="completed",
            risk_audit_skip_reason=None,
            market_state_trade_date=trade_day,
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=when,
            started_at=when,
            finished_at=None,
            updated_at=when,
            created_at=when,
        )
    )
    db.commit()
    return task_id


def insert_pool_snapshot(
    db,
    *,
    trade_date: str,
    stock_codes: list[str],
    status: str = "COMPLETED",
    pool_role: str = "core",
    pool_version: int = 1,
    standby_codes: list[str] | None = None,
) -> str:
    task_id = str(uuid4())
    now = utc_now()
    trade_day = date.fromisoformat(trade_date)
    # Auto-generate standby codes for core pools to pass runtime validation
    effective_standby: list[str] = standby_codes if standby_codes is not None else []
    if pool_role == "core" and not effective_standby:
        effective_standby = [f"99{i:04d}.SH" for i in range(1, 4)]
    db.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=task_id,
            trade_date=trade_day,
            status=status,
            pool_version=pool_version,
            fallback_from=None,
            filter_params_json={"target_pool_size": len(stock_codes) if pool_role == "core" else 0},
            core_pool_size=len(stock_codes) if pool_role == "core" else 0,
            standby_pool_size=len(effective_standby) if pool_role == "core" else (len(stock_codes) if pool_role == "standby" else 0),
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(stock_codes, start=1):
        db.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=trade_day,
                pool_version=pool_version,
                stock_code=stock_code,
                pool_role=pool_role,
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    for rank_no, stock_code in enumerate(effective_standby, start=1):
        db.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=trade_day,
                pool_version=pool_version,
                stock_code=stock_code,
                pool_role="standby",
                rank_no=rank_no,
                score=50 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db.commit()
    return task_id


def insert_market_state_cache(
    db,
    *,
    trade_date: str,
    market_state: str = "BULL",
    cache_status: str = "FRESH",
    state_reason: str = "market ok",
    reference_date: str | None = None,
    market_state_degraded: bool = False,
) -> None:
    now = utc_now()
    trade_day = date.fromisoformat(trade_date)
    table = Base.metadata.tables["market_state_cache"]
    kline_batch_id = _ensure_data_batch(
        db,
        batch_id=str(uuid4()),
        trade_day=trade_day,
        source_name="tdx_local",
        batch_scope="core_pool",
    )
    hotspot_batch_id = _ensure_data_batch(
        db,
        batch_id=str(uuid4()),
        trade_day=trade_day,
        source_name="eastmoney",
        batch_scope="full_market",
    )
    values = dict(
        trade_date=trade_day,
        market_state=market_state,
        cache_status=cache_status,
        state_reason=state_reason,
        reference_date=date.fromisoformat(reference_date or trade_date),
        market_state_degraded=market_state_degraded,
        a_type_pct=0.4,
        b_type_pct=0.3,
        c_type_pct=0.2,
        kline_batch_id=kline_batch_id,
        hotspot_batch_id=hotspot_batch_id,
        computed_at=now,
    )
    existing = db.execute(
        table.select().where(table.c.trade_date == trade_day)
    ).mappings().first()
    if existing:
        db.execute(
            table.update()
            .where(table.c.trade_date == trade_day)
            .values(**values)
        )
    else:
        db.execute(
            table.insert().values(
                **values,
                created_at=now,
            )
        )
    db.commit()


def insert_strategy_metric_snapshot(
    db,
    *,
    snapshot_date: str,
    strategy_type: str,
    window_days: int = 30,
    data_status: str = "READY",
    sample_size: int = 30,
    coverage_pct: float = 1.0,
    win_rate: float | None = 0.6,
    profit_loss_ratio: float | None = 1.8,
    alpha_annual: float | None = 0.12,
    max_drawdown_pct: float | None = -0.08,
    cumulative_return_pct: float | None = 0.2,
    signal_validity_warning: bool = False,
    display_hint: str | None = None,
) -> None:
    db.execute(
        Base.metadata.tables["strategy_metric_snapshot"].insert().values(
            metric_snapshot_id=str(uuid4()),
            snapshot_date=date.fromisoformat(snapshot_date),
            strategy_type=strategy_type,
            window_days=window_days,
            data_status=data_status,
            sample_size=sample_size,
            coverage_pct=coverage_pct,
            win_rate=win_rate,
            profit_loss_ratio=profit_loss_ratio,
            alpha_annual=alpha_annual,
            max_drawdown_pct=max_drawdown_pct,
            cumulative_return_pct=cumulative_return_pct,
            signal_validity_warning=signal_validity_warning,
            display_hint=display_hint,
            created_at=utc_now(),
        )
    )
    db.commit()


def insert_baseline_metric_snapshot(
    db,
    *,
    snapshot_date: str,
    baseline_type: str,
    window_days: int = 30,
    simulation_runs: int = 500,
    sample_size: int = 30,
    win_rate: float | None = 0.55,
    profit_loss_ratio: float | None = 1.5,
    alpha_annual: float | None = 0.08,
    max_drawdown_pct: float | None = -0.09,
    cumulative_return_pct: float | None = 0.16,
    display_hint: str | None = None,
) -> None:
    db.execute(
        Base.metadata.tables["baseline_metric_snapshot"].insert().values(
            baseline_metric_snapshot_id=str(uuid4()),
            snapshot_date=date.fromisoformat(snapshot_date),
            window_days=window_days,
            baseline_type=baseline_type,
            simulation_runs=simulation_runs,
            sample_size=sample_size,
            win_rate=win_rate,
            profit_loss_ratio=profit_loss_ratio,
            alpha_annual=alpha_annual,
            max_drawdown_pct=max_drawdown_pct,
            cumulative_return_pct=cumulative_return_pct,
            display_hint=display_hint,
            created_at=utc_now(),
        )
    )
    db.commit()


def insert_settlement_result(
    db,
    *,
    report_id: str,
    stock_code: str = "600519.SH",
    signal_date: str,
    window_days: int = 30,
    strategy_type: str = "B",
    settlement_status: str = "settled",
    quality_flag: str = "ok",
    entry_trade_date: str | None = None,
    exit_trade_date: str | None = None,
    buy_price: float = 10.0,
    sell_price: float = 10.8,
    net_return_pct: float = 0.05,
    gross_return_pct: float | None = None,
    shares: int = 100,
    display_hint: str | None = None,
) -> None:
    signal_day = date.fromisoformat(signal_date)
    entry_day = date.fromisoformat(entry_trade_date or signal_date)
    exit_day = date.fromisoformat(exit_trade_date or signal_date)
    now = utc_now()
    db.execute(
        Base.metadata.tables["settlement_result"].insert().values(
            settlement_id=str(uuid4()),
            settlement_result_id=str(uuid4()),
            report_id=report_id,
            stock_code=stock_code,
            trade_date=signal_day,
            signal_date=signal_day,
            window_days=window_days,
            strategy_type=strategy_type,
            settlement_status=settlement_status,
            is_misclassified=False,
            quality_flag=quality_flag,
            status_reason=None,
            entry_trade_date=entry_day,
            exit_trade_date=exit_day,
            shares=shares,
            buy_price=buy_price,
            sell_price=sell_price,
            buy_commission=5.0,
            sell_commission=5.0,
            stamp_duty=0.5,
            buy_slippage_cost=0.5,
            sell_slippage_cost=0.5,
            gross_return_pct=gross_return_pct if gross_return_pct is not None else net_return_pct,
            net_return_pct=net_return_pct,
            display_hint=display_hint,
            created_at=now,
            updated_at=now,
        )
    )
    db.commit()


def insert_sim_dashboard_snapshot(
    db,
    *,
    capital_tier: str,
    snapshot_date: str,
    data_status: str = "READY",
    status_reason: str | None = None,
    total_return_pct: float = 0.2,
    win_rate: float | None = 0.6,
    profit_loss_ratio: float | None = 1.8,
    alpha_annual: float | None = 0.12,
    max_drawdown_pct: float | None = -0.08,
    sample_size: int = 30,
    display_hint: str | None = None,
    is_simulated_only: bool = True,
) -> None:
    db.execute(
        Base.metadata.tables["sim_dashboard_snapshot"].insert().values(
            dashboard_snapshot_id=str(uuid4()),
            capital_tier=capital_tier,
            snapshot_date=date.fromisoformat(snapshot_date),
            data_status=data_status,
            status_reason=status_reason,
            total_return_pct=total_return_pct,
            win_rate=win_rate,
            profit_loss_ratio=profit_loss_ratio,
            alpha_annual=alpha_annual,
            max_drawdown_pct=max_drawdown_pct,
            sample_size=sample_size,
            display_hint=display_hint,
            is_simulated_only=is_simulated_only,
            created_at=utc_now(),
        )
    )
    db.commit()


def insert_sim_equity_curve_point(
    db,
    *,
    capital_tier: str,
    trade_date: str,
    equity: float,
    cash_available: float = 0.0,
    position_market_value: float = 0.0,
    drawdown_state: str = "NORMAL",
) -> None:
    db.execute(
        Base.metadata.tables["sim_equity_curve_point"].insert().values(
            equity_curve_point_id=str(uuid4()),
            capital_tier=capital_tier,
            trade_date=date.fromisoformat(trade_date),
            equity=equity,
            cash_available=cash_available,
            position_market_value=position_market_value,
            drawdown_state=drawdown_state,
            created_at=utc_now(),
        )
    )
    db.commit()


def insert_baseline_equity_curve_point(
    db,
    *,
    capital_tier: str,
    baseline_type: str,
    trade_date: str,
    equity: float,
) -> None:
    db.execute(
        Base.metadata.tables["baseline_equity_curve_point"].insert().values(
            baseline_equity_curve_point_id=str(uuid4()),
            capital_tier=capital_tier,
            baseline_type=baseline_type,
            trade_date=date.fromisoformat(trade_date),
            equity=equity,
            created_at=utc_now(),
        )
    )
    db.commit()
