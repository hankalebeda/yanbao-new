from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import time
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.feature("FR-01"),
    pytest.mark.feature("FR01-POOL-01"),
    pytest.mark.feature("FR01-POOL-02"),
    pytest.mark.feature("FR01-POOL-03"),
    pytest.mark.feature("FR01-POOL-04"),
    pytest.mark.feature("FR01-POOL-05"),
]

from fastapi.testclient import TestClient

from app.api import routes_admin
from app.models import Base
from app.services import stock_pool as stock_pool_service


def _auth_headers(client, create_user):
    admin = create_user(
        email="admin-fr01@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": admin["user"].email, "password": admin["password"]},
    )
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


def _seed_universe(db_session, *, trade_date: str, total_stocks: int = 206):
    stock_master = Base.metadata.tables["stock_master"]
    kline_daily = Base.metadata.tables["kline_daily"]
    trade_day = date.fromisoformat(trade_date)
    start_day = trade_day - timedelta(days=24)

    for index in range(total_stocks):
        code = f"{600000 + index:06d}.SH"
        is_st = index == total_stocks - 1
        stock_name = f"STOCK{index:03d}" if not is_st else f"ST{index:03d}"
        circulating_shares = 800_000_000 + index * 1_000_000
        db_session.execute(
            stock_master.insert().values(
                stock_code=code,
                stock_name=stock_name,
                exchange="SH",
                industry=f"IND{index % 10}",
                list_date=trade_day - timedelta(days=500 + index),
                circulating_shares=circulating_shares,
                is_st=is_st,
                is_suspended=False,
                is_delisted=False,
            )
        )

        base_price = 8 + index * 0.04
        daily_trend = 0.03 + index * 0.001
        acceleration = index * 0.00005
        volume_ratio = min(0.006 + index * 0.00015, 0.12)
        for offset in range(25):
            current_day = start_day + timedelta(days=offset)
            trend_boost = max(offset - 10, 0) * acceleration
            close_price = round(base_price + offset * daily_trend + trend_boost, 4)
            volume = round(circulating_shares * volume_ratio, 2)
            amount = round(volume * close_price, 2)
            ma20 = round(close_price - (daily_trend * (1.8 - min(index / total_stocks, 0.75))), 4)
            db_session.execute(
                kline_daily.insert().values(
                    kline_id=str(uuid4()),
                    stock_code=code,
                    trade_date=current_day,
                    open=round(close_price - 0.05, 4),
                    high=round(close_price + 0.08, 4),
                    low=round(close_price - 0.08, 4),
                    close=close_price,
                    volume=volume,
                    amount=amount,
                    adjust_type="front_adjusted",
                    atr_pct=0.03 + index * 0.00001,
                    turnover_rate=round(volume_ratio * 100, 4),
                    ma5=round(close_price - daily_trend * 0.6, 4),
                    ma10=round(close_price - daily_trend, 4),
                    ma20=ma20,
                    ma60=round(close_price - daily_trend * 2.5, 4),
                    volatility_20d=0.02 + index * 0.00001,
                    hs300_return_20d=0.05,
                    is_suspended=False,
                    source_batch_id=str(uuid4()),
                )
            )
    db_session.commit()


def _seed_previous_pool(db_session, *, trade_date: str, stock_codes: list[str]):
    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    trade_day = date.fromisoformat(trade_date)
    task_id = str(uuid4())
    db_session.execute(
        refresh_task.insert().values(
            task_id=task_id,
            trade_date=trade_day,
            status="COMPLETED",
            pool_version=1,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=200,
            standby_pool_size=max(len(stock_codes) - 200, 0),
            evicted_stocks_json=[],
            status_reason=None,
            request_id="seed-prev",
        )
    )
    for rank, stock_code in enumerate(stock_codes[:200], start=1):
        db_session.execute(
            snapshot.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=trade_day,
                pool_version=1,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank,
                score=90 - rank * 0.01,
                is_suspended=False,
            )
        )
    for rank, stock_code in enumerate(stock_codes[200:250], start=1):
        db_session.execute(
            snapshot.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=trade_day,
                pool_version=1,
                stock_code=stock_code,
                pool_role="standby",
                rank_no=rank,
                score=70 - rank * 0.01,
                is_suspended=False,
            )
        )
    db_session.commit()


def test_fr01_pool_no_st(client, db_session, create_user):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    headers = _auth_headers(client, create_user)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers | {"X-Request-ID": "req-fr01-no-st"},
        json={"trade_date": trade_date, "force_rebuild": False},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-fr01-no-st"
    task_id = body["data"]["task_id"]

    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    core_codes = [
        row.stock_code
        for row in db_session.execute(
            snapshot.select().where(
                snapshot.c.refresh_task_id == task_id,
                snapshot.c.pool_role == "core",
            )
        ).fetchall()
    ]
    assert "600205.SH" not in core_codes


def test_fr01_pool_size_200(client, db_session, create_user):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    headers = _auth_headers(client, create_user)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "COMPLETED"
    assert body["data"]["core_pool_size"] == 200
    assert body["data"]["standby_pool_size"] >= 1

    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    rows = db_session.execute(
        snapshot.select().where(snapshot.c.refresh_task_id == body["data"]["task_id"])
    ).fetchall()
    core_scores = [float(row.score) for row in rows if row.pool_role == "core"]
    standby_scores = [float(row.score) for row in rows if row.pool_role == "standby"]
    assert len(core_scores) == 200
    assert standby_scores
    assert max(standby_scores) < min(core_scores)


def test_fr01_scheduler_handler_refreshes_same_day_pool(isolated_app, monkeypatch):
    from app.services import scheduler as scheduler_service

    trade_date = "2026-03-09"
    sessionmaker = isolated_app["sessionmaker"]
    monkeypatch.setattr(scheduler_service, "SessionLocal", sessionmaker)
    seed_db = sessionmaker()
    try:
        _seed_universe(seed_db, trade_date=trade_date)
    finally:
        seed_db.close()

    result = scheduler_service._handler_fr01_stock_pool(date.fromisoformat(trade_date))

    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    verify_db = sessionmaker()
    try:
        row = verify_db.execute(
            refresh_task.select()
            .where(refresh_task.c.trade_date == date.fromisoformat(trade_date))
            .order_by(refresh_task.c.created_at.desc())
        ).first()
    finally:
        verify_db.close()

    assert row is not None
    assert row.status == "COMPLETED"
    assert result["pool_size"] == 200
    assert result["trade_date"] == trade_date


def test_fr01_pool_excludes_stale_latest_kline_rows(client, db_session, create_user):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    headers = _auth_headers(client, create_user)

    stale_code = "600200.SH"
    kline_daily = Base.metadata.tables["kline_daily"]
    db_session.execute(
        kline_daily.delete().where(
            kline_daily.c.stock_code == stale_code,
            kline_daily.c.trade_date == date.fromisoformat(trade_date),
        )
    )
    db_session.commit()

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    rows = db_session.execute(
        snapshot.select().where(snapshot.c.refresh_task_id == response.json()["data"]["task_id"])
    ).fetchall()
    selected_codes = {row.stock_code for row in rows}
    assert stale_code not in selected_codes


def test_fr01_fallback_on_fail(client, db_session, create_user, monkeypatch):
    previous_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(206)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=all_codes)
    headers = _auth_headers(client, create_user)

    def fail_history_map(*args, **kwargs):
        raise RuntimeError("mocked_upstream_failure")

    monkeypatch.setattr(stock_pool_service, "_history_map", fail_history_map)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "FALLBACK"
    assert body["data"]["fallback_from"] == previous_trade_date
    assert body["data"]["status_reason"] == "mocked_upstream_failure"
    assert body["data"]["core_pool_size"] == 200


def test_fr01_exact_pool_view_can_explicitly_accept_same_day_fallback(client, db_session, create_user, monkeypatch):
    previous_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(206)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=all_codes)

    def fail_history_map(*args, **kwargs):
        raise RuntimeError("mocked_upstream_failure")

    monkeypatch.setattr(stock_pool_service, "_history_map", fail_history_map)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=_auth_headers(client, create_user),
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    exact_default = stock_pool_service.get_exact_pool_view(db_session, trade_date=trade_date)
    exact_with_fallback = stock_pool_service.get_exact_pool_view(
        db_session,
        trade_date=trade_date,
        allow_fallback_as_runtime_anchor=True,
    )
    daily_pool = stock_pool_service.get_daily_stock_pool(
        trade_date=trade_date,
        exact_trade_date=True,
        allow_same_day_fallback=True,
    )

    assert exact_default is None
    assert exact_with_fallback is not None
    assert exact_with_fallback.task.trade_date.isoformat() == trade_date
    assert exact_with_fallback.task.status == "FALLBACK"
    assert exact_with_fallback.task.fallback_from.isoformat() == previous_trade_date
    assert len(daily_pool) == 200


def test_fr01_fallback_when_trade_day_kline_coverage_is_incomplete(client, db_session, create_user):
    previous_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date, total_stocks=260)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(250)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=all_codes)

    kline_daily = Base.metadata.tables["kline_daily"]
    for index in range(60, 260):
        db_session.execute(
            kline_daily.delete().where(
                kline_daily.c.stock_code == f"{600000 + index:06d}.SH",
                kline_daily.c.trade_date == date.fromisoformat(trade_date),
            )
        )
    db_session.commit()

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=_auth_headers(client, create_user),
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "FALLBACK"
    assert body["data"]["fallback_from"] == previous_trade_date
    assert body["data"]["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert body["data"]["core_pool_size"] == 200


def test_fr01_rebuilds_existing_kline_fallback_once_coverage_recovers(client, db_session, create_user):
    previous_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date, total_stocks=260)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(250)]
    missing_codes = [f"{600000 + index:06d}.SH" for index in range(60, 260)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=all_codes)

    kline_daily = Base.metadata.tables["kline_daily"]
    removed_rows = db_session.execute(
        kline_daily.select().where(
            kline_daily.c.trade_date == date.fromisoformat(trade_date),
            kline_daily.c.stock_code.in_(missing_codes),
        )
    ).mappings().all()
    for row in removed_rows:
        db_session.execute(
            kline_daily.delete().where(kline_daily.c.kline_id == row["kline_id"])
        )
    db_session.commit()

    headers = _auth_headers(client, create_user)
    first = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert first.status_code == 200
    first_data = first.json()["data"]
    assert first_data["status"] == "FALLBACK"
    assert first_data["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert first_data["fallback_from"] == previous_trade_date

    for row in removed_rows:
        restored = dict(row)
        restored.pop("kline_id", None)
        db_session.execute(
            kline_daily.insert().values(
                kline_id=str(uuid4()),
                **restored,
            )
        )
    db_session.commit()

    second = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert second.status_code == 200
    second_data = second.json()["data"]
    assert second_data["status"] == "COMPLETED"
    assert second_data["status_reason"] is None
    assert second_data["fallback_from"] is None
    assert second_data["trade_date"] == trade_date


def test_fr01_split_core_pool_reserves_low_vol_candidates():
    params = stock_pool_service._normalize_filter_params()
    candidates = []
    for index in range(240):
        candidates.append(
            stock_pool_service.Candidate(
                stock_code=f"{600000 + index:06d}.SH",
                stock_name=f"B{index:03d}",
                industry=f"IND{index % 10}",
                is_suspended=False,
                score=1000 - index,
                factor_values={},
                low_vol_candidate=False,
            )
        )
    for index in range(20):
        candidates.append(
            stock_pool_service.Candidate(
                stock_code=f"{601000 + index:06d}.SH",
                stock_name=f"C{index:03d}",
                industry=f"LOW{index % 10}",
                is_suspended=False,
                score=700 - index,
                factor_values={},
                low_vol_candidate=True,
            )
        )

    core, standby = stock_pool_service._split_core_and_standby(candidates, params)

    assert len(core) == 200
    assert len(standby) == stock_pool_service.STANDBY_POOL_SIZE
    assert sum(1 for candidate in core if candidate.low_vol_candidate) >= stock_pool_service.MIN_LOW_VOL_CORE_COUNT


def test_fr01_core_to_standby_demotion_does_not_write_evicted_snapshot(client, db_session, create_user, monkeypatch):
    previous_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(250)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=all_codes)
    headers = _auth_headers(client, create_user)

    def fake_build_candidates(*args, **kwargs):
        candidates = []
        for index, stock_code in enumerate(all_codes):
            score = 1000 - index
            if stock_code == "600199.SH":
                score = 50
            elif stock_code == "600200.SH":
                score = 999.5
            candidates.append(
                stock_pool_service.Candidate(
                    stock_code=stock_code,
                    stock_name=stock_code,
                    industry=f"IND{index % 10}",
                    is_suspended=False,
                    score=score,
                    factor_values={},
                )
            )
        return candidates

    monkeypatch.setattr(stock_pool_service, "_build_candidates", fake_build_candidates)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "COMPLETED"
    assert "600199.SH" in body["data"]["evicted_stocks"]

    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    rows = db_session.execute(
        snapshot.select().where(snapshot.c.refresh_task_id == body["data"]["task_id"])
    ).mappings().all()
    standby_codes = {row["stock_code"] for row in rows if row["pool_role"] == "standby"}
    evicted_codes = {row["stock_code"] for row in rows if row["pool_role"] == "evicted"}
    assert "600199.SH" in standby_codes
    assert "600199.SH" not in evicted_codes
    assert standby_codes.isdisjoint(evicted_codes)


def test_fr01_fallback_skips_corrupt_previous_snapshot(client, db_session, create_user, monkeypatch):
    valid_trade_date = "2026-03-07"
    corrupt_trade_date = "2026-03-08"
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    all_codes = [f"{600000 + index:06d}.SH" for index in range(206)]
    _seed_previous_pool(db_session, trade_date=valid_trade_date, stock_codes=all_codes)

    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    corrupt_task_id = str(uuid4())
    db_session.execute(
        refresh_task.insert().values(
            task_id=corrupt_task_id,
            trade_date=date.fromisoformat(corrupt_trade_date),
            status="COMPLETED",
            pool_version=1,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=2,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id="seed-corrupt",
        )
    )
    db_session.execute(
        snapshot.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=corrupt_task_id,
            trade_date=date.fromisoformat(corrupt_trade_date),
            pool_version=1,
            stock_code=all_codes[0],
            pool_role="core",
            rank_no=1,
            score=99.0,
            is_suspended=False,
        )
    )
    db_session.commit()

    def fail_history_map(*args, **kwargs):
        raise RuntimeError("mocked_upstream_failure")

    monkeypatch.setattr(stock_pool_service, "_history_map", fail_history_map)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=_auth_headers(client, create_user),
        json={"trade_date": trade_date},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "FALLBACK"
    assert body["data"]["fallback_from"] == valid_trade_date
    assert body["data"]["core_pool_size"] == 200


def test_fr01_effective_pool_view_skips_full_core_snapshot_without_standby(db_session):
    previous_trade_date = "2026-03-08"
    corrupt_trade_date = "2026-03-09"
    stock_codes = [f"{600000 + index:06d}.SH" for index in range(250)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=stock_codes)

    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    corrupt_task_id = str(uuid4())
    db_session.execute(
        refresh_task.insert().values(
            task_id=corrupt_task_id,
            trade_date=date.fromisoformat(corrupt_trade_date),
            status="COMPLETED",
            pool_version=2,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=200,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id="seed-invalid-full-core",
        )
    )
    for rank, stock_code in enumerate(stock_codes[:200], start=1):
        db_session.execute(
            snapshot.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=corrupt_task_id,
                trade_date=date.fromisoformat(corrupt_trade_date),
                pool_version=2,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank,
                score=90 - rank * 0.01,
                is_suspended=False,
            )
        )
    db_session.commit()

    view = stock_pool_service.get_effective_pool_view(db_session, trade_date=corrupt_trade_date)

    assert view is not None
    assert view.task.trade_date.isoformat() == previous_trade_date
    assert len(view.core_rows) == 200
    assert len(view.standby_rows) > 0


def test_fr01_pool_views_reject_underfilled_completed_snapshot(db_session):
    previous_trade_date = "2026-03-19"
    corrupt_trade_date = "2026-03-20"
    stock_codes = [f"{600000 + index:06d}.SH" for index in range(250)]
    _seed_previous_pool(db_session, trade_date=previous_trade_date, stock_codes=stock_codes)

    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    corrupt_task_id = str(uuid4())
    db_session.execute(
        refresh_task.insert().values(
            task_id=corrupt_task_id,
            trade_date=date.fromisoformat(corrupt_trade_date),
            status="COMPLETED",
            pool_version=2,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=1,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id="seed-underfilled-runtime-batch",
        )
    )
    db_session.execute(
        snapshot.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=corrupt_task_id,
            trade_date=date.fromisoformat(corrupt_trade_date),
            pool_version=2,
            stock_code=stock_codes[0],
            pool_role="core",
            rank_no=1,
            score=99.0,
            is_suspended=False,
        )
    )
    db_session.commit()

    exact = stock_pool_service.get_exact_pool_view(db_session, trade_date=corrupt_trade_date)
    effective = stock_pool_service.get_effective_pool_view(db_session, trade_date=corrupt_trade_date)

    assert exact is None
    assert effective is not None
    assert effective.task.trade_date.isoformat() == previous_trade_date
    assert len(effective.core_rows) == 200
    assert len(effective.standby_rows) == 50


def test_fr01_concurrent_conflict_409(client, db_session, create_user, isolated_app, monkeypatch):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    headers = _auth_headers(client, create_user)
    original_build_candidates = stock_pool_service._build_candidates

    def slow_build_candidates(*args, **kwargs):
        time.sleep(0.3)
        return original_build_candidates(*args, **kwargs)

    monkeypatch.setattr(stock_pool_service, "_build_candidates", slow_build_candidates)

    def do_refresh():
        db = isolated_app["sessionmaker"]()
        try:
            data = stock_pool_service.refresh_stock_pool(db, trade_date=trade_date)
            return ("ok", data)
        except stock_pool_service.PoolRefreshConflict:
            db.rollback()
            return ("conflict", None)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(do_refresh)
        second = executor.submit(do_refresh)
        outcomes = [first.result(), second.result()]

    assert sorted(result[0] for result in outcomes) == ["conflict", "ok"]

    refresh_task = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot = Base.metadata.tables["stock_pool_snapshot"]
    task_rows = db_session.execute(
        refresh_task.select().where(refresh_task.c.trade_date == date.fromisoformat(trade_date))
    ).fetchall()
    assert len(task_rows) == 1
    assert task_rows[0].status == "COMPLETED"

    snapshot_rows = db_session.execute(
        snapshot.select().where(snapshot.c.trade_date == date.fromisoformat(trade_date))
    ).fetchall()
    assert {row.pool_version for row in snapshot_rows} == {task_rows[0].pool_version}

    def raise_conflict(*args, **kwargs):
        raise stock_pool_service.PoolRefreshConflict(trade_date)

    monkeypatch.setattr(routes_admin, "refresh_stock_pool", raise_conflict)

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date, "force_rebuild": True},
    )

    assert response.status_code == 409
    assert response.json()["error_code"] == "CONCURRENT_CONFLICT"
