"""
Debug FR01: intercept the 500 error response body.
Run inside pytest so we get the proper fixtures.
"""
import pytest
from datetime import date, timedelta
from uuid import uuid4
from app.models import Base


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
                stock_code=code, stock_name=stock_name, exchange="SH",
                industry=f"IND{index % 10}",
                list_date=trade_day - timedelta(days=500 + index),
                circulating_shares=circulating_shares,
                is_st=is_st, is_suspended=False, is_delisted=False,
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
                    kline_id=str(uuid4()), stock_code=code, trade_date=current_day,
                    open=round(close_price - 0.05, 4), high=round(close_price + 0.08, 4),
                    low=round(close_price - 0.08, 4), close=close_price,
                    volume=volume, amount=amount, adjust_type="front_adjusted",
                    atr_pct=0.03, turnover_rate=round(volume_ratio * 100, 4),
                    ma5=round(close_price - daily_trend * 0.6, 4),
                    ma10=round(close_price - daily_trend, 4),
                    ma20=ma20, ma60=round(close_price - daily_trend * 2.5, 4),
                    volatility_20d=0.02 + index * 0.00001, hs300_return_20d=0.05,
                    is_suspended=False, source_batch_id=str(uuid4()),
                )
            )
    db_session.commit()


def test_debug_fr01(client, db_session, create_user):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    admin = create_user(email="admin@x.com", password="Password123", role="admin", email_verified=True)
    login = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    response = client.post(
        "/api/v1/admin/pool/refresh",
        headers=headers,
        json={"trade_date": trade_date, "force_rebuild": False},
    )
    print(f"\n=== RESPONSE STATUS: {response.status_code} ===")
    print(f"=== RESPONSE BODY: {response.text[:2000]} ===")
    assert response.status_code == 200
