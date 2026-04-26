"""Debug FR01: check candidate count."""
import pytest
from datetime import date, timedelta
from uuid import uuid4
from app.models import Base
from app.services.stock_pool import _build_candidates, _normalize_filter_params 


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
        db_session.execute(stock_master.insert().values(
            stock_code=code, stock_name=stock_name, exchange="SH",
            industry=f"IND{index % 10}",
            list_date=trade_day - timedelta(days=500 + index),
            circulating_shares=circulating_shares,
            is_st=is_st, is_suspended=False, is_delisted=False,
        ))
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
            db_session.execute(kline_daily.insert().values(
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
            ))
    db_session.commit()


def test_debug_candidate_count(db_session):
    trade_date = "2026-03-09"
    _seed_universe(db_session, trade_date=trade_date)
    td = date.fromisoformat(trade_date)
    
    params = _normalize_filter_params(None)
    print(f"\nParams: {params}")
    
    from app.services.stock_pool import _history_map
    history = _history_map(db_session, td)
    print(f"History map size: {len(history)}")
    
    candidates = _build_candidates(db_session, td, params)
    print(f"Candidates: {len(candidates)}")
    print(f"Target: {params['target_pool_size']}")
    print(f"Sufficient: {len(candidates) >= params['target_pool_size']}")
    
    if len(candidates) < params['target_pool_size']:
        # Debug: check why some were filtered
        filtered = {"delisted": 0, "st": 0, "no_list_date": 0, "young": 0, 
                     "no_val": 0, "low_cap": 0, "low_amount": 0}
        for code, row in history.items():
            if row.get("is_delisted"):
                filtered["delisted"] += 1; continue
            if row.get("is_st"):
                filtered["st"] += 1; continue
            stock_name = str(row.get("stock_name") or code)
            import re
            if re.search(r"ST|[*]?ST|退|退市", stock_name, re.I):
                filtered["st"] += 1; continue
            list_date = row.get("list_date")
            if isinstance(list_date, str):
                list_date = date.fromisoformat(list_date)
            if not isinstance(list_date, date):
                filtered["no_list_date"] += 1; continue
            if (td - list_date).days < params["min_listing_days"]:
                filtered["young"] += 1; continue
            close = float(row.get("close") or 0)
            shares = float(row.get("circulating_shares") or 0) 
            amount = float(row.get("amount") or 0)
            if close <= 0 or shares <= 0 or amount <= 0:
                filtered["no_val"] += 1; continue
            if close * shares < params["min_market_cap_cny"]:
                filtered["low_cap"] += 1; continue
            if amount < params["min_avg_amount_20d_cny"]:
                filtered["low_amount"] += 1; continue
        print(f"Filtered breakdown: {filtered}")
