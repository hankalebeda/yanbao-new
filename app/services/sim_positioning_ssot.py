from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import Base
from app.services.runtime_materialization import ensure_sim_accounts

MAX_POSITIONS_BY_TIER = {"10k": 2, "100k": 5, "500k": 10}
INITIAL_CASH_BY_TIER = {"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}
DRAWDOWN_FACTOR_BY_STATE = {"NORMAL": 1.0, "REDUCE": 0.5, "HALT": 0.0}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _query_all(db: Session, sql_text: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(text(sql_text), params).mappings().all()]


def _query_one(db: Session, sql_text: str, params: dict[str, Any]) -> dict[str, Any] | None:
    row = db.execute(text(sql_text), params).mappings().first()
    return dict(row) if row else None


def _buy_cost(open_price: float, shares: int) -> tuple[float, float, float]:
    amount = open_price * shares
    commission = max(amount * 0.00025, 5.0)
    slippage = amount * 0.0005
    return amount + commission + slippage, commission, slippage


def _sell_proceeds(exit_price: float, shares: int) -> tuple[float, float, float, float]:
    amount = exit_price * shares
    commission = max(amount * 0.00025, 5.0)
    stamp_duty = amount * 0.0005
    slippage = amount * 0.0005
    return amount - commission - stamp_duty - slippage, commission, stamp_duty, slippage


def _load_accounts(db: Session) -> dict[str, dict[str, Any]]:
    rows = _query_all(db, "SELECT * FROM sim_account", {})
    return {str(row["capital_tier"]): row for row in rows}


def _load_kline(db: Session, *, stock_code: str, trade_day: date) -> dict[str, Any] | None:
    return _query_one(
        db,
        """
        SELECT open, high, low, close, volume, is_suspended
        FROM kline_daily
        WHERE stock_code = :stock_code AND trade_date = :trade_date
        LIMIT 1
        """,
        {"stock_code": stock_code, "trade_date": trade_day},
    )


def _update_position(db: Session, position_id: str, values: dict[str, Any]) -> None:
    table = Base.metadata.tables["sim_position"]
    db.execute(table.update().where(table.c.position_id == position_id).values(**values))


def _insert_position(db: Session, values: dict[str, Any]) -> None:
    table = Base.metadata.tables["sim_position"]
    db.execute(table.insert().values(**values))


def _close_positions(db: Session, *, trade_day: date, accounts: dict[str, dict[str, Any]]) -> None:
    positions = _query_all(
        db,
        """
        SELECT *
        FROM sim_position
        WHERE position_status = 'OPEN'
        ORDER BY entry_date ASC, created_at ASC
        """,
        {},
    )
    now = utc_now()
    for row in positions:
        tier = str(row["capital_tier"])
        if tier not in accounts:
            continue

        # FR08-SIM-01: 退市核销 DELISTED_LIQUIDATED
        delisted_row = _query_one(
            db,
            "SELECT is_delisted FROM stock_master WHERE stock_code = :sc LIMIT 1",
            {"sc": row["stock_code"]},
        )
        if delisted_row and delisted_row.get("is_delisted"):
            shares = int(row.get("shares") or 0)
            entry_date = _as_date(row["entry_date"])
            holding_days = (trade_day - entry_date).days if entry_date else 0
            _, buy_commission, buy_slippage = _buy_cost(float(row["actual_entry_price"]), shares)
            buy_paid = float(row["actual_entry_price"]) * shares + buy_commission + buy_slippage
            _update_position(
                db,
                row["position_id"],
                {
                    "position_status": "DELISTED_LIQUIDATED",
                    "exit_price": 0.0,
                    "exit_date": trade_day,
                    "holding_days": holding_days,
                    "net_return_pct": -1.0,
                    "commission_total": round(buy_commission, 4),
                    "stamp_duty": 0.0,
                    "slippage_total": round(buy_slippage, 4),
                    "updated_at": now,
                },
            )
            # 退市后资金归零（不回收）
            continue

        kline = _load_kline(db, stock_code=row["stock_code"], trade_day=trade_day)
        if not kline:
            continue
        entry_date = _as_date(row["entry_date"])
        holding_days = (trade_day - entry_date).days if entry_date else 0
        if bool(kline.get("is_suspended")) or float(kline.get("volume") or 0.0) <= 0:
            _update_position(
                db,
                row["position_id"],
                {
                    "holding_days": holding_days,
                    "suspended_pending": True,
                    "updated_at": now,
                },
            )
            continue

        # FR08-SIM-07: 一字涨跌停检测 — high==low 表示全天锁定无法成交
        if float(kline["high"]) > 0 and float(kline["high"]) == float(kline["low"]):
            _update_position(
                db,
                row["position_id"],
                {
                    "holding_days": holding_days,
                    "limit_locked_pending": True,
                    "updated_at": now,
                },
            )
            continue

        target_status = None
        exit_price = float(kline["close"])
        # FR08-SIM-03: 前复权动态护城河 — 用当前前复权序列重算止损/目标价
        raw_stop_loss = float(row.get("stop_loss_price") or 0.0)
        raw_target_price = float(row.get("target_price") or 0.0)
        raw_entry_price = float(row.get("actual_entry_price") or 0.0)
        # 获取入仓日的前复权收盘价
        entry_kline = _load_kline(db, stock_code=row["stock_code"], trade_day=_as_date(row["entry_date"]))
        if entry_kline and raw_entry_price > 0:
            adj_entry_close = float(entry_kline.get("close") or 0.0)
            if adj_entry_close > 0:
                # 复权因子 = 当前前复权入仓日close / 原始入仓价
                adj_factor = adj_entry_close / raw_entry_price
                stop_loss = round(raw_stop_loss * adj_factor, 4)
                target_price = round(raw_target_price * adj_factor, 4)
            else:
                stop_loss = raw_stop_loss
                target_price = raw_target_price
        else:
            stop_loss = raw_stop_loss
            target_price = raw_target_price
        if holding_days >= 180:
            target_status = "TIMEOUT"
        elif entry_date and entry_date < trade_day and float(kline["high"]) >= target_price and float(kline["low"]) <= stop_loss:
            target_status = "STOP_LOSS"
            exit_price = stop_loss
        elif entry_date and entry_date < trade_day and float(kline["low"]) <= stop_loss:
            target_status = "STOP_LOSS"
            exit_price = stop_loss
        elif entry_date and entry_date < trade_day and float(kline["high"]) >= target_price:
            target_status = "TAKE_PROFIT"
            exit_price = target_price

        if not target_status:
            _update_position(
                db,
                row["position_id"],
                {"holding_days": holding_days, "updated_at": now},
            )
            continue

        shares = int(row.get("shares") or 0)
        proceeds, sell_commission, stamp_duty, sell_slippage = _sell_proceeds(exit_price, shares)
        _, buy_commission, buy_slippage = _buy_cost(float(row["actual_entry_price"]), shares)
        buy_paid = float(row["actual_entry_price"]) * shares + buy_commission + buy_slippage
        net_return_pct = (proceeds - buy_paid) / buy_paid if buy_paid else 0.0
        accounts[tier]["cash_available"] = float(accounts[tier]["cash_available"]) + proceeds
        _update_position(
            db,
            row["position_id"],
            {
                "position_status": target_status,
                "exit_date": trade_day,
                "exit_price": round(exit_price, 4),
                "holding_days": holding_days,
                "net_return_pct": round(net_return_pct, 6),
                "commission_total": round(buy_commission + sell_commission, 4),
                "stamp_duty": round(stamp_duty, 4),
                "slippage_total": round(buy_slippage + sell_slippage, 4),
                "updated_at": now,
            },
        )
        # FR08-SIM-08: Outbox POSITION_CLOSED 事件派发
        from app.services.event_dispatcher import enqueue_position_closed_event
        enqueue_position_closed_event(
            db,
            position_id=row["position_id"],
            stock_code=row["stock_code"],
            trade_date=trade_day,
            capital_tier=tier,
            position_status=target_status,
            now=now,
        )


def _open_positions(db: Session, *, trade_day: date, accounts: dict[str, dict[str, Any]]) -> None:
    from datetime import timedelta
    # 超过 180 天的旧信号不得再开仓（与持仓超时门限一致，P0-08）
    cutoff_date = trade_day - timedelta(days=180)
    candidates = _query_all(
        db,
        """
        SELECT
            r.report_id,
            r.stock_code,
            r.trade_date,
            r.confidence,
            i.capital_tier,
            i.position_ratio,
            c.signal_entry_price,
            c.atr_pct,
            c.atr_multiplier,
            c.stop_loss,
            c.target_price
        FROM report r
        JOIN sim_trade_instruction i ON i.report_id = r.report_id
        JOIN instruction_card c ON c.report_id = r.report_id
        WHERE r.published = 1
          AND r.is_deleted = 0
          AND r.recommendation = 'BUY'
          AND i.status = 'EXECUTE'
          AND r.trade_date < :trade_date
          AND r.trade_date >= :cutoff_date
        ORDER BY i.capital_tier ASC, r.confidence DESC, r.report_id ASC
        """,
        {"trade_date": trade_day, "cutoff_date": cutoff_date},
    )
    now = utc_now()
    existing_pairs = {
        (row["report_id"], row["capital_tier"])
        for row in _query_all(
            db,
            "SELECT report_id, capital_tier FROM sim_position",
            {},
        )
    }

    by_tier: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        by_tier.setdefault(str(row["capital_tier"]), []).append(row)

    for tier, rows in by_tier.items():
        if tier not in accounts:
            continue
        rows = sorted(rows, key=lambda item: (-float(item["confidence"]), item["report_id"]))
        open_count = _query_one(
            db,
            """
            SELECT COUNT(*) AS total
            FROM sim_position
            WHERE capital_tier = :capital_tier AND position_status = 'OPEN'
            """,
            {"capital_tier": tier},
        )["total"]
        for row in rows:
            pair = (row["report_id"], tier)
            if pair in existing_pairs:
                continue
            if open_count >= MAX_POSITIONS_BY_TIER[tier]:
                break
            account = accounts[tier]
            if account["drawdown_state"] == "HALT":
                _insert_position(
                    db,
                    {
                        "position_id": str(uuid4()),
                        "report_id": row["report_id"],
                        "stock_code": row["stock_code"],
                        "capital_tier": tier,
                        "position_status": "SKIPPED",
                        "signal_date": _as_date(row["trade_date"]),
                        "entry_date": trade_day,
                        "actual_entry_price": None,
                        "signal_entry_price": row["signal_entry_price"],
                        "position_ratio": 0.0,
                        "shares": 0,
                        "atr_pct_snapshot": row["atr_pct"],
                        "atr_multiplier_snapshot": row["atr_multiplier"],
                        "stop_loss_price": row["stop_loss"],
                        "target_price": row["target_price"],
                        "exit_date": None,
                        "exit_price": None,
                        "holding_days": 0,
                        "net_return_pct": None,
                        "commission_total": None,
                        "stamp_duty": None,
                        "slippage_total": None,
                        "take_profit_pending_t1": False,
                        "stop_loss_pending_t1": False,
                        "suspended_pending": False,
                        "limit_locked_pending": False,
                        "skip_reason": "drawdown_halt",
                        "status_reason": "HALT",
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                existing_pairs.add(pair)
                continue

            kline = _load_kline(db, stock_code=row["stock_code"], trade_day=trade_day)
            if not kline:
                continue
            if bool(kline.get("is_suspended")) or float(kline.get("volume") or 0.0) <= 0:
                continue

            # FR08-SIM-07: 一字涨跌停 — 开仓日锁死无法买入
            if float(kline["high"]) > 0 and float(kline["high"]) == float(kline["low"]):
                continue

            effective_ratio = float(row["position_ratio"]) * float(account["drawdown_state_factor"])
            open_price = float(kline["open"])
            raw_shares = int((float(account["cash_available"]) * effective_ratio) // (open_price * 100)) * 100
            if raw_shares < 100:
                _insert_position(
                    db,
                    {
                        "position_id": str(uuid4()),
                        "report_id": row["report_id"],
                        "stock_code": row["stock_code"],
                        "capital_tier": tier,
                        "position_status": "SKIPPED",
                        "signal_date": _as_date(row["trade_date"]),
                        "entry_date": trade_day,
                        "actual_entry_price": None,
                        "signal_entry_price": row["signal_entry_price"],
                        "position_ratio": 0.0,
                        "shares": 0,
                        "atr_pct_snapshot": row["atr_pct"],
                        "atr_multiplier_snapshot": row["atr_multiplier"],
                        "stop_loss_price": row["stop_loss"],
                        "target_price": row["target_price"],
                        "exit_date": None,
                        "exit_price": None,
                        "holding_days": 0,
                        "net_return_pct": None,
                        "commission_total": None,
                        "stamp_duty": None,
                        "slippage_total": None,
                        "take_profit_pending_t1": False,
                        "stop_loss_pending_t1": False,
                        "suspended_pending": False,
                        "limit_locked_pending": False,
                        "skip_reason": "INSUFFICIENT_FUNDS",
                        "status_reason": None,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                existing_pairs.add(pair)
                continue

            shares = raw_shares
            total_cost, _, _ = _buy_cost(open_price, shares)
            while shares >= 100 and total_cost > float(account["cash_available"]):
                shares -= 100
                total_cost, _, _ = _buy_cost(open_price, shares)
            if shares < 100:
                continue

            account["cash_available"] = float(account["cash_available"]) - total_cost
            _insert_position(
                db,
                {
                    "position_id": str(uuid4()),
                    "report_id": row["report_id"],
                    "stock_code": row["stock_code"],
                    "capital_tier": tier,
                    "position_status": "OPEN",
                    "signal_date": _as_date(row["trade_date"]),
                    "entry_date": trade_day,
                    "actual_entry_price": round(open_price, 4),
                    "signal_entry_price": row["signal_entry_price"],
                    "position_ratio": round(effective_ratio, 6),
                    "shares": shares,
                    "atr_pct_snapshot": row["atr_pct"],
                    "atr_multiplier_snapshot": row["atr_multiplier"],
                    "stop_loss_price": row["stop_loss"],
                    "target_price": row["target_price"],
                    "exit_date": None,
                    "exit_price": None,
                    "holding_days": 0,
                    "net_return_pct": None,
                    "commission_total": None,
                    "stamp_duty": None,
                    "slippage_total": None,
                    "take_profit_pending_t1": False,
                    "stop_loss_pending_t1": False,
                    "suspended_pending": False,
                    "limit_locked_pending": False,
                    "skip_reason": None,
                    "status_reason": None,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            existing_pairs.add(pair)
            open_count += 1


def _reconcile_accounts(db: Session, *, trade_day: date, accounts: dict[str, dict[str, Any]]) -> None:
    account_table = Base.metadata.tables["sim_account"]
    equity_table = Base.metadata.tables["sim_equity_curve_point"]
    now = utc_now()
    for tier, account in accounts.items():
        open_positions = _query_all(
            db,
            """
            SELECT stock_code, shares, actual_entry_price
            FROM sim_position
            WHERE capital_tier = :capital_tier AND position_status = 'OPEN'
            """,
            {"capital_tier": tier},
        )
        market_value = 0.0
        for row in open_positions:
            kline = _load_kline(db, stock_code=row["stock_code"], trade_day=trade_day)
            close_price = float(kline["close"]) if kline else float(row["actual_entry_price"] or 0.0)
            market_value += close_price * int(row.get("shares") or 0)
        total_asset = float(account["cash_available"]) + market_value
        peak_total_asset = max(float(account["peak_total_asset"]), total_asset)
        current_drawdown = (total_asset / peak_total_asset - 1) if peak_total_asset else 0.0
        max_drawdown_pct = min(float(account["max_drawdown_pct"]), current_drawdown)
        if max_drawdown_pct <= -0.20:
            drawdown_state = "HALT"
        elif max_drawdown_pct <= -0.12:
            drawdown_state = "REDUCE"
        else:
            drawdown_state = "NORMAL"
        drawdown_state_factor = DRAWDOWN_FACTOR_BY_STATE[drawdown_state]
        # FR08-SIM-08: 回撤进入 REDUCE/HALT → 触发 DRAWDOWN_ALERT 事件
        prev_drawdown_state = str(account.get("drawdown_state", "NORMAL"))
        if drawdown_state in ("REDUCE", "HALT") and prev_drawdown_state != drawdown_state:
            from app.services.event_dispatcher import enqueue_drawdown_alert
            enqueue_drawdown_alert(
                db,
                account_id=str(account.get("account_id", tier)),
                drawdown_pct=round(max_drawdown_pct, 6),
                capital_tier=tier,
                drawdown_state=drawdown_state,
                now=now,
            )
        db.execute(
            account_table.update()
            .where(account_table.c.capital_tier == tier)
            .values(
                cash_available=round(float(account["cash_available"]), 2),
                total_asset=round(total_asset, 2),
                peak_total_asset=round(peak_total_asset, 2),
                max_drawdown_pct=round(max_drawdown_pct, 6),
                drawdown_state=drawdown_state,
                drawdown_state_factor=drawdown_state_factor,
                active_position_count=len(open_positions),
                last_reconciled_trade_date=trade_day,
                updated_at=now,
            )
        )
        existing_curve = _query_one(
            db,
            """
            SELECT equity_curve_point_id
            FROM sim_equity_curve_point
            WHERE capital_tier = :capital_tier AND trade_date = :trade_date
            LIMIT 1
            """,
            {"capital_tier": tier, "trade_date": trade_day},
        )
        curve_values = {
            "capital_tier": tier,
            "trade_date": trade_day,
            "equity": round(total_asset, 2),
            "cash_available": round(float(account["cash_available"]), 2),
            "position_market_value": round(market_value, 2),
            "drawdown_state": drawdown_state,
            "created_at": now,
        }
        if existing_curve:
            db.execute(
                equity_table.delete().where(equity_table.c.equity_curve_point_id == existing_curve["equity_curve_point_id"])
            )
        db.execute(
            equity_table.insert().values(
                equity_curve_point_id=str(uuid4()),
                **curve_values,
            )
        )


def process_trade_date(db: Session, trade_date: str) -> None:
    trade_day = date.fromisoformat(trade_date)
    ensure_sim_accounts(db)
    accounts = _load_accounts(db)
    if not accounts:
        return
    _close_positions(db, trade_day=trade_day, accounts=accounts)
    _open_positions(db, trade_day=trade_day, accounts=accounts)
    _reconcile_accounts(db, trade_day=trade_day, accounts=accounts)
    db.commit()
