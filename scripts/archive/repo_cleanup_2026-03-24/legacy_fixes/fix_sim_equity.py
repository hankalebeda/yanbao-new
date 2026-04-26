"""Fix sim equity: debit cash for all positions, recompute equity curves & dashboard.

The DB has initial_cash = 100K/1M/5M (positions sized for these values).
Bug: positions opened on 2026-03-10 didn't debit cash_available.
Fix: recalculate cash, recompute equity curve points and dashboard snapshot.
"""
import sqlite3
from uuid import uuid4

DB_PATH = "data/app.db"


def buy_cost(price, shares):
    amount = price * shares
    commission = max(amount * 0.00025, 5.0)
    slippage = amount * 0.001
    return amount + commission + slippage


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    for tier in ["10k", "100k", "500k"]:
        acct = cur.execute(
            "SELECT initial_cash FROM sim_account WHERE capital_tier = ?", (tier,)
        ).fetchone()
        if not acct:
            continue
        init_cash = float(acct["initial_cash"])

        positions = cur.execute(
            "SELECT position_id, stock_code, position_status, actual_entry_price, "
            "shares, exit_price FROM sim_position WHERE capital_tier = ?", (tier,)
        ).fetchall()

        total_buy = 0.0
        total_sell = 0.0
        open_list = []

        for p in positions:
            ep = float(p["actual_entry_price"] or 0)
            sh = int(p["shares"] or 0)
            if ep <= 0 or sh <= 0:
                continue
            total_buy += buy_cost(ep, sh)
            if p["position_status"] == "OPEN":
                open_list.append({"stock_code": p["stock_code"], "shares": sh, "entry_price": ep})
            elif p["exit_price"]:
                sell_amt = float(p["exit_price"]) * sh
                sell_comm = max(sell_amt * 0.00025, 5.0)
                stamp = sell_amt * 0.0005
                sell_slip = sell_amt * 0.001
                total_sell += sell_amt - sell_comm - stamp - sell_slip

        correct_cash = init_cash - total_buy + total_sell
        print(f"\n=== {tier} ===")
        print(f"  initial={init_cash}, buy={total_buy:.2f}, sell={total_sell:.2f}, cash={correct_cash:.2f}")

        curve_dates = cur.execute(
            "SELECT DISTINCT trade_date FROM sim_equity_curve_point "
            "WHERE capital_tier = ? ORDER BY trade_date", (tier,)
        ).fetchall()

        peak = init_cash
        for cdt in curve_dates:
            td = cdt["trade_date"]
            mv = 0.0
            for op in open_list:
                kl = cur.execute(
                    "SELECT close FROM kline_daily WHERE stock_code = ? AND trade_date = ? LIMIT 1",
                    (op["stock_code"], td)
                ).fetchone()
                close = float(kl["close"]) if kl else op["entry_price"]
                mv += close * op["shares"]
            total_asset = correct_cash + mv
            peak = max(peak, total_asset)
            dd = (total_asset / peak - 1) if peak > 0 else 0.0
            cur.execute(
                "UPDATE sim_equity_curve_point SET equity=?, cash_available=?, position_market_value=? "
                "WHERE capital_tier=? AND trade_date=?",
                (round(total_asset, 2), round(correct_cash, 2), round(mv, 2), tier, td)
            )
            print(f"  {td}: mv={mv:.2f}, equity={total_asset:.2f}, dd={dd:.4%}")

        latest_equity = cur.execute(
            "SELECT equity FROM sim_equity_curve_point WHERE capital_tier=? ORDER BY trade_date DESC LIMIT 1",
            (tier,)
        ).fetchone()
        if latest_equity:
            final_equity = float(latest_equity["equity"])
            dd_final = (final_equity / peak - 1) if peak > 0 else 0.0
            ret_pct = (final_equity / init_cash) - 1.0
            cur.execute(
                "UPDATE sim_account SET cash_available=?, total_asset=?, peak_total_asset=?, max_drawdown_pct=? "
                "WHERE capital_tier=?",
                (round(correct_cash, 2), round(final_equity, 2), round(peak, 2), round(dd_final, 6), tier)
            )
            cur.execute(
                "UPDATE sim_dashboard_snapshot SET total_return_pct=?, max_drawdown_pct=? WHERE capital_tier=?",
                (round(ret_pct, 6), round(dd_final, 6), tier)
            )
            print(f"  return={ret_pct:.2%}, drawdown={dd_final:.4%}")

    conn.commit()
    print("\n--- Done ---")
    conn.close()


if __name__ == "__main__":
    main()
