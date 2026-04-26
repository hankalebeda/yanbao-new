"""Fetch HS300 kline data and insert into DB to fix market_state_degraded."""
import httpx
import json
import math
from datetime import datetime
from uuid import uuid4
from sqlalchemy import create_engine, text

DB_URL = "sqlite:///D:/yanbao/data/app.db"

def main():
    e = create_engine(DB_URL)

    # Fetch HS300 kline from eastmoney
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": "1.000300",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "beg": "20260101",
        "end": "20260310",
        "lmt": "120",
    }

    client = httpx.Client(trust_env=False, timeout=15)
    r = client.get(url, params=params)
    data = r.json()
    klines_raw = data.get("data", {}).get("klines", [])
    print(f"Fetched {len(klines_raw)} HS300 klines")

    if not klines_raw:
        print("ERROR: No HS300 data!")
        return

    # Parse: date,open,close,high,low,volume,amount,...
    parsed = []
    for line in klines_raw:
        parts = line.split(",")
        parsed.append({
            "trade_date": parts[0],
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "volume": float(parts[5]),
            "amount": float(parts[6]),
        })

    print(f"Date range: {parsed[0]['trade_date']} to {parsed[-1]['trade_date']}")

    # Compute technical indicators
    closes = [p["close"] for p in parsed]

    def ma(n, idx):
        if idx < n - 1:
            return None
        return round(sum(closes[idx - n + 1 : idx + 1]) / n, 4)

    def vol_20d(idx):
        if idx < 20:
            return None
        window = closes[idx - 19 : idx + 1]
        rets = []
        for i in range(1, len(window)):
            if window[i - 1] > 0:
                rets.append(math.log(window[i] / window[i - 1]))
        if len(rets) < 2:
            return None
        avg = sum(rets) / len(rets)
        var = sum((r - avg) ** 2 for r in rets) / (len(rets) - 1)
        return round(math.sqrt(var), 6)

    def atr_pct(idx):
        if idx < 1:
            return None
        prev_c = parsed[idx - 1]["close"]
        h, l, c = parsed[idx]["high"], parsed[idx]["low"], parsed[idx]["close"]
        if c == 0:
            return None
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        return round(tr / c, 6)

    # Insert into DB
    batch_id = str(uuid4())
    with e.begin() as conn:
        conn.execute(text("DELETE FROM kline_daily WHERE stock_code = :c"), {"c": "000300.SH"})
        for i, p in enumerate(parsed):
            conn.execute(
                text("""
                    INSERT INTO kline_daily 
                    (kline_id, stock_code, trade_date, open, high, low, close, 
                     volume, amount, adjust_type, atr_pct, turnover_rate,
                     ma5, ma10, ma20, ma60, volatility_20d, hs300_return_20d,
                     is_suspended, source_batch_id, created_at)
                    VALUES 
                    (:kline_id, :stock_code, :trade_date, :open, :high, :low, :close,
                     :volume, :amount, :adjust_type, :atr_pct, :turnover_rate,
                     :ma5, :ma10, :ma20, :ma60, :volatility_20d, :hs300_return_20d,
                     :is_suspended, :source_batch_id, :created_at)
                """),
                {
                    "kline_id": str(uuid4()),
                    "stock_code": "000300.SH",
                    "trade_date": p["trade_date"],
                    "open": p["open"],
                    "high": p["high"],
                    "low": p["low"],
                    "close": p["close"],
                    "volume": p["volume"],
                    "amount": p["amount"],
                    "adjust_type": "none",
                    "atr_pct": atr_pct(i),
                    "turnover_rate": None,
                    "ma5": ma(5, i),
                    "ma10": ma(10, i),
                    "ma20": ma(20, i),
                    "ma60": ma(60, i),
                    "volatility_20d": vol_20d(i),
                    "hs300_return_20d": (
                        round((closes[i] - closes[max(0, i - 20)]) / closes[max(0, i - 20)], 6)
                        if i >= 20
                        else None
                    ),
                    "is_suspended": False,
                    "source_batch_id": batch_id,
                    "created_at": datetime.utcnow().isoformat(),
                },
            )
        print(f"Inserted {len(parsed)} HS300 klines")

    # Verify
    with e.connect() as conn:
        cnt = conn.execute(text("SELECT count(*) FROM kline_daily WHERE stock_code = :c"), {"c": "000300.SH"}).scalar()
        print(f"Verify: {cnt} HS300 klines in DB")


if __name__ == "__main__":
    main()
