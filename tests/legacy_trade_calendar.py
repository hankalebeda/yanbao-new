from datetime import datetime
from zoneinfo import ZoneInfo

from app.services import trade_calendar


def test_latest_trade_date_str_weekend_with_calendar(monkeypatch):
    monkeypatch.setattr(trade_calendar, "_trade_days_set", lambda: {"2026-02-13", "2026-02-12"})
    dt = datetime(2026, 2, 15, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert trade_calendar.latest_trade_date_str(dt) == "2026-02-13"


def test_is_trade_day_uses_known_calendar_then_future_weekday_fallback(monkeypatch):
    monkeypatch.setattr(trade_calendar, "_trade_days_set", lambda: {"2026-02-13"})

    known_trade_day = datetime(2026, 2, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    missing_historical_day = datetime(2026, 2, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    future_weekday = datetime(2026, 2, 16, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert trade_calendar.is_trade_day(known_trade_day) is True
    assert trade_calendar.is_trade_day(missing_historical_day) is False
    assert trade_calendar.is_trade_day(future_weekday) is True
