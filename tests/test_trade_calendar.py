from datetime import datetime
from zoneinfo import ZoneInfo

from app.services import trade_calendar


def test_latest_trade_date_str_weekend_with_calendar(monkeypatch):
    monkeypatch.setattr(trade_calendar, "_trade_days_set", lambda: {"2026-02-13", "2026-02-12"})
    dt = datetime(2026, 2, 15, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Sunday
    assert trade_calendar.latest_trade_date_str(dt) == "2026-02-13"


def test_is_trade_day_uses_calendar_set(monkeypatch):
    monkeypatch.setattr(trade_calendar, "_trade_days_set", lambda: {"2026-02-13"})
    dt_holiday = datetime(2026, 2, 16, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Monday but holiday in set logic
    assert trade_calendar.is_trade_day(dt_holiday) is False
    dt_trade = datetime(2026, 2, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert trade_calendar.is_trade_day(dt_trade) is True

