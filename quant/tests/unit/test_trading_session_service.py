from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry
from a_share_quant.services.trading_session_service import TradingSessionService


class _Repo:
    def __init__(self, calendar, trade_dates=None):
        self._calendar = calendar
        self._trade_dates = trade_dates or []

    def load_calendar(self, **kwargs):
        return list(self._calendar)

    def load_bar_trade_dates(self, **kwargs):
        return list(self._trade_dates)


def _security() -> Security:
    return Security(ts_code="600000.SH", name="PF Bank", exchange="SSE", board="MAIN", is_st=False, status="L")


def _bar(trade_date: date) -> Bar:
    return Bar(ts_code="600000.SH", trade_date=trade_date, open=10.0, high=10.5, low=9.8, close=10.2, volume=1000.0, amount=10200.0, pre_close=10.0)


def test_demo_policy_derives_calendar_and_warns() -> None:
    service = TradingSessionService(_Repo(calendar=[]), SimpleNamespace(calendar_policy="demo", default_exchange="SSE"))
    result = service.resolve_preloaded_session(
        securities={"600000.SH": _security()},
        bars_by_symbol={"600000.SH": [_bar(date(2024, 1, 2))]},
        exchange_scope=["SSE"],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
    )
    assert result.trade_dates == [date(2024, 1, 2)]
    assert result.degradation_flags == ["calendar_inferred_from_bars"]
    assert result.warnings


def test_strict_policy_rejects_missing_calendar() -> None:
    service = TradingSessionService(_Repo(calendar=[]), SimpleNamespace(calendar_policy="strict", default_exchange="SSE"))
    with pytest.raises(ValueError):
        service.resolve_preloaded_session(
            securities={"600000.SH": _security()},
            bars_by_symbol={"600000.SH": [_bar(date(2024, 1, 2))]},
            exchange_scope=["SSE"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 2),
        )


def test_existing_calendar_absorbs_bar_dates_with_warning() -> None:
    calendar = [TradingCalendarEntry(exchange="SSE", cal_date=date(2024, 1, 2), is_open=True)]
    service = TradingSessionService(_Repo(calendar=calendar), SimpleNamespace(calendar_policy="derive", default_exchange="SSE"))
    result = service.resolve_preloaded_session(
        securities={"600000.SH": _security()},
        bars_by_symbol={"600000.SH": [_bar(date(2024, 1, 2)), _bar(date(2024, 1, 3))]},
        exchange_scope=["SSE"],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
    )
    assert result.trade_dates == [date(2024, 1, 2), date(2024, 1, 3)]
    assert "calendar_missing_trade_dates_for_bars" in result.degradation_flags
    assert result.warnings
