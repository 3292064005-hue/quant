"""交易日历提供器。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import TradingCalendarEntry
from a_share_quant.repositories.market_repository import MarketRepository


class CalendarProvider:
    """读取交易日历。"""

    def __init__(self, market_repository: MarketRepository) -> None:
        self.market_repository = market_repository

    def load(self, *, exchanges: list[str] | None = None, start_date: date | None = None, end_date: date | None = None) -> list[TradingCalendarEntry]:
        """加载交易日历。"""
        return self.market_repository.load_calendar(exchanges=exchanges, start_date=start_date, end_date=end_date)
