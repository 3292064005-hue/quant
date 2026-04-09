"""行情提供器。"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from a_share_quant.domain.models import Bar
from a_share_quant.repositories.market_repository import MarketRepository


class BarProvider:
    """读取分组行情或按日流。"""

    def __init__(self, market_repository: MarketRepository) -> None:
        self.market_repository = market_repository

    def load_grouped(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, list[Bar]]:
        """加载按证券分组的行情。"""
        return self.market_repository.load_bars_grouped(start_date=start_date, end_date=end_date, ts_codes=ts_codes)

    def stream_daily(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> Iterator[tuple[date, dict[str, Bar]]]:
        """按交易日流式读取行情。"""
        return self.market_repository.stream_bars_by_trade_date(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
