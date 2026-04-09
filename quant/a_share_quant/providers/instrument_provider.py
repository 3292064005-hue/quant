"""证券提供器。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import Security
from a_share_quant.repositories.market_repository import MarketRepository


class InstrumentProvider:
    """读取证券元数据。"""

    def __init__(self, market_repository: MarketRepository) -> None:
        self.market_repository = market_repository

    def load(self, *, ts_codes: list[str] | None = None, as_of_date: date | None = None, active_only: bool = False) -> dict[str, Security]:
        """加载证券集合。"""
        return self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only)
