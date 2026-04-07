"""数据源适配器基类与共享数据结构。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry


@dataclass(slots=True)
class MarketDataBundle:
    """统一市场数据载荷。"""

    bars: list[Bar] = field(default_factory=list)
    securities: dict[str, Security] = field(default_factory=dict)
    calendar: list[TradingCalendarEntry] = field(default_factory=list)
    degradation_flags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class MarketDataProvider(Protocol):
    """市场数据适配器协议。"""

    def fetch_bundle(
        self,
        start_date: str,
        end_date: str,
        ts_codes: list[str] | None = None,
        exchange: str = "SSE",
    ) -> MarketDataBundle:
        """抓取统一结构的市场数据。"""
