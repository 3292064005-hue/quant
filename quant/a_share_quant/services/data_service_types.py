"""DataService 对外数据契约与通用工具。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

from a_share_quant.domain.models import Bar, DataLineage, Security, TradingCalendarEntry


@dataclass(slots=True)
class LoadedMarketData:
    """主链读取到的预加载市场数据。"""

    bars_by_symbol: dict[str, list[Bar]] = field(default_factory=dict)
    securities: dict[str, Security] = field(default_factory=dict)
    trade_calendar: list[TradingCalendarEntry] = field(default_factory=list)
    data_lineage: DataLineage = field(default_factory=DataLineage)


@dataclass(slots=True)
class StreamingMarketData:
    """主链读取到的流式市场数据。"""

    day_batches: Iterator[tuple[date, dict[str, Bar]]]
    securities: dict[str, Security]
    trade_dates: list[date]
    data_lineage: DataLineage = field(default_factory=DataLineage)
    lineage_tracker: StreamLineageTracker | None = None



def update_digest(digest: hashlib._Hash, payload: dict) -> None:
    digest.update(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    digest.update(b"\n")


class StreamLineageTracker:
    """在单遍流式消费过程中累计 digest，并在消费完成后生成最终谱系。"""

    def __init__(
        self,
        *,
        base_digest: hashlib._Hash,
        day_batches: Iterator[tuple[date, dict[str, Bar]]],
        finalize_callback,
    ) -> None:
        self._digest = base_digest
        self._day_batches = day_batches
        self._finalize_callback = finalize_callback
        self._bar_symbols: set[str] = set()
        self._bar_count = 0
        self._closed = False
        self._lineage: DataLineage | None = None

    def iter_day_batches(self) -> Iterator[tuple[date, dict[str, Bar]]]:
        """包装原始流式迭代器，在消费过程中单遍累计 digest。"""
        if self._closed:
            raise RuntimeError("流式行情迭代器不能重复消费")
        try:
            for trade_date, day_bars in self._day_batches:
                for ts_code in sorted(day_bars):
                    bar = day_bars[ts_code]
                    self._bar_symbols.add(ts_code)
                    self._bar_count += 1
                    update_digest(
                        self._digest,
                        {
                            "ts_code": bar.ts_code,
                            "trade_date": trade_date.isoformat(),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": bar.volume,
                            "amount": bar.amount,
                            "adj_type": bar.adj_type,
                        },
                    )
                yield trade_date, day_bars
        finally:
            self._closed = True

    def finalize(self) -> DataLineage:
        """在流式消费完成后构造最终谱系。"""
        if self._lineage is not None:
            return self._lineage
        if not self._closed:
            raise RuntimeError("流式谱系尚未完成消费，不能提前生成最终 lineage")
        self._lineage = self._finalize_callback(
            dataset_digest=self._digest.hexdigest(),
            bar_symbols=sorted(self._bar_symbols),
            bar_count=self._bar_count,
        )
        return self._lineage
