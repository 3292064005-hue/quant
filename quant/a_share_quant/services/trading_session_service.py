"""交易日历/会话解析服务。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from a_share_quant.config.models import DataSection
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry
from a_share_quant.repositories.market_repository import MarketRepository


@dataclass(slots=True)
class SessionResolution:
    """交易会话解析结果。"""

    trade_calendar: list[TradingCalendarEntry] = field(default_factory=list)
    trade_dates: list[date] = field(default_factory=list)
    degradation_flags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class TradingSessionService:
    """以显式策略解析正式交易日历与交易日。"""

    def __init__(self, market_repository: MarketRepository, data_config: DataSection) -> None:
        self.market_repository = market_repository
        self.data_config = data_config

    def resolve_preloaded_session(
        self,
        *,
        securities: dict[str, Security],
        bars_by_symbol: dict[str, list[Bar]],
        exchange_scope: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> SessionResolution:
        calendar = self.market_repository.load_calendar(exchanges=exchange_scope, start_date=start_date, end_date=end_date)
        bar_trade_dates = sorted({bar.trade_date for bars in bars_by_symbol.values() for bar in bars})
        return self._resolve(calendar=calendar, bar_trade_dates=bar_trade_dates, exchange_scope=exchange_scope)

    def resolve_stream_session(
        self,
        *,
        securities: dict[str, Security],
        exchange_scope: list[str],
        start_date: date | None,
        end_date: date | None,
        requested_ts_codes: list[str] | None,
    ) -> SessionResolution:
        del securities
        calendar = self.market_repository.load_calendar(exchanges=exchange_scope, start_date=start_date, end_date=end_date)
        if hasattr(self.market_repository, "load_bar_trade_dates"):
            bar_trade_dates = self.market_repository.load_bar_trade_dates(start_date=start_date, end_date=end_date)
        else:  # pragma: no cover - 兼容精简仓储桩对象
            try:
                bar_trade_dates = self.market_repository.load_trade_dates(
                    start_date=start_date,
                    end_date=end_date,
                    ts_codes=requested_ts_codes,
                )
            except TypeError:
                bar_trade_dates = self.market_repository.load_trade_dates(start_date=start_date, end_date=end_date)
        return self._resolve(calendar=calendar, bar_trade_dates=bar_trade_dates, exchange_scope=exchange_scope)

    def _resolve(
        self,
        *,
        calendar: list[TradingCalendarEntry],
        bar_trade_dates: list[date],
        exchange_scope: list[str],
    ) -> SessionResolution:
        policy = self.data_config.calendar_policy
        open_calendar_dates = sorted({item.cal_date for item in calendar if item.is_open})
        degradation_flags: list[str] = []
        warnings: list[str] = []
        if open_calendar_dates:
            if set(bar_trade_dates) - set(open_calendar_dates):
                degradation_flags.append("calendar_missing_trade_dates_for_bars")
                warnings.append("部分 bar 交易日未出现在正式交易日历中，已将 bar 日期并入会话集合")
            trade_dates = sorted(set(open_calendar_dates) | set(bar_trade_dates))
            return SessionResolution(trade_calendar=calendar, trade_dates=trade_dates, degradation_flags=degradation_flags, warnings=warnings)

        if not bar_trade_dates:
            return SessionResolution(trade_calendar=[], trade_dates=[], degradation_flags=[], warnings=[])

        if policy == "strict":
            raise ValueError(
                f"交易日历缺失且 data.calendar_policy=strict；exchange_scope={exchange_scope or ['*']}，禁止继续使用 bars 日期隐式替代正式交易日历"
            )

        derived_calendar = [
            TradingCalendarEntry(exchange=(exchange_scope[0] if exchange_scope else self.data_config.default_exchange), cal_date=item, is_open=True)
            for item in bar_trade_dates
        ]
        degradation_flags.append("calendar_inferred_from_bars")
        if policy == "derive":
            warnings.append("交易日历缺失，已按 bars 交易日推导正式会话；请尽快补齐交易所日历")
        else:
            warnings.append("交易日历缺失，当前处于 demo 会话模式，已按 bars 交易日推导")
        return SessionResolution(trade_calendar=derived_calendar, trade_dates=bar_trade_dates, degradation_flags=degradation_flags, warnings=warnings)
