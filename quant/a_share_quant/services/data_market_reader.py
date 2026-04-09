"""市场数据读取与 bundle 组装。"""
from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from a_share_quant.adapters.data.base import MarketDataBundle
from a_share_quant.domain.models import Bar, Security
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.services.data_lineage_builder import DataLineageBuilder
from a_share_quant.services.data_service_types import LoadedMarketData, StreamingMarketData
from a_share_quant.services.trading_session_service import TradingSessionService

logger = logging.getLogger(__name__)


class MarketDataReader:
    """负责行情读取与 bundle 组装。"""

    def __init__(
        self,
        market_repository: MarketRepository,
        lineage_builder: DataLineageBuilder,
        session_service: TradingSessionService,
    ) -> None:
        self.market_repository = market_repository
        self.lineage_builder = lineage_builder
        self.session_service = session_service

    def load_market_data_bundle(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> LoadedMarketData:
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only)
        bars_by_symbol = self.market_repository.load_bars_grouped(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        exchange_scope = self.lineage_builder._resolve_exchange_scope(securities=securities, requested_ts_codes=ts_codes)
        session = self.session_service.resolve_preloaded_session(
            securities=securities,
            bars_by_symbol=bars_by_symbol,
            exchange_scope=exchange_scope,
            start_date=start_date,
            end_date=end_date,
        )
        lineage = self.lineage_builder.build_data_lineage(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=session.trade_calendar,
            exchange_scope=exchange_scope,
            explicit_start_date=start_date,
            explicit_end_date=end_date,
            requested_ts_codes=ts_codes,
        )
        lineage.degradation_flags = sorted(set(lineage.degradation_flags) | set(session.degradation_flags))
        lineage.warnings = [*lineage.warnings, *session.warnings]
        return LoadedMarketData(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=session.trade_calendar,
            data_lineage=lineage,
        )

    def prepare_stream_market_data(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> StreamingMarketData:
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only)
        exchange_scope = self.lineage_builder._resolve_exchange_scope(securities=securities, requested_ts_codes=ts_codes)
        session = self.session_service.resolve_stream_session(
            securities=securities,
            exchange_scope=exchange_scope,
            start_date=start_date,
            end_date=end_date,
            requested_ts_codes=ts_codes,
        )
        data_start_date = start_date or (session.trade_dates[0] if session.trade_dates else None)
        data_end_date = end_date or (session.trade_dates[-1] if session.trade_dates else None)
        tracker, provisional_lineage = self.lineage_builder.prepare_stream_tracker(
            securities=securities,
            trade_dates=session.trade_dates,
            exchange_scope=exchange_scope,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            requested_ts_codes=ts_codes,
        )
        provisional_lineage.degradation_flags = sorted(set(provisional_lineage.degradation_flags) | set(session.degradation_flags))
        provisional_lineage.warnings = [*provisional_lineage.warnings, *session.warnings]
        return StreamingMarketData(
            day_batches=tracker.iter_day_batches(),
            securities=securities,
            trade_dates=session.trade_dates,
            data_lineage=provisional_lineage,
            lineage_tracker=tracker,
        )

    def stream_market_data(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> tuple[Iterator[tuple[date, dict[str, Bar]]], dict[str, Security], list[date]]:
        bundle = self.prepare_stream_market_data(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
        )
        return bundle.day_batches, bundle.securities, bundle.trade_dates


def log_bundle_degradation(provider_name: str, bundle: MarketDataBundle) -> None:
    if bundle.degradation_flags:
        logger.warning(
            "数据源发生降级 provider=%s degradation_flags=%s warnings=%s",
            provider_name,
            bundle.degradation_flags,
            bundle.warnings,
        )
