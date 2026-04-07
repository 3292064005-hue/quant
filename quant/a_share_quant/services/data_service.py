"""数据服务。"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterator

from a_share_quant.adapters.data.akshare_adapter import AKShareDataAdapter
from a_share_quant.adapters.data.base import MarketDataBundle, MarketDataProvider
from a_share_quant.adapters.data.csv_adapter import CSVDataAdapter
from a_share_quant.adapters.data.tushare_adapter import TushareDataAdapter
from a_share_quant.config.models import DataSection
from a_share_quant.core.exceptions import DataSourceError
from a_share_quant.domain.models import Bar, DataImportRun, DataLineage, Security, TradingCalendarEntry
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.market_repository import MarketRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LoadedMarketData:
    """主链读取到的市场数据。"""

    bars_by_symbol: dict[str, list[Bar]] = field(default_factory=dict)
    securities: dict[str, Security] = field(default_factory=dict)
    trade_calendar: list[TradingCalendarEntry] = field(default_factory=list)
    data_lineage: DataLineage = field(default_factory=DataLineage)


class DataService:
    """负责数据导入、在线同步、读取与谱系计算。"""

    def __init__(
        self,
        market_repository: MarketRepository,
        data_config: DataSection,
        data_import_repository: DataImportRepository | None = None,
    ) -> None:
        self.market_repository = market_repository
        self.data_config = data_config
        self.data_import_repository = data_import_repository
        self.last_import_run_id: str | None = None
        Path(self.data_config.storage_dir).mkdir(parents=True, exist_ok=True)
        self.csv_adapter = CSVDataAdapter()
        self.providers: dict[str, MarketDataProvider] = {
            "tushare": TushareDataAdapter(
                token=data_config.tushare_token,
                token_env=data_config.tushare_token_env,
                adj_type=data_config.adj_type,
                timeout_seconds=data_config.request_timeout_seconds,
            ),
            "akshare": AKShareDataAdapter(
                adj_type=data_config.adj_type,
                timeout_seconds=data_config.request_timeout_seconds,
            ),
        }

    def import_csv(self, csv_path: str | Path, encoding: str = "utf-8") -> MarketDataBundle:
        """导入 CSV 到持久层。"""
        resolved_csv = Path(csv_path).resolve()
        bundle = self.csv_adapter.load(resolved_csv, encoding=encoding)
        self._persist(
            bundle,
            source="csv",
            request_context={"csv_path": str(resolved_csv), "encoding": encoding},
        )
        return bundle

    def sync_from_provider(
        self,
        provider_name: str,
        start_date: str,
        end_date: str,
        ts_codes: list[str] | None = None,
        exchange: str | None = None,
    ) -> MarketDataBundle:
        """从在线数据源拉取并写入持久层。"""
        provider_key = provider_name.lower()
        if provider_key not in self.providers:
            raise DataSourceError(f"不支持的数据源 provider: {provider_name}")
        provider = self.providers[provider_key]
        selected_codes = ts_codes
        if selected_codes is not None and self.data_config.max_symbols_per_run is not None:
            selected_codes = selected_codes[: self.data_config.max_symbols_per_run]
        bundle = provider.fetch_bundle(
            start_date=start_date,
            end_date=end_date,
            ts_codes=selected_codes,
            exchange=exchange or self.data_config.default_exchange,
        )
        self._log_bundle_degradation(provider_key, bundle)
        if bundle.degradation_flags and (self.data_config.fail_on_degraded_data or not self.data_config.allow_degraded_data):
            raise DataSourceError(
                f"数据源 {provider_name} 发生降级，配置禁止继续写入；degradation_flags={bundle.degradation_flags}"
            )
        self._persist(
            bundle,
            source=provider_key,
            request_context={
                "provider": provider_key,
                "start_date": start_date,
                "end_date": end_date,
                "ts_codes": selected_codes or [],
                "exchange": exchange or self.data_config.default_exchange,
            },
        )
        return bundle

    def _persist(self, bundle: MarketDataBundle, *, source: str, request_context: dict) -> None:
        """以显式事务落库市场数据，并补写导入审计。"""
        import_run_id: str | None = None
        self.last_import_run_id = None
        if self.data_import_repository is not None:
            import_run_id = self.data_import_repository.create_run(source=source, request_context=request_context)
            self.last_import_run_id = import_run_id
        try:
            with self.market_repository.store.transaction():
                self.market_repository.upsert_securities(bundle.securities)
                self.market_repository.upsert_calendar(bundle.calendar)
                self.market_repository.upsert_bars(bundle.bars)
                if import_run_id is not None:
                    self._write_quality_events(import_run_id, bundle)
            if import_run_id is not None:
                self.data_import_repository.finish_run(
                    import_run_id,
                    status="COMPLETED",
                    securities_count=len(bundle.securities),
                    calendar_count=len(bundle.calendar),
                    bars_count=len(bundle.bars),
                    degradation_flags=bundle.degradation_flags,
                    warnings=bundle.warnings,
                )
        except Exception as exc:
            if import_run_id is not None:
                self.data_import_repository.write_quality_event(
                    import_run_id,
                    event_type="import_failed",
                    payload={
                        "error": str(exc),
                        "source": source,
                        "requested_symbols": request_context.get("ts_codes", []),
                    },
                    level="ERROR",
                )
                self.data_import_repository.finish_run(
                    import_run_id,
                    status="FAILED",
                    securities_count=len(bundle.securities),
                    calendar_count=len(bundle.calendar),
                    bars_count=len(bundle.bars),
                    degradation_flags=bundle.degradation_flags,
                    warnings=bundle.warnings,
                    error_message=str(exc),
                )
            raise

    def _write_quality_events(self, import_run_id: str, bundle: MarketDataBundle) -> None:
        if self.data_import_repository is None:
            return
        self.data_import_repository.write_quality_event(
            import_run_id,
            event_type="row_count_summary",
            payload={
                "securities_count": len(bundle.securities),
                "calendar_count": len(bundle.calendar),
                "bars_count": len(bundle.bars),
            },
            level="INFO",
        )
        for flag in bundle.degradation_flags:
            self.data_import_repository.write_quality_event(
                import_run_id,
                event_type="degradation_flag",
                payload={"flag": flag},
                level="WARNING",
            )
        for warning in bundle.warnings:
            self.data_import_repository.write_quality_event(
                import_run_id,
                event_type="provider_warning",
                payload={"warning": warning},
                level="WARNING",
            )

    def load_market_data(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> tuple[dict[str, list[Bar]], dict[str, Security]]:
        """兼容旧接口：仅返回行情与证券元信息。"""
        bundle = self.load_market_data_bundle(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
        )
        return bundle.bars_by_symbol, bundle.securities

    def load_market_data_bundle(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> LoadedMarketData:
        """读取行情、证券元数据、交易日历与数据谱系。"""
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only)
        bars_by_symbol = self.market_repository.load_bars_grouped(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        calendar = self.market_repository.load_calendar(exchange=self.data_config.default_exchange)
        if start_date is not None or end_date is not None:
            calendar = [
                item
                for item in calendar
                if (start_date is None or item.cal_date >= start_date) and (end_date is None or item.cal_date <= end_date)
            ]
        lineage = self.build_data_lineage(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=calendar,
            explicit_start_date=start_date,
            explicit_end_date=end_date,
        )
        return LoadedMarketData(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=calendar,
            data_lineage=lineage,
        )

    def stream_market_data(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> tuple[Iterator[tuple[date, dict[str, Bar]]], dict[str, Security], list[date]]:
        """按交易日流式返回行情主链。"""
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only)
        trade_dates = self.market_repository.load_trade_dates(
            start_date=start_date,
            end_date=end_date,
            exchange=self.data_config.default_exchange,
            open_only=True,
        )
        return self.market_repository.iter_day_bars(trade_dates, ts_codes=ts_codes), securities, trade_dates

    def build_data_lineage(
        self,
        *,
        bars_by_symbol: dict[str, list[Bar]],
        securities: dict[str, Security],
        trade_calendar: list[TradingCalendarEntry],
        explicit_start_date: date | None = None,
        explicit_end_date: date | None = None,
    ) -> DataLineage:
        """根据当前加载到内存的数据构造可持久化谱系摘要。

        Args:
            bars_by_symbol: 当前运行实际使用的行情。
            securities: 当前运行实际使用的证券池。
            trade_calendar: 当前运行实际使用的交易日历。
            explicit_start_date: 调用方显式限定的开始日期。
            explicit_end_date: 调用方显式限定的结束日期。

        Returns:
            ``DataLineage``，包含导入批次引用、使用时间窗与数据摘要哈希。

        Boundary Behavior:
            - 若当前进程内没有发生导入，则会回退到最近一次 ``COMPLETED`` 导入运行作为参考来源；
            - 失败导入不会被挂接到后续成功回测的谱系上；
            - 即便找不到任何导入运行，也会基于已加载数据生成 ``dataset_digest``，避免报告无谱系。
        """
        import_run = self._resolve_reference_import_run()
        all_dates = sorted({bar.trade_date for bars in bars_by_symbol.values() for bar in bars})
        data_start_date = explicit_start_date or (all_dates[0] if all_dates else None)
        data_end_date = explicit_end_date or (all_dates[-1] if all_dates else None)
        digest_payload = {
            "bars": [
                {
                    "ts_code": bar.ts_code,
                    "trade_date": bar.trade_date.isoformat(),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "amount": bar.amount,
                    "adj_type": bar.adj_type,
                }
                for ts_code in sorted(bars_by_symbol)
                for bar in bars_by_symbol[ts_code]
            ],
            "securities": [
                {
                    "ts_code": sec.ts_code,
                    "exchange": sec.exchange,
                    "board": sec.board,
                    "status": sec.status,
                    "list_date": sec.list_date.isoformat() if sec.list_date else None,
                    "delist_date": sec.delist_date.isoformat() if sec.delist_date else None,
                }
                for sec in sorted(securities.values(), key=lambda item: item.ts_code)
            ],
            "trade_calendar": [
                {
                    "exchange": item.exchange,
                    "cal_date": item.cal_date.isoformat(),
                    "is_open": item.is_open,
                    "pretrade_date": item.pretrade_date.isoformat() if item.pretrade_date else None,
                }
                for item in trade_calendar
            ],
        }
        dataset_digest = hashlib.sha256(json.dumps(digest_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        degradation_flags: list[str] = []
        warnings: list[str] = []
        data_source = "database_snapshot"
        import_run_id: str | None = None
        if import_run is not None:
            import_run_id = import_run.import_run_id
            data_source = import_run.source
            degradation_flags = json.loads(import_run.degradation_flags_json)
            warnings = json.loads(import_run.warnings_json)
        return DataLineage(
            import_run_id=import_run_id,
            data_source=data_source,
            data_start_date=data_start_date.isoformat() if data_start_date else None,
            data_end_date=data_end_date.isoformat() if data_end_date else None,
            dataset_digest=dataset_digest,
            degradation_flags=degradation_flags,
            warnings=warnings,
        )

    def load_latest_data_lineage(self) -> DataLineage:
        """基于数据库当前快照生成谱系摘要。"""
        bundle = self.load_market_data_bundle()
        return bundle.data_lineage

    def _resolve_reference_import_run(self) -> DataImportRun | None:
        """解析当前回测应挂接的参考导入运行。

        Returns:
            当前进程最近一次成功导入；若当前进程没有成功导入，则回退到数据库中最近一次
            ``COMPLETED`` 的导入运行。

        Boundary Behavior:
            - ``FAILED`` / ``RUNNING`` 导入不会被当成可追溯谱系来源；
            - 若数据库中不存在任何成功导入，返回 ``None``，由调用方继续基于快照计算摘要。
        """
        if self.data_import_repository is None:
            return None
        if self.last_import_run_id:
            run = self.data_import_repository.get_run(self.last_import_run_id)
            if run is not None and run.status == "COMPLETED":
                return run
        return self.data_import_repository.get_latest_completed_run()

    @staticmethod
    def _log_bundle_degradation(provider_name: str, bundle: MarketDataBundle) -> None:
        if bundle.degradation_flags:
            logger.warning(
                "数据源发生降级 provider=%s degradation_flags=%s warnings=%s",
                provider_name,
                bundle.degradation_flags,
                bundle.warnings,
            )
