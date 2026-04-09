"""数据服务外观。"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

from a_share_quant.adapters.data.akshare_adapter import AKShareDataAdapter
from a_share_quant.adapters.data.base import MarketDataBundle, MarketDataProvider
from a_share_quant.adapters.data.csv_adapter import CSVDataAdapter
from a_share_quant.adapters.data.tushare_adapter import TushareDataAdapter
from a_share_quant.config.models import DataSection
from a_share_quant.core.exceptions import DataSourceError
from a_share_quant.domain.models import Bar, DataLineage, Security
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.dataset_version_repository import DatasetVersionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.services.data_import_persistence import DataImportPersistence
from a_share_quant.services.data_lineage_builder import DataLineageBuilder
from a_share_quant.services.data_market_reader import MarketDataReader, log_bundle_degradation
from a_share_quant.services.trading_session_service import TradingSessionService
from a_share_quant.services.data_service_types import LoadedMarketData, StreamingMarketData, StreamLineageTracker


class DataService:
    """负责数据导入、在线同步、读取与谱系计算。

    Notes:
        - ``DataService`` 作为对外 façade，保留既有 API；
        - 导入审计/落库由 ``DataImportPersistence`` 负责；
        - 谱系与 digest 计算由 ``DataLineageBuilder`` 负责；
        - preload/stream 读取与 bundle 组装由 ``MarketDataReader`` 负责。
    """

    def __init__(
        self,
        market_repository: MarketRepository,
        data_config: DataSection,
        data_import_repository: DataImportRepository | None = None,
        dataset_version_repository: DatasetVersionRepository | None = None,
    ) -> None:
        self.market_repository = market_repository
        self.data_config = data_config
        self.data_import_repository = data_import_repository
        self.dataset_version_repository = dataset_version_repository
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
        self.persistence = DataImportPersistence(market_repository, data_import_repository)
        self.lineage_builder = DataLineageBuilder(
            market_repository,
            data_config,
            data_import_repository=data_import_repository,
            dataset_version_repository=dataset_version_repository,
        )
        self.session_service = TradingSessionService(market_repository, data_config)
        self.reader = MarketDataReader(market_repository, self.lineage_builder, self.session_service)

    @property
    def last_import_run_id(self) -> str | None:
        return self.persistence.last_import_run_id

    def import_csv(self, csv_path: str | Path, encoding: str = "utf-8") -> MarketDataBundle:
        """导入 CSV 到持久层。"""
        resolved_csv = Path(csv_path).resolve()
        bundle = self.csv_adapter.load(resolved_csv, encoding=encoding)
        self.persistence.persist(
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
        log_bundle_degradation(provider_key, bundle)
        if bundle.degradation_flags and (self.data_config.fail_on_degraded_data or not self.data_config.allow_degraded_data):
            raise DataSourceError(
                f"数据源 {provider_name} 发生降级，配置禁止继续写入；degradation_flags={bundle.degradation_flags}"
            )
        self.persistence.persist(
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
        return self.reader.load_market_data_bundle(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
        )

    def prepare_stream_market_data(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> StreamingMarketData:
        """构建单遍流式可消费的数据主链，并把最终谱系延迟到消费完成后生成。"""
        return self.reader.prepare_stream_market_data(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
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
        return self.reader.stream_market_data(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
        )

    def build_data_lineage(
        self,
        *,
        bars_by_symbol: dict[str, list[Bar]],
        securities: dict[str, Security],
        trade_calendar,
        exchange_scope: list[str],
        explicit_start_date: date | None = None,
        explicit_end_date: date | None = None,
        requested_ts_codes: list[str] | None = None,
    ) -> DataLineage:
        """根据当前加载到内存的数据构造可持久化谱系摘要。"""
        return self.lineage_builder.build_data_lineage(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=trade_calendar,
            exchange_scope=exchange_scope,
            explicit_start_date=explicit_start_date,
            explicit_end_date=explicit_end_date,
            requested_ts_codes=requested_ts_codes,
        )

    def build_stream_data_lineage(
        self,
        *,
        securities: dict[str, Security],
        trade_dates: list[date],
        exchange_scope: list[str],
        explicit_start_date: date | None = None,
        explicit_end_date: date | None = None,
        requested_ts_codes: list[str] | None = None,
    ) -> DataLineage:
        """在不回退到全量 preload 的前提下生成流式模式谱系。"""
        return self.lineage_builder.build_stream_data_lineage(
            securities=securities,
            trade_dates=trade_dates,
            exchange_scope=exchange_scope,
            explicit_start_date=explicit_start_date,
            explicit_end_date=explicit_end_date,
            requested_ts_codes=requested_ts_codes,
        )

    def load_latest_data_lineage(self) -> DataLineage:
        """基于数据库当前快照生成谱系摘要。"""
        return self.lineage_builder.load_latest_data_lineage()


__all__ = [
    "DataService",
    "LoadedMarketData",
    "StreamLineageTracker",
    "StreamingMarketData",
]
