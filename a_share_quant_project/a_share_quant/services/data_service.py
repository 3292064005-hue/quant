"""数据服务。"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from a_share_quant.adapters.data.akshare_adapter import AKShareDataAdapter
from a_share_quant.adapters.data.base import MarketDataBundle, MarketDataProvider
from a_share_quant.adapters.data.csv_adapter import CSVDataAdapter
from a_share_quant.adapters.data.tushare_adapter import TushareDataAdapter
from a_share_quant.config.models import DataSection
from a_share_quant.core.exceptions import DataSourceError
from a_share_quant.repositories.market_repository import MarketRepository


class DataService:
    """负责数据导入、在线同步与读取。"""

    def __init__(self, market_repository: MarketRepository, data_config: DataSection) -> None:
        self.market_repository = market_repository
        self.data_config = data_config
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
        bundle = self.csv_adapter.load(csv_path, encoding=encoding)
        self._persist(bundle)
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
        self._persist(bundle)
        return bundle

    def _persist(self, bundle: MarketDataBundle) -> None:
        self.market_repository.upsert_securities(bundle.securities)
        self.market_repository.upsert_calendar(bundle.calendar)
        self.market_repository.upsert_bars(bundle.bars)

    def load_market_data(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> tuple[dict[str, list], dict[str, object]]:
        """读取行情和证券元信息。

        Args:
            start_date: 起始交易日。
            end_date: 结束交易日。
            ts_codes: 可选证券集合。
            as_of_date: 证券池历史过滤日期。
            active_only: 是否仅返回 `as_of_date` 当日有效证券。
        """
        return (
            self.market_repository.load_bars_grouped(start_date=start_date, end_date=end_date, ts_codes=ts_codes),
            self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=as_of_date, active_only=active_only),
        )
