"""应用上下文。"""
from __future__ import annotations

from dataclasses import dataclass

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.storage.sqlite_store import SQLiteStore


@dataclass(slots=True)
class AppContext:
    """应用依赖容器。

    当前上下文允许按命令最小依赖装配：
        - 存储命令：只注入 store/repositories
        - 数据命令：额外注入 data_service
        - 报表命令：额外注入 report_service
        - 回测命令：额外注入 broker/strategy_service/backtest_service
    """

    config: AppConfig
    market_repository: MarketRepository
    order_repository: OrderRepository
    account_repository: AccountRepository
    audit_repository: AuditRepository
    strategy_repository: StrategyRepository
    backtest_run_repository: BacktestRunRepository
    data_import_repository: DataImportRepository
    store: SQLiteStore
    broker: BrokerBase | LiveBrokerPort | None = None
    data_service: DataService | None = None
    strategy_service: StrategyService | None = None
    backtest_service: BacktestService | None = None
    report_service: ReportService | None = None

    def require_broker(self) -> BrokerBase | LiveBrokerPort:
        if self.broker is None:
            raise RuntimeError("当前上下文未注入 broker；请使用完整回测/交易装配入口")
        return self.broker

    def require_data_service(self) -> DataService:
        if self.data_service is None:
            raise RuntimeError("当前上下文未注入 DataService；请使用数据装配入口")
        return self.data_service

    def require_strategy_service(self) -> StrategyService:
        if self.strategy_service is None:
            raise RuntimeError("当前上下文未注入 StrategyService；请使用回测装配入口")
        return self.strategy_service

    def require_backtest_service(self) -> BacktestService:
        if self.backtest_service is None:
            raise RuntimeError("当前上下文未注入 BacktestService；请使用回测装配入口")
        return self.backtest_service

    def require_report_service(self) -> ReportService:
        if self.report_service is None:
            raise RuntimeError("当前上下文未注入 ReportService；请使用报表装配入口")
        return self.report_service

    def close(self) -> None:
        """关闭底层资源。

        Boundary Behavior:
            - 重复调用是幂等的。
            - 若 broker 未注入，则仅关闭数据库。
            - 优先关闭 broker，再关闭数据库，避免持有已关闭连接的资源继续回调。
        """
        broker_error: Exception | None = None
        if self.broker is not None:
            try:
                self.broker.close()
            except Exception as exc:  # pragma: no cover - 防御性保护
                broker_error = exc
        self.store.close()
        if broker_error is not None:
            raise broker_error

    def __enter__(self) -> AppContext:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
