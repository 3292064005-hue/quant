"""应用上下文。"""
from __future__ import annotations

from dataclasses import dataclass

from a_share_quant.config.models import AppConfig
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.storage.sqlite_store import SQLiteStore


@dataclass(slots=True)
class AppContext:
    """应用依赖容器。

    Attributes:
        store: 当前应用实例持有的 SQLite 持久层连接。

    Notes:
        本对象提供显式 ``close()``，用于确保脚本与测试在异常或提前退出时
        仍然能够正确释放数据库连接。
    """

    config: AppConfig
    data_service: DataService
    strategy_service: StrategyService
    backtest_service: BacktestService
    market_repository: MarketRepository
    order_repository: OrderRepository
    account_repository: AccountRepository
    audit_repository: AuditRepository
    strategy_repository: StrategyRepository
    backtest_run_repository: BacktestRunRepository
    store: SQLiteStore

    def close(self) -> None:
        """关闭底层资源。

        Raises:
            RuntimeError: 当底层存储关闭失败时向上抛出。

        Boundary Behavior:
            重复调用是幂等的；若连接已关闭，则不再重复关闭。
        """
        self.store.close()

    def __enter__(self) -> AppContext:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
