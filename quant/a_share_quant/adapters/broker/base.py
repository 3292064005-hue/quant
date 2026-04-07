"""券商/执行端口抽象。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, PositionSnapshot


class ConnectionPort(ABC):
    """共享的连接生命周期与查询能力。"""

    @abstractmethod
    def connect(self) -> None:
        """建立连接。"""

    @abstractmethod
    def close(self) -> None:
        """关闭连接并释放资源。"""

    @abstractmethod
    def heartbeat(self) -> bool:
        """心跳检测。"""


class SimulatedExecutionPort(ConnectionPort, ABC):
    """研究回测/回放执行端口。

    该端口只承载可由历史数据驱动的账户状态推进语义，供 ``BacktestEngine`` 使用。
    未来若继续扩展 paper/live，应避免把真实 broker 状态机直接塞回该端口。
    """

    @abstractmethod
    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """返回账户快照。"""

    @abstractmethod
    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """返回持仓快照。"""

    @abstractmethod
    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """提交订单并返回成交结果。"""

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """撤单。"""

    @abstractmethod
    def query_orders(self) -> list[OrderRequest]:
        """查询订单。"""

    @abstractmethod
    def query_trades(self) -> list[Fill]:
        """查询成交。"""


class LiveBrokerPort(ConnectionPort, ABC):
    """真实 broker 边界端口。

    该端口保留真实客户端的统一领域映射能力，用于 runtime 校验、paper/live orchestration
    以及未来的 operator workflow，但不应被研究回测引擎直接依赖。
    """

    @abstractmethod
    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """返回账户快照。"""

    @abstractmethod
    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """返回持仓快照。"""

    @abstractmethod
    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """提交订单并返回成交结果。"""

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """撤单。"""

    @abstractmethod
    def query_orders(self) -> list[OrderRequest]:
        """查询订单。"""

    @abstractmethod
    def query_trades(self) -> list[Fill]:
        """查询成交。"""


class BrokerBase(SimulatedExecutionPort, ABC):
    """兼容别名。

    历史代码广泛使用 ``BrokerBase``。当前将其固定为研究回测端口别名，
    以便在不破坏导入路径的前提下，把研究/真实 broker 语义逐步拆开。
    """


__all__ = [
    "ConnectionPort",
    "SimulatedExecutionPort",
    "LiveBrokerPort",
    "BrokerBase",
]
