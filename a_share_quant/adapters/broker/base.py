"""券商适配器基类。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, PositionSnapshot


class BrokerBase(ABC):
    """统一券商接口。"""

    @abstractmethod
    def connect(self) -> None:
        """建立连接。"""

    @abstractmethod
    def get_account(self, last_prices: dict[str, float]) -> AccountSnapshot:
        """返回账户快照。"""

    @abstractmethod
    def get_positions(self, last_prices: dict[str, float]) -> list[PositionSnapshot]:
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

    @abstractmethod
    def heartbeat(self) -> bool:
        """心跳检测。"""
