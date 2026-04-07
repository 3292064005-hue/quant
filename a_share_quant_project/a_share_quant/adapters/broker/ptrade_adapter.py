"""PTrade 适配器。"""
from __future__ import annotations

from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.timeout_utils import call_with_timeout
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, PositionSnapshot


class PTradeAdapter(BrokerBase):
    """对接 PTrade 客户端的边界适配器。"""

    def __init__(self, client: object, timeout_seconds: float | None = None) -> None:
        self._client = client
        self._timeout_seconds = timeout_seconds

    def connect(self) -> None:
        connect = getattr(self._client, "connect", None)
        if callable(connect):
            call_with_timeout(connect, timeout_seconds=self._timeout_seconds, operation_name="broker.connect")

    def get_account(self, last_prices: dict[str, float]) -> AccountSnapshot:
        return call_with_timeout(self._client.get_account, last_prices, timeout_seconds=self._timeout_seconds, operation_name="broker.get_account")

    def get_positions(self, last_prices: dict[str, float]) -> list[PositionSnapshot]:
        return call_with_timeout(self._client.get_positions, last_prices, timeout_seconds=self._timeout_seconds, operation_name="broker.get_positions")

    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        return call_with_timeout(self._client.submit_order, order, fill_price, trade_date, timeout_seconds=self._timeout_seconds, operation_name="broker.submit_order")

    def cancel_order(self, broker_order_id: str) -> None:
        call_with_timeout(self._client.cancel_order, broker_order_id, timeout_seconds=self._timeout_seconds, operation_name="broker.cancel_order")

    def query_orders(self) -> list[OrderRequest]:
        return call_with_timeout(self._client.query_orders, timeout_seconds=self._timeout_seconds, operation_name="broker.query_orders")

    def query_trades(self) -> list[Fill]:
        return call_with_timeout(self._client.query_trades, timeout_seconds=self._timeout_seconds, operation_name="broker.query_trades")

    def heartbeat(self) -> bool:
        return bool(call_with_timeout(self._client.heartbeat, timeout_seconds=self._timeout_seconds, operation_name="broker.heartbeat"))
