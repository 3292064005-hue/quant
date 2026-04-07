"""执行引擎。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.events import Event, EventBus
from a_share_quant.core.exceptions import OrderRejectedError
from a_share_quant.domain.models import Bar, Fill, OrderRequest, OrderSide, OrderStatus


@dataclass(slots=True)
class ExecutionOutcome:
    """执行结果。"""

    fills: list[Fill] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)


class ExecutionEngine:
    """将订单路由到券商适配器。"""

    def __init__(self, broker: BrokerBase, event_bus: EventBus | None = None, slippage_bps: float = 0.0) -> None:
        self.broker = broker
        self.event_bus = event_bus or EventBus()
        self.slippage_ratio = slippage_bps / 10000.0

    def execute(self, orders: list[OrderRequest], bars: dict[str, Bar], trade_date: date) -> ExecutionOutcome:
        """执行订单。

        Args:
            orders: 待执行订单列表。
            bars: 当日行情映射。
            trade_date: 当前交易日。

        Returns:
            `ExecutionOutcome`，其中包含成交回报与执行拒绝原因。

        Raises:
            None。执行层会将缺少行情与券商拒绝统一收敛为拒单结果，而不是中断整批执行。
        """
        outcome = ExecutionOutcome()
        for order in orders:
            bar = bars.get(order.ts_code)
            if bar is None:
                order.status = OrderStatus.EXECUTION_REJECTED
                outcome.rejected[order.order_id] = f"缺少行情数据: {order.ts_code}"
                self.event_bus.publish(Event("ORDER_REJECTED", {"order_id": order.order_id, "reason": outcome.rejected[order.order_id]}))
                continue
            fill_price = self._apply_slippage(bar.close, order.side)
            try:
                fill = self.broker.submit_order(order, fill_price, trade_date)
            except OrderRejectedError as exc:
                order.status = OrderStatus.EXECUTION_REJECTED
                outcome.rejected[order.order_id] = str(exc)
                self.event_bus.publish(Event("ORDER_REJECTED", {"order_id": order.order_id, "reason": str(exc)}))
                continue
            outcome.fills.append(fill)
            self.event_bus.publish(Event("ORDER_FILLED", {"order_id": order.order_id, "fill_id": fill.fill_id}))
        return outcome

    def _apply_slippage(self, reference_price: float, side: OrderSide) -> float:
        if reference_price <= 0:
            return reference_price
        if side == OrderSide.BUY:
            return reference_price * (1.0 + self.slippage_ratio)
        return reference_price * (1.0 - self.slippage_ratio)
