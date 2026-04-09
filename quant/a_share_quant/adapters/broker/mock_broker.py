"""可运行的本地模拟券商。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.exceptions import OrderRejectedError
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, OrderStatus, PositionSnapshot


@dataclass(slots=True)
class _Position:
    ts_code: str
    quantity: int = 0
    available_quantity: int = 0
    avg_cost: float = 0.0
    last_buy_date: date | None = None


class MockBroker(BrokerBase):
    """以撮合即成交模型模拟券商账户。

    Notes:
        MockBroker 仍支持在 ``get_account/get_positions`` 时按外部价格估值，
        但它不再维护任何依赖查询次数的 ``daily_pnl`` 状态。回测链路中的
        账户估值、EOD ``daily_pnl`` 与回撤均应由独立估值组件统一生成。
    """

    def __init__(self, initial_cash: float, fee_bps: float = 3.0, tax_bps: float = 10.0) -> None:
        self._initial_cash = initial_cash
        self._cash = initial_cash
        self._fee_bps = fee_bps / 10000.0
        self._tax_bps = tax_bps / 10000.0
        self._positions: dict[str, _Position] = {}
        self._orders: list[OrderRequest] = []
        self._fills: list[Fill] = []
        self._peak_assets = initial_cash
        self._connected = False
        self._closed = False

    def connect(self) -> None:
        self._closed = False
        self._connected = True

    def close(self) -> None:
        """关闭 mock broker。

        Boundary Behavior:
            重复关闭是幂等的；关闭后再次调用查询/下单接口会抛出 ``RuntimeError``。
        """
        self._closed = True
        self._connected = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("MockBroker 已关闭")
        if not self._connected:
            raise RuntimeError("MockBroker 尚未 connect")

    def _fee(self, turnover: float) -> float:
        return turnover * self._fee_bps

    def _tax(self, turnover: float, side: OrderSide) -> float:
        return turnover * self._tax_bps if side == OrderSide.SELL else 0.0

    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """返回账户快照。

        Args:
            last_prices: 最新估值价格映射。若缺失某持仓价格，则退回到持仓平均成本，
                避免缺报价时把持仓估值为 0。

        Returns:
            ``AccountSnapshot``。其中 ``pnl``/``cum_pnl`` 表示相对初始资金的累计盈亏；
            ``daily_pnl`` 恒为 ``0.0``，因为日度收益应由外部 EOD 估值链计算。
        """
        self._ensure_open()
        marks = last_prices or {}
        market_value = 0.0
        for code, position in self._positions.items():
            mark = marks.get(code, position.avg_cost)
            market_value += position.quantity * mark
        total_assets = self._cash + market_value
        self._peak_assets = max(self._peak_assets, total_assets)
        drawdown = 0.0 if self._peak_assets <= 0 else total_assets / self._peak_assets - 1.0
        cumulative_pnl = total_assets - self._initial_cash
        return AccountSnapshot(
            cash=self._cash,
            available_cash=self._cash,
            market_value=market_value,
            total_assets=total_assets,
            pnl=cumulative_pnl,
            cum_pnl=cumulative_pnl,
            daily_pnl=0.0,
            drawdown=drawdown,
        )

    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        self._ensure_open()
        marks = last_prices or {}
        snapshots: list[PositionSnapshot] = []
        for code, position in sorted(self._positions.items()):
            last_price = marks.get(code, position.avg_cost)
            market_value = position.quantity * last_price
            unrealized = (last_price - position.avg_cost) * position.quantity
            snapshots.append(
                PositionSnapshot(
                    ts_code=code,
                    quantity=position.quantity,
                    available_quantity=position.available_quantity,
                    avg_cost=position.avg_cost,
                    market_value=market_value,
                    unrealized_pnl=unrealized,
                )
            )
        return snapshots

    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """按传入价格即时成交。"""
        self._ensure_open()
        if order.quantity <= 0:
            order.status = OrderStatus.EXECUTION_REJECTED
            raise OrderRejectedError("订单数量必须大于 0")
        turnover = fill_price * order.quantity
        fee = self._fee(turnover)
        tax = self._tax(turnover, order.side)
        position = self._positions.setdefault(order.ts_code, _Position(ts_code=order.ts_code))
        order.status = OrderStatus.SUBMITTED
        if order.side == OrderSide.BUY:
            total_cost = turnover + fee + tax
            if total_cost > self._cash + 1e-9:
                order.status = OrderStatus.EXECUTION_REJECTED
                raise OrderRejectedError("可用资金不足")
            new_qty = position.quantity + order.quantity
            position.avg_cost = (position.avg_cost * position.quantity + turnover) / new_qty if new_qty > 0 else 0.0
            position.quantity = new_qty
            position.available_quantity += order.quantity
            position.last_buy_date = trade_date
            self._cash -= total_cost
        else:
            if order.quantity > position.available_quantity:
                order.status = OrderStatus.EXECUTION_REJECTED
                raise OrderRejectedError("可卖数量不足")
            position.quantity -= order.quantity
            position.available_quantity -= order.quantity
            self._cash += turnover - fee - tax
            if position.quantity == 0:
                position.avg_cost = 0.0
        order.status = OrderStatus.FILLED
        self._orders.append(order)
        fill = Fill(
            fill_id=new_id("fill"),
            order_id=order.order_id,
            trade_date=trade_date,
            ts_code=order.ts_code,
            side=order.side,
            fill_price=fill_price,
            fill_quantity=order.quantity,
            fee=fee,
            tax=tax,
            run_id=order.run_id,
            broker_order_id=order.order_id,
        )
        self._fills.append(fill)
        return fill

    def cancel_order(self, broker_order_id: str) -> None:
        self._ensure_open()
        for order in self._orders:
            if order.order_id == broker_order_id and order.status == OrderStatus.CREATED:
                order.status = OrderStatus.CANCELLED
                return
        raise OrderRejectedError(f"无法撤单，订单不存在或已成交: {broker_order_id}")

    def query_orders(self) -> list[OrderRequest]:
        self._ensure_open()
        return list(self._orders)

    def query_trades(self) -> list[Fill]:
        self._ensure_open()
        return list(self._fills)

    def heartbeat(self) -> bool:
        return self._connected and not self._closed
