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
    """按收盘价即时成交的本地模拟券商。"""

    def __init__(self, initial_cash: float, fee_bps: float, tax_bps: float) -> None:
        self._initial_cash = initial_cash
        self._cash = initial_cash
        self._fee_bps = fee_bps / 10000.0
        self._tax_bps = tax_bps / 10000.0
        self._positions: dict[str, _Position] = {}
        self._orders: list[OrderRequest] = []
        self._fills: list[Fill] = []
        self._peak_assets = initial_cash
        self._last_total_assets: float | None = None

    def connect(self) -> None:
        return None

    def _fee(self, turnover: float) -> float:
        return turnover * self._fee_bps

    def _tax(self, turnover: float, side: OrderSide) -> float:
        return turnover * self._tax_bps if side == OrderSide.SELL else 0.0

    def get_account(self, last_prices: dict[str, float]) -> AccountSnapshot:
        """返回账户快照。

        Args:
            last_prices: 最新估值价格映射。

        Returns:
            `AccountSnapshot`，其中：
            - `pnl` 与 `cum_pnl` 均表示相对初始资金的累计盈亏；
            - `daily_pnl` 表示相对上一次账户查询时点的资产变化；
            - `drawdown` 表示相对峰值资产的回撤。

        Boundary Behavior:
            第一次查询时 `daily_pnl` 记为 0.0。
        """
        market_value = 0.0
        for code, position in self._positions.items():
            market_value += position.quantity * last_prices.get(code, 0.0)
        total_assets = self._cash + market_value
        previous_total_assets = self._last_total_assets if self._last_total_assets is not None else total_assets
        daily_pnl = total_assets - previous_total_assets
        self._peak_assets = max(self._peak_assets, total_assets)
        drawdown = 0.0 if self._peak_assets <= 0 else total_assets / self._peak_assets - 1.0
        self._last_total_assets = total_assets
        cumulative_pnl = total_assets - self._initial_cash
        return AccountSnapshot(
            cash=self._cash,
            available_cash=self._cash,
            market_value=market_value,
            total_assets=total_assets,
            pnl=cumulative_pnl,
            cum_pnl=cumulative_pnl,
            daily_pnl=daily_pnl,
            drawdown=drawdown,
        )

    def get_positions(self, last_prices: dict[str, float]) -> list[PositionSnapshot]:
        snapshots: list[PositionSnapshot] = []
        for code, position in sorted(self._positions.items()):
            last_price = last_prices.get(code, 0.0)
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
        """按传入价格即时成交。

        Args:
            order: 订单请求。
            fill_price: 成交价格。
            trade_date: 交易日期。

        Returns:
            成交回报。

        Raises:
            OrderRejectedError: 当资金不足、可卖数量不足或数量非法时抛出。
        """
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
        )
        self._fills.append(fill)
        return fill

    def cancel_order(self, broker_order_id: str) -> None:
        for order in self._orders:
            if order.order_id == broker_order_id and order.status == OrderStatus.CREATED:
                order.status = OrderStatus.CANCELLED
                return
        raise OrderRejectedError(f"无法撤单，订单不存在或已成交: {broker_order_id}")

    def query_orders(self) -> list[OrderRequest]:
        return list(self._orders)

    def query_trades(self) -> list[Fill]:
        return list(self._fills)

    def heartbeat(self) -> bool:
        return True
