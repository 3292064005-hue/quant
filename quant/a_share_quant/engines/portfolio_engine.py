"""组合引擎。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import AccountSnapshot, Bar, OrderRequest, OrderSide, PositionSnapshot, Security, TargetPosition


@dataclass(slots=True)
class PortfolioContext:
    """组合生成上下文。"""

    strategy_id: str
    trade_date: date
    account: AccountSnapshot
    positions: dict[str, PositionSnapshot]
    bars: dict[str, Bar]
    securities: dict[str, Security]


class PortfolioEngine:
    """将目标权重转换为订单。"""

    def __init__(self, enforce_lot_size: bool = True, rebalance_mode: str = "close") -> None:
        self.enforce_lot_size = enforce_lot_size
        self.rebalance_mode = rebalance_mode
        if self.rebalance_mode != "close":
            raise ValueError(f"当前仅支持 close 调仓模式，实际收到: {self.rebalance_mode}")

    def generate_orders(self, targets: list[TargetPosition], context: PortfolioContext) -> list[OrderRequest]:
        """根据目标仓位生成调仓订单。

        Args:
            targets: 目标仓位列表。
            context: 账户、持仓、行情与证券元数据上下文。

        Returns:
            订单列表。无可执行差额时返回空列表。

        Boundary Behavior:
            - 当 `enforce_lot_size=True` 时，数量按板块最小交易单位向下取整。
            - 若卖出为清仓，允许最后不足一手的残余股一次性卖出。
            - 为便于现金回笼优先覆盖买单，返回结果始终按“先卖后买”排序。
        """
        sell_orders: list[OrderRequest] = []
        buy_orders: list[OrderRequest] = []
        total_assets = context.account.total_assets
        target_map = {item.ts_code: item for item in targets}
        symbols = sorted(set(target_map) | set(context.positions))
        for ts_code in symbols:
            bar = context.bars.get(ts_code)
            security = context.securities.get(ts_code)
            if bar is None or bar.close <= 0:
                continue
            current = context.positions.get(ts_code)
            current_qty = current.quantity if current else 0
            target = target_map.get(ts_code)
            target_weight = target.target_weight if target is not None else 0.0
            target_value = total_assets * target_weight
            raw_target_qty = int(target_value / bar.close)
            if self.enforce_lot_size:
                target_qty = MarketRules.normalize_order_quantity(raw_target_qty, security)
            else:
                target_qty = max(raw_target_qty, 0)
            delta_qty = target_qty - current_qty
            if delta_qty == 0:
                continue
            side = OrderSide.BUY if delta_qty > 0 else OrderSide.SELL
            qty = abs(delta_qty)
            if self.enforce_lot_size:
                if side == OrderSide.BUY:
                    qty = MarketRules.normalize_order_quantity(qty, security)
                else:
                    qty = MarketRules.normalize_sell_quantity(qty, security, current_qty)
            if qty <= 0:
                continue
            reason = target.reason if target is not None else "目标仓位为 0，执行清仓"
            order = OrderRequest(
                order_id=new_id("order"),
                trade_date=context.trade_date,
                strategy_id=context.strategy_id,
                ts_code=ts_code,
                side=side,
                price=bar.close,
                quantity=qty,
                reason=reason,
            )
            if side == OrderSide.SELL:
                sell_orders.append(order)
            else:
                buy_orders.append(order)
        return sell_orders + buy_orders
