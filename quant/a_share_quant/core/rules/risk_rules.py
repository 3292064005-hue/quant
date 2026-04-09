"""风控规则。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.domain.models import OrderRequest, RiskResult, Security


@dataclass(slots=True)
class RiskContext:
    """风控上下文。"""

    order_value: float
    security: Security
    projected_weight: float
    blocked_symbols: set[str]
    kill_switch: bool
    can_trade: bool
    violates_price_limit: bool


class RiskRule(Protocol):
    """风控规则协议。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        ...


class KillSwitchRule:
    """熔断规则。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        return RiskResult(not context.kill_switch, "KillSwitchRule", "ERROR", "kill switch 已开启" if context.kill_switch else "通过")


class BlockedSecurityRule:
    """黑名单规则。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        passed = order.ts_code not in context.blocked_symbols
        return RiskResult(passed, "BlockedSecurityRule", "ERROR", f"{order.ts_code} 在黑名单中" if not passed else "通过")


class STBlockRule:
    """ST 证券买入限制。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        passed = not (context.security.is_st and order.side.value == "BUY")
        return RiskResult(passed, "STBlockRule", "ERROR", f"{order.ts_code} 为 ST 证券，禁止买入" if not passed else "通过")


class TradingAvailabilityRule:
    """停牌与交易可用性规则。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        return RiskResult(context.can_trade, "TradingAvailabilityRule", "ERROR", "停牌或不可交易" if not context.can_trade else "通过")


class LotSizeRule:
    """最小交易单位规则。"""

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        lot_size = MarketRules.get_lot_size(context.security)
        passed = order.quantity > 0 and order.quantity % lot_size == 0
        return RiskResult(
            passed,
            "LotSizeRule",
            "ERROR",
            f"订单数量 {order.quantity} 不符合最小交易单位 {lot_size}" if not passed else "通过",
        )


class PriceLimitRule:
    """涨跌停限制规则。"""

    def __init__(self, block_limit_up_buy: bool = True, block_limit_down_sell: bool = True) -> None:
        self.block_limit_up_buy = block_limit_up_buy
        self.block_limit_down_sell = block_limit_down_sell

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        if order.side.value == "BUY" and not self.block_limit_up_buy:
            return RiskResult(True, "PriceLimitRule", "INFO", "未启用涨停买入限制")
        if order.side.value == "SELL" and not self.block_limit_down_sell:
            return RiskResult(True, "PriceLimitRule", "INFO", "未启用跌停卖出限制")
        return RiskResult(not context.violates_price_limit, "PriceLimitRule", "ERROR", "触发涨跌停限制" if context.violates_price_limit else "通过")


class MaxOrderValueRule:
    """最大单笔订单金额规则。"""

    def __init__(self, max_order_value: float) -> None:
        self.max_order_value = max_order_value

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        passed = context.order_value <= self.max_order_value
        return RiskResult(passed, "MaxOrderValueRule", "ERROR", f"订单金额 {context.order_value:.2f} 超出上限 {self.max_order_value:.2f}" if not passed else "通过")


class MaxPositionWeightRule:
    """单票仓位上限规则。"""

    def __init__(self, max_position_weight: float) -> None:
        self.max_position_weight = max_position_weight

    def evaluate(self, order: OrderRequest, context: RiskContext) -> RiskResult:
        passed = context.projected_weight <= self.max_position_weight + 1e-9
        return RiskResult(passed, "MaxPositionWeightRule", "ERROR", f"预估仓位 {context.projected_weight:.4f} 超出上限 {self.max_position_weight:.4f}" if not passed else "通过")
