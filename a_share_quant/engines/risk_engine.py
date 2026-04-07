"""风控引擎。"""
from __future__ import annotations

from dataclasses import dataclass

from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.core.rules.risk_rules import RiskContext, RiskRule
from a_share_quant.domain.models import AccountSnapshot, Bar, OrderRequest, OrderSide, OrderStatus, PositionSnapshot, RiskResult, Security


@dataclass(slots=True)
class RiskEngine:
    """订单风控评估器。

    本引擎在逐单规则校验之外，还负责：
    - 组合级可用现金顺序预留；
    - 卖单可卖数量校验；
    - 风控拒绝结果的结构化审计输出。
    """

    rules: list[RiskRule]
    blocked_symbols: set[str]
    kill_switch: bool
    sequential_cash_reservation: bool
    fee_bps: float
    tax_bps: float

    def validate_orders(
        self,
        orders: list[OrderRequest],
        securities: dict[str, Security],
        bars: dict[str, Bar],
        positions: dict[str, PositionSnapshot],
        account: AccountSnapshot,
        target_weights: dict[str, float],
    ) -> tuple[list[OrderRequest], dict[str, list[RiskResult]]]:
        """过滤可执行订单并返回详细审计结果。

        Args:
            orders: 原始订单。
            securities: 证券字典。
            bars: 当日行情。
            positions: 当前持仓。
            account: 当前账户。
            target_weights: 目标权重映射。

        Returns:
            `(accepted_orders, audit_results)`。

        Raises:
            KeyError: 当订单证券在 `securities` 或 `bars` 中缺失时抛出。
        """
        accepted: list[OrderRequest] = []
        audit: dict[str, list[RiskResult]] = {}
        remaining_cash = account.available_cash
        available_positions = {code: snapshot.available_quantity for code, snapshot in positions.items()}
        for order in orders:
            security = securities[order.ts_code]
            bar = bars[order.ts_code]
            order_value = order.quantity * order.price
            projected_weight = target_weights.get(order.ts_code, 0.0)
            context = RiskContext(
                order_value=order_value,
                security=security,
                projected_weight=projected_weight,
                blocked_symbols=self.blocked_symbols,
                kill_switch=self.kill_switch,
                can_trade=MarketRules.can_trade(bar),
                violates_price_limit=MarketRules.violates_price_limit(bar, order.side),
            )
            results = [rule.evaluate(order, context) for rule in self.rules]
            if all(item.passed for item in results):
                inventory_result, remaining_cash, available_positions = self._evaluate_inventory_and_cash(order, remaining_cash, available_positions)
                results.append(inventory_result)
            audit[order.order_id] = results
            if all(item.passed for item in results):
                accepted.append(order)
            else:
                order.status = OrderStatus.PRE_TRADE_REJECTED
        return accepted, audit

    def _evaluate_inventory_and_cash(
        self,
        order: OrderRequest,
        remaining_cash: float,
        available_positions: dict[str, int],
    ) -> tuple[RiskResult, float, dict[str, int]]:
        if order.side == OrderSide.BUY:
            required_cash = self._estimate_buy_cash_requirement(order)
            if required_cash > remaining_cash + 1e-9:
                return (
                    RiskResult(False, "AvailableCashRule", "ERROR", f"可用资金不足，所需 {required_cash:.2f}，剩余 {remaining_cash:.2f}"),
                    remaining_cash,
                    available_positions,
                )
            updated_cash = remaining_cash - required_cash if self.sequential_cash_reservation else remaining_cash
            return RiskResult(True, "AvailableCashRule", "INFO", "通过"), updated_cash, available_positions
        available_quantity = available_positions.get(order.ts_code, 0)
        if order.quantity > available_quantity:
            return (
                RiskResult(False, "AvailableQuantityRule", "ERROR", f"可卖数量不足，尝试卖出 {order.quantity}，可卖 {available_quantity}"),
                remaining_cash,
                available_positions,
            )
        updated_positions = dict(available_positions)
        updated_positions[order.ts_code] = available_quantity - order.quantity
        updated_cash = remaining_cash
        if self.sequential_cash_reservation:
            updated_cash += self._estimate_sell_cash_release(order)
        return RiskResult(True, "AvailableQuantityRule", "INFO", "通过"), updated_cash, updated_positions

    def _estimate_buy_cash_requirement(self, order: OrderRequest) -> float:
        turnover = order.price * order.quantity
        fee = turnover * self.fee_bps / 10000.0
        return turnover + fee

    def _estimate_sell_cash_release(self, order: OrderRequest) -> float:
        turnover = order.price * order.quantity
        fee = turnover * self.fee_bps / 10000.0
        tax = turnover * self.tax_bps / 10000.0
        return turnover - fee - tax
