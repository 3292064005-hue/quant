"""research/backtest 与 paper/live 共享的执行合同服务。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

from a_share_quant.domain.models import AccountSnapshot, Bar, OrderRequest, OrderSide, PositionSnapshot, RiskResult, Security, TargetPosition


@dataclass(slots=True)
class BasicOrderValidationOutcome:
    """基础订单输入校验结果。

    Attributes:
        candidate_orders: 通过基础输入校验、可继续进入 RiskEngine 的订单。
        rejected_orders: 在进入 RiskEngine 前已被拒绝的订单。
        audit_results: 每笔订单对应的基础风险审计结果。
        reasons_by_symbol: 按证券聚合的拒绝原因，用于 session summary 与审计落库。
    """

    candidate_orders: list[OrderRequest]
    rejected_orders: list[OrderRequest]
    audit_results: dict[str, list[RiskResult]]
    reasons_by_symbol: dict[str, list[str]]


class SharedExecutionContractService:
    """共享 research/backtest 与 paper/live 的执行合同解析与输入约束。

    该服务不试图把两条主编排链粗暴并成同一个 orchestrator，而是显式收口两类会漂移的合同：

    1. 策略执行合同：required_history_bars / should_rebalance / generate_targets
    2. 订单输入合同：基础 pre-trade 输入校验 / projected target weights

    Boundary Behavior:
        - 回测与 operator 都必须经过同一份合同解析逻辑，禁止各自产生轻微分叉；
        - 对 strategy 的 runtime 绑定采用“显式优先、旧接口回退”的兼容策略；
        - 对 operator 订单只做进入 RiskEngine 前的基础输入校验，不替代正式 RiskEngine；
        - 返回值全部为纯 Python 数据，不持有 repository / broker 资源，便于测试与跨主链复用。
    """

    def required_history_bars(self, strategy) -> int:
        """解析策略所需的最小历史窗口。"""
        execution_runtime = getattr(strategy, "_execution_runtime", None)
        if execution_runtime is not None and hasattr(execution_runtime, "required_history_bars"):
            return max(int(execution_runtime.required_history_bars(strategy)), 1)
        if hasattr(strategy, "required_history_bars"):
            return max(int(strategy.required_history_bars()), 1)
        return 1

    def should_rebalance(self, strategy, *, eligible_trade_index: int) -> bool:
        """解析当前交易日是否应触发调仓。"""
        execution_runtime = getattr(strategy, "_execution_runtime", None)
        if execution_runtime is not None and hasattr(execution_runtime, "should_rebalance"):
            return bool(execution_runtime.should_rebalance(strategy, eligible_trade_index))
        if hasattr(strategy, "should_rebalance"):
            return bool(strategy.should_rebalance(eligible_trade_index))
        return True

    def generate_targets(self, strategy, frame) -> list[TargetPosition]:
        """生成正式目标仓位。

        Args:
            strategy: 策略对象。
            frame: trade day frame；至少需要 ``trade_date`` / ``active_history`` / ``active_securities``。

        Returns:
            目标仓位列表。

        Raises:
            AttributeError: 当 strategy/runtime 缺少正式目标生成合同。
        """
        execution_runtime = getattr(strategy, "_execution_runtime", None)
        if execution_runtime is not None and hasattr(execution_runtime, "generate_targets"):
            return list(execution_runtime.generate_targets(strategy, frame))
        if hasattr(strategy, "generate_targets"):
            return list(strategy.generate_targets(frame.active_history, frame.trade_date, frame.active_securities))
        raise AttributeError(f"策略 {type(strategy).__name__} 缺少 generate_targets 合同")

    def validate_basic_order_inputs(
        self,
        orders: list[OrderRequest],
        *,
        trade_date: date,
        securities: dict[str, Security],
        bars: dict[str, Bar],
    ) -> BasicOrderValidationOutcome:
        """执行进入 RiskEngine 前的基础订单输入校验。"""
        candidate_orders: list[OrderRequest] = []
        rejected_orders: list[OrderRequest] = []
        reasons_by_symbol: dict[str, list[str]] = defaultdict(list)
        audit_results: dict[str, list[RiskResult]] = {}
        for order in orders:
            preflight_failure = self._preflight_validate_order(order, trade_date=trade_date, securities=securities, bars=bars)
            if preflight_failure is None:
                candidate_orders.append(order)
                continue
            audit_results[order.order_id] = [preflight_failure]
            rejection_reason = preflight_failure.reason
            rejected_orders.append(order)
            reasons_by_symbol[order.ts_code].append(rejection_reason)
        return BasicOrderValidationOutcome(
            candidate_orders=candidate_orders,
            rejected_orders=rejected_orders,
            audit_results=audit_results,
            reasons_by_symbol=dict(reasons_by_symbol),
        )

    def build_projected_target_weights(
        self,
        orders: list[OrderRequest],
        *,
        positions: dict[str, PositionSnapshot],
        account: AccountSnapshot,
    ) -> dict[str, float]:
        """按顺序估算订单批次执行后的目标权重。"""
        total_assets = float(account.total_assets)
        if total_assets <= 0:
            total_assets = max(
                float(account.available_cash) + sum(max(float(item.market_value), 0.0) for item in positions.values()),
                1.0,
            )
        projected_values = {
            code: max(float(snapshot.market_value), float(snapshot.quantity) * float(snapshot.avg_cost), 0.0)
            for code, snapshot in positions.items()
        }
        weights: dict[str, float] = {}
        for order in orders:
            current_value = projected_values.get(order.ts_code, 0.0)
            delta = float(order.price) * int(order.quantity)
            next_value = current_value + delta if order.side == OrderSide.BUY else max(current_value - delta, 0.0)
            projected_values[order.ts_code] = next_value
            weights[order.ts_code] = next_value / total_assets
        return weights

    @staticmethod
    def _preflight_validate_order(
        order: OrderRequest,
        *,
        trade_date: date,
        securities: dict[str, Security],
        bars: dict[str, Bar],
    ) -> RiskResult | None:
        """执行进入 RiskEngine 前必须满足的基础前置校验。"""
        security = securities.get(order.ts_code)
        if security is None:
            return RiskResult(False, "SecurityPresenceRule", "ERROR", f"证券不存在: {order.ts_code}")
        if not security.is_active_on(trade_date):
            return RiskResult(False, "SecurityActiveRule", "ERROR", f"证券在 {trade_date.isoformat()} 不处于可交易状态: {order.ts_code}")
        if bars.get(order.ts_code) is None:
            return RiskResult(False, "MarketBarAvailabilityRule", "ERROR", f"缺少交易日行情: {order.ts_code} @ {trade_date.isoformat()}")
        if order.quantity <= 0 or order.price <= 0:
            return RiskResult(False, "BasicOrderInputRule", "ERROR", "数量与价格必须大于 0")
        return None
