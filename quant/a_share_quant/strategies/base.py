"""策略基类与组件契约。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from a_share_quant.domain.models import Bar, Security, TargetPosition


@dataclass(slots=True)
class StrategyComponentManifest:
    """描述策略所依赖的核心组件契约。

    Boundary Behavior:
        - 这是当前 research kernel 的正式组件声明，而不是未来 paper/live orchestration 的执行编排定义；
        - 未声明的组件会在服务层补默认值，避免历史策略因新增字段无法装载；
        - 该 manifest 会进入策略仓储与 run manifest，用于追踪同一策略在不同组件配置下的可重复性。
    """

    universe_component: str = "builtin.all_active_a_share"
    signal_component: str = "builtin.direct_targets"
    factor_component: str = "builtin.none"
    portfolio_construction_component: str = "builtin.portfolio_engine"
    execution_policy_component: str = "builtin.execution_engine"
    risk_gate_component: str = "builtin.risk_engine"
    benchmark_component: str = "builtin.daily_close_relative"
    capability_tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "universe_component": self.universe_component,
            "signal_component": self.signal_component,
            "factor_component": self.factor_component,
            "portfolio_construction_component": self.portfolio_construction_component,
            "execution_policy_component": self.execution_policy_component,
            "risk_gate_component": self.risk_gate_component,
            "benchmark_component": self.benchmark_component,
            "capability_tags": list(self.capability_tags),
        }


class StrategyBase(ABC):
    """统一策略接口。"""

    strategy_id: str

    @classmethod
    def component_manifest(cls) -> StrategyComponentManifest:
        """返回策略组件契约。

        Returns:
            `StrategyComponentManifest`，用于声明该策略依赖的证券池/信号/因子/组合/执行组件。

        Boundary Behavior:
            - 子类未覆写时返回 research kernel 的默认组件；
            - 该方法不执行真实组件装配，仅提供正式元数据契约。
        """
        return StrategyComponentManifest(capability_tags=["research", "single_strategy"])

    @abstractmethod
    def required_history_bars(self) -> int:
        """返回策略所需最小历史 bar 数量。"""

    @abstractmethod
    def generate_targets(
        self,
        history_by_symbol: dict[str, list[Bar]],
        current_date: date,
        securities: dict[str, Security],
    ) -> list[TargetPosition]:
        """生成目标仓位。"""

    def should_rebalance(self, eligible_trade_index: int) -> bool:
        """判断当前是否执行调仓。

        Args:
            eligible_trade_index: 自策略满足最小历史要求开始计数的交易索引，从 0 开始。

        Returns:
            是否执行调仓。默认每个可调仓日都调仓。
        """
        return True
