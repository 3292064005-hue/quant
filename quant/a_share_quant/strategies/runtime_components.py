"""正式策略组件运行时。

本模块把策略的 component_manifest 从“只用于登记的声明”推进为
可被回测主链实际消费的执行合同。
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from a_share_quant.domain.models import Bar, Security, TargetPosition
from a_share_quant.strategies.base import StrategyComponentManifest


class StrategyRuntimeBindingError(ValueError):
    """策略运行时绑定非法。"""


@dataclass(frozen=True, slots=True)
class RankedSelection:
    """信号层输出的有序证券选择结果。"""

    ts_code: str
    score: float


class AllActiveAShareUniverse:
    """选择当前交易日存在行情且处于活跃状态的证券池。"""

    def select(self, frame) -> tuple[dict[str, list[Bar]], dict[str, Security]]:
        """返回活跃证券的历史序列与证券元数据。"""
        histories = {ts_code: bars for ts_code, bars in frame.active_history.items() if bars}
        securities = {ts_code: sec for ts_code, sec in frame.active_securities.items() if ts_code in histories}
        return histories, securities


class NoFactorComponent:
    """空因子组件。

    Boundary Behavior:
        - 该组件不直接生成信号，仅用于 direct-target / research-signal 等
          已有目标权重来源的执行链。
        - ``required_history_bars`` 返回 1，表示主链至少需要当前交易日行情。
    """

    def required_history_bars(self, strategy) -> int:
        return 1

    def compute(self, histories_by_symbol: dict[str, list[Bar]], *, strategy) -> dict[str, float]:
        return {}


class MomentumFactorComponent:
    """基于收盘价收益率的动量因子组件。"""

    def required_history_bars(self, strategy) -> int:
        lookback = int(getattr(strategy, "lookback", 0) or 0)
        if lookback < 1:
            raise StrategyRuntimeBindingError("动量因子组件要求 strategy.lookback >= 1")
        return lookback + 1

    def compute(self, histories_by_symbol: dict[str, list[Bar]], *, strategy) -> dict[str, float]:
        from a_share_quant.engines.factor_engine import FactorEngine

        lookback = int(getattr(strategy, "lookback", 0) or 0)
        if lookback < 1:
            raise StrategyRuntimeBindingError("动量因子组件要求 strategy.lookback >= 1")
        values: dict[str, float] = {}
        for ts_code, bars in histories_by_symbol.items():
            try:
                values[ts_code] = FactorEngine.momentum(bars, lookback)
            except ValueError:
                continue
        return values


class TopNSelectionSignal:
    """按照因子分数降序选择前 N 名证券。"""

    def select(self, factor_values: dict[str, float], *, strategy) -> list[RankedSelection]:
        top_n = int(getattr(strategy, "top_n", 0) or 0)
        if top_n < 1:
            raise StrategyRuntimeBindingError("top_n_selection 要求 strategy.top_n >= 1")
        ordered = sorted(factor_values.items(), key=lambda item: item[1], reverse=True)
        return [RankedSelection(ts_code=ts_code, score=score) for ts_code, score in ordered[:top_n]]


class DirectTargetsSignal:
    """兼容历史策略接口的直接目标仓位信号组件。"""

    def build_targets(self, strategy, frame, histories_by_symbol: dict[str, list[Bar]], securities: dict[str, Security]) -> list[TargetPosition]:
        if not hasattr(strategy, "generate_targets"):
            raise StrategyRuntimeBindingError(f"策略 {type(strategy).__name__} 未实现 generate_targets()")
        return list(strategy.generate_targets(histories_by_symbol, frame.trade_date, securities))


class ResearchSignalSnapshotComponent:
    """把 research signal_snapshot 产物转换为回测目标仓位。"""

    def build_targets(self, payload: dict[str, Any], *, active_securities: dict[str, Security]) -> list[TargetPosition]:
        selected_symbols = payload.get("selected_symbols")
        if not isinstance(selected_symbols, list):
            raise StrategyRuntimeBindingError("research signal_snapshot 缺少 selected_symbols 列表")
        filtered: list[dict[str, Any]] = []
        for item in selected_symbols:
            if not isinstance(item, dict):
                continue
            ts_code = str(item.get("ts_code", "")).strip()
            if not ts_code or ts_code not in active_securities:
                continue
            filtered.append(item)
        if not filtered:
            return []
        explicit_weights = [float(item.get("target_weight", 0.0) or 0.0) for item in filtered]
        has_explicit_weight = any(weight > 0 for weight in explicit_weights)
        if has_explicit_weight:
            total_weight = sum(max(weight, 0.0) for weight in explicit_weights)
            if total_weight <= 0:
                raise StrategyRuntimeBindingError("research signal_snapshot target_weight 总和必须大于 0")
            normalized_weights = [max(weight, 0.0) / total_weight for weight in explicit_weights]
        else:
            normalized_weights = [1.0 / len(filtered)] * len(filtered)
        targets: list[TargetPosition] = []
        for item, weight in zip(filtered, normalized_weights, strict=True):
            score = float(item.get("score", 0.0) or 0.0)
            ts_code = str(item["ts_code"])
            targets.append(
                TargetPosition(
                    ts_code=ts_code,
                    target_weight=weight,
                    score=score,
                    reason=f"research signal_snapshot::{payload.get('signal_type', 'unknown')}",
                )
            )
        return targets




class BypassedPortfolioComponent:
    """显式表示当前执行链不经过组合构造步骤。

    Boundary Behavior:
        - 该组件仅用于 manifest 语义收口，不参与 research/direct-target 路径的真实目标生成；
        - 若误被直接调用，会原样返回输入的目标仓位序列。
    """

    def build_targets(self, targets: Iterable[TargetPosition], *, strategy) -> list[TargetPosition]:
        return list(targets)


class EqualWeightTopNPortfolio:
    """把证券选择结果转换为等权目标仓位。"""

    def build_targets(self, selections: Iterable[RankedSelection], *, strategy) -> list[TargetPosition]:
        ordered = list(selections)
        if not ordered:
            return []
        weight = 1.0 / len(ordered)
        lookback = getattr(strategy, "lookback", None)
        reason_prefix = f"{lookback}日动量排名" if lookback else "组件化选股排名"
        return [
            TargetPosition(
                ts_code=item.ts_code,
                target_weight=weight,
                score=item.score,
                reason=reason_prefix,
            )
            for item in ordered
        ]


@dataclass(slots=True)
class StrategyExecutionRuntime:
    """由 component_manifest 驱动的正式策略执行运行时。

    Args:
        manifest: 策略组件契约。
        universe_component: 证券池组件。
        factor_component: 因子组件。
        signal_component: 信号组件。
        portfolio_component: 目标仓位构造组件。
        research_signal_payload: 可选 research signal_snapshot 载荷。

    Boundary Behavior:
        - 该运行时是回测主链唯一消费的组件执行入口；
        - ``builtin.direct_targets`` 会回退到策略对象自身的 ``generate_targets``，用于兼容外部策略；
        - ``research.signal_snapshot`` 只接受正式 research workflow 落库的 ``signal_snapshot`` 结果，不接受裸 dict 伪装输入；
        - universe/factor/signal/portfolio 任一组件缺失都会在装配期显式失败，不允许 silent fallback。
    """

    manifest: StrategyComponentManifest
    universe_component: Any
    factor_component: Any
    signal_component: Any
    portfolio_component: Any
    research_signal_payload: dict[str, Any] | None = None

    def required_history_bars(self, strategy) -> int:
        factor_component = self.factor_component
        if hasattr(factor_component, "required_history_bars"):
            return max(int(factor_component.required_history_bars(strategy)), 1)
        if hasattr(strategy, "required_history_bars"):
            return max(int(strategy.required_history_bars()), 1)
        return 1

    def should_rebalance(self, strategy, eligible_trade_index: int) -> bool:
        if hasattr(strategy, "should_rebalance"):
            return bool(strategy.should_rebalance(eligible_trade_index))
        return True

    def generate_targets(self, strategy, frame) -> list[TargetPosition]:
        histories_by_symbol, securities = self.universe_component.select(frame)
        signal_name = self.manifest.signal_component
        if signal_name == "builtin.direct_targets":
            return self.signal_component.build_targets(strategy, frame, histories_by_symbol, securities)
        if signal_name == "research.signal_snapshot":
            if self.research_signal_payload is None:
                raise StrategyRuntimeBindingError("research.signal_snapshot 已绑定但缺少 research signal payload")
            return self.signal_component.build_targets(self.research_signal_payload, active_securities=securities)
        factor_values = self.factor_component.compute(histories_by_symbol, strategy=strategy)
        selections = self.signal_component.select(factor_values, strategy=strategy)
        return self.portfolio_component.build_targets(selections, strategy=strategy)
