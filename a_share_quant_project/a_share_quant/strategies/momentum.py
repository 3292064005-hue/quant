"""动量选股策略。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from a_share_quant.domain.models import Bar, Security, TargetPosition
from a_share_quant.strategies.base import StrategyBase


@dataclass(slots=True)
class TopNMomentumStrategy(StrategyBase):
    """按过去 N 个 bar 收益率排名，等权持有前 N 名证券。"""

    strategy_id: str
    lookback: int
    top_n: int
    holding_days: int = 1

    def required_history_bars(self) -> int:
        return self.lookback + 1

    def should_rebalance(self, eligible_trade_index: int) -> bool:
        """按配置的 holding_days 控制调仓节奏。"""
        if self.holding_days <= 1:
            return True
        return eligible_trade_index % self.holding_days == 0

    def generate_targets(
        self,
        history_by_symbol: dict[str, list[Bar]],
        current_date: date,
        securities: dict[str, Security],
    ) -> list[TargetPosition]:
        """根据历史收盘价构造目标权重。

        Args:
            history_by_symbol: 截至当前日期的历史行情。
            current_date: 当前交易日。
            securities: 证券元数据字典。

        Returns:
            目标仓位列表。历史不足时返回空列表。

        Boundary Behavior:
            ST 与非上市状态不在本层过滤，由风控层统一拦截。
            本层仅在入参提供的证券范围内打分，因此调用方必须保证证券池已经
            过历史有效性过滤。
        """
        scored: list[tuple[str, float]] = []
        for ts_code, bars in history_by_symbol.items():
            if len(bars) < self.required_history_bars():
                continue
            latest = bars[-1]
            if latest.trade_date != current_date:
                continue
            base = bars[-(self.lookback + 1)].close
            if base <= 0:
                continue
            momentum = latest.close / base - 1.0
            scored.append((ts_code, momentum))
        scored.sort(key=lambda item: item[1], reverse=True)
        selected = scored[: self.top_n]
        if not selected:
            return []
        weight = 1.0 / len(selected)
        return [
            TargetPosition(ts_code=ts_code, target_weight=weight, score=score, reason=f"{self.lookback}日动量排名")
            for ts_code, score in selected
        ]
