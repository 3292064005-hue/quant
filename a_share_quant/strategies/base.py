"""策略基类。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from a_share_quant.domain.models import Bar, Security, TargetPosition


class StrategyBase(ABC):
    """统一策略接口。"""

    strategy_id: str

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
