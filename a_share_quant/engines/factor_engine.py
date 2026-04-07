"""因子引擎。"""
from __future__ import annotations

from a_share_quant.domain.models import Bar


class FactorEngine:
    """提供基础因子计算。"""

    @staticmethod
    def momentum(bars: list[Bar], lookback: int) -> float:
        """计算 lookback 期动量收益率。"""
        if len(bars) < lookback + 1:
            raise ValueError("历史数据不足")
        base = bars[-(lookback + 1)].close
        if base <= 0:
            raise ValueError("基准价格必须大于 0")
        return bars[-1].close / base - 1.0
