"""绩效指标。"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(slots=True)
class PerformanceMetrics:
    """回测绩效指标。"""

    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe: float
    volatility: float


def compute_metrics(equity_curve: list[float], annual_days: int = 252, risk_free_rate: float = 0.0) -> PerformanceMetrics:
    """根据净值曲线计算绩效指标。

    Args:
        equity_curve: 每个观测点的总资产值。
        annual_days: 年化交易日数量。
        risk_free_rate: 年化无风险利率，按离散日收益近似换算为日收益率。

    Returns:
        `PerformanceMetrics`。

    Raises:
        ValueError: 当净值曲线为空或长度不足时抛出。
    """
    if len(equity_curve) < 2:
        raise ValueError("净值曲线长度至少为 2")
    curve = np.array(equity_curve, dtype=float)
    returns = curve[1:] / curve[:-1] - 1.0
    total_return = float(curve[-1] / curve[0] - 1.0)
    annual_return = float((curve[-1] / curve[0]) ** (annual_days / max(len(returns), 1)) - 1.0)
    running_max = np.maximum.accumulate(curve)
    drawdowns = curve / running_max - 1.0
    max_drawdown = float(drawdowns.min())
    volatility = float(np.std(returns, ddof=1) * np.sqrt(annual_days)) if len(returns) > 1 else 0.0
    daily_rf = (1.0 + risk_free_rate) ** (1.0 / annual_days) - 1.0
    excess_returns = returns - daily_rf
    sharpe = float(np.mean(excess_returns) / np.std(returns, ddof=1) * np.sqrt(annual_days)) if len(returns) > 1 and np.std(returns, ddof=1) > 0 else 0.0
    return PerformanceMetrics(
        total_return=total_return,
        annual_return=annual_return,
        max_drawdown=max_drawdown,
        sharpe=sharpe,
        volatility=volatility,
    )
