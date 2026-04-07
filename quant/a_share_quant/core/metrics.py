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


@dataclass(slots=True)
class RelativePerformanceMetrics:
    """相对 benchmark 的绩效指标。"""

    benchmark_total_return: float
    benchmark_annual_return: float
    excess_total_return: float
    tracking_error: float
    information_ratio: float
    beta: float
    alpha: float


def compute_metrics(equity_curve: list[float], annual_days: int = 252, risk_free_rate: float = 0.0) -> PerformanceMetrics:
    """根据净值曲线计算绩效指标。"""
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


def compute_relative_metrics(
    equity_curve: list[float],
    benchmark_curve: list[float],
    *,
    annual_days: int = 252,
    risk_free_rate: float = 0.0,
) -> RelativePerformanceMetrics:
    """根据策略/benchmark 两条曲线计算相对绩效。

    Raises:
        ValueError: 输入长度不足或两条曲线长度不一致时抛出。
    """
    if len(equity_curve) < 2 or len(benchmark_curve) < 2:
        raise ValueError("策略曲线与 benchmark 曲线长度至少为 2")
    if len(equity_curve) != len(benchmark_curve):
        raise ValueError("策略曲线与 benchmark 曲线长度必须一致")
    strategy_curve = np.array(equity_curve, dtype=float)
    benchmark = np.array(benchmark_curve, dtype=float)
    strategy_returns = strategy_curve[1:] / strategy_curve[:-1] - 1.0
    benchmark_returns = benchmark[1:] / benchmark[:-1] - 1.0
    excess_returns = strategy_returns - benchmark_returns
    benchmark_total_return = float(benchmark[-1] / benchmark[0] - 1.0)
    benchmark_annual_return = float((benchmark[-1] / benchmark[0]) ** (annual_days / max(len(benchmark_returns), 1)) - 1.0)
    excess_total_return = float(strategy_curve[-1] / strategy_curve[0] - benchmark[-1] / benchmark[0])
    tracking_error = float(np.std(excess_returns, ddof=1) * np.sqrt(annual_days)) if len(excess_returns) > 1 else 0.0
    information_ratio = float(np.mean(excess_returns) / np.std(excess_returns, ddof=1) * np.sqrt(annual_days)) if len(excess_returns) > 1 and np.std(excess_returns, ddof=1) > 0 else 0.0
    if len(strategy_returns) > 1 and np.var(benchmark_returns, ddof=1) > 0:
        covariance = float(np.cov(strategy_returns, benchmark_returns, ddof=1)[0, 1])
        beta = covariance / float(np.var(benchmark_returns, ddof=1))
    else:
        beta = 0.0
    daily_rf = (1.0 + risk_free_rate) ** (1.0 / annual_days) - 1.0
    alpha = float((np.mean(strategy_returns - daily_rf) - beta * np.mean(benchmark_returns - daily_rf)) * annual_days)
    return RelativePerformanceMetrics(
        benchmark_total_return=benchmark_total_return,
        benchmark_annual_return=benchmark_annual_return,
        excess_total_return=excess_total_return,
        tracking_error=tracking_error,
        information_ratio=information_ratio,
        beta=beta,
        alpha=alpha,
    )
