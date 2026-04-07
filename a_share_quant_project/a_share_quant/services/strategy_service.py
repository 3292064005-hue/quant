"""策略服务。"""
from __future__ import annotations

from a_share_quant.config.models import AppConfig
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.strategies.momentum import TopNMomentumStrategy


class StrategyService:
    """策略工厂与注册中心。"""

    def __init__(self, config: AppConfig, strategy_repository: StrategyRepository) -> None:
        self.config = config
        self.strategy_repository = strategy_repository

    def build_default(self) -> TopNMomentumStrategy:
        """根据配置构建默认策略并持久化元信息。"""
        strategy = TopNMomentumStrategy(
            strategy_id=self.config.strategy.strategy_id,
            lookback=self.config.strategy.lookback,
            top_n=self.config.strategy.top_n,
            holding_days=self.config.strategy.holding_days,
        )
        self.strategy_repository.save(
            strategy_id=strategy.strategy_id,
            strategy_type=type(strategy).__name__,
            params={"lookback": strategy.lookback, "top_n": strategy.top_n, "holding_days": strategy.holding_days},
        )
        return strategy
