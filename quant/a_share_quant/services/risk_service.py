"""风险服务。"""
from __future__ import annotations

from a_share_quant.config.models import BacktestSection, RiskSection
from a_share_quant.core.rules.risk_rules import (
    BlockedSecurityRule,
    KillSwitchRule,
    LotSizeRule,
    MaxOrderValueRule,
    MaxPositionWeightRule,
    PriceLimitRule,
    RiskRule,
    STBlockRule,
    TradingAvailabilityRule,
)
from a_share_quant.engines.risk_engine import RiskEngine


class RiskService:
    """构造风险引擎。"""

    def __init__(self, risk_config: RiskSection, backtest_config: BacktestSection) -> None:
        self.risk_config = risk_config
        self.backtest_config = backtest_config

    def build_engine(self) -> RiskEngine:
        rules: list[RiskRule] = [KillSwitchRule(), BlockedSecurityRule()]
        if self.risk_config.rules.enforce_lot_size:
            rules.append(LotSizeRule())
        if self.risk_config.rules.block_st:
            rules.append(STBlockRule())
        if self.risk_config.rules.block_suspended:
            rules.append(TradingAvailabilityRule())
        rules.append(
            PriceLimitRule(
                block_limit_up_buy=self.risk_config.rules.block_limit_up_buy,
                block_limit_down_sell=self.risk_config.rules.block_limit_down_sell,
            )
        )
        rules.extend(
            [
                MaxOrderValueRule(self.risk_config.max_order_value),
                MaxPositionWeightRule(self.risk_config.max_position_weight),
            ]
        )
        return RiskEngine(
            rules=rules,
            blocked_symbols=set(self.risk_config.blocked_symbols),
            kill_switch=self.risk_config.kill_switch,
            sequential_cash_reservation=self.risk_config.rules.sequential_cash_reservation,
            fee_bps=self.backtest_config.fee_bps,
            tax_bps=self.backtest_config.tax_bps,
        )
