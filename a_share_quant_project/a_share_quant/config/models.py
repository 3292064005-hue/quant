"""配置模型定义。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class AppSection(BaseModel):
    """应用级配置。"""

    name: str = "AShareQuantWorkstation"
    environment: str = "local"
    timezone: str = "Asia/Shanghai"
    logs_dir: str = "runtime/logs"


class DataSection(BaseModel):
    """数据配置。"""

    storage_dir: str = "runtime/data"
    reports_dir: str = "runtime/reports"
    default_exchange: str = "SSE"
    default_csv_encoding: str = "utf-8"
    provider: str = "csv"
    adj_type: str = ""
    tushare_token: str | None = None
    tushare_token_env: str = "TUSHARE_TOKEN"
    max_symbols_per_run: int | None = None
    request_timeout_seconds: float | None = 15.0


class DatabaseSection(BaseModel):
    """数据库配置。"""

    path: str = "runtime/a_share_quant.db"


class BacktestMetricsSection(BaseModel):
    """绩效计算配置。"""

    annual_trading_days: int = 252
    risk_free_rate: float = 0.0


class BacktestSection(BaseModel):
    """回测配置。"""

    initial_cash: float = 1_000_000.0
    fee_bps: float = 3.0
    tax_bps: float = 10.0
    slippage_bps: float = 5.0
    benchmark_symbol: str = "000300.SH"
    rebalance_mode: str = "close"
    report_name_template: str = "{strategy_id}_{run_id}_backtest.json"
    metrics: BacktestMetricsSection = Field(default_factory=BacktestMetricsSection)


class RiskRuleSection(BaseModel):
    """风险规则开关。"""

    enforce_lot_size: bool = True
    block_st: bool = True
    block_suspended: bool = True
    block_limit_up_buy: bool = True
    block_limit_down_sell: bool = True
    sequential_cash_reservation: bool = True


class RiskSection(BaseModel):
    """风险控制配置。"""

    max_position_weight: float = 0.2
    max_order_value: float = 200_000.0
    blocked_symbols: list[str] = Field(default_factory=list)
    kill_switch: bool = False
    rules: RiskRuleSection = Field(default_factory=RiskRuleSection)


class StrategySection(BaseModel):
    """策略配置。"""

    strategy_id: str = "momentum_top_n"
    lookback: int = 5
    top_n: int = 2
    holding_days: int = 3


class BrokerSection(BaseModel):
    """券商配置。"""

    provider: str = "mock"
    endpoint: str = ""
    account_id: str = ""
    operation_timeout_seconds: float | None = 15.0


class AppConfig(BaseModel):
    """聚合配置对象。"""

    app: AppSection = Field(default_factory=AppSection)
    data: DataSection = Field(default_factory=DataSection)
    database: DatabaseSection = Field(default_factory=DatabaseSection)
    backtest: BacktestSection = Field(default_factory=BacktestSection)
    risk: RiskSection = Field(default_factory=RiskSection)
    strategy: StrategySection = Field(default_factory=StrategySection)
    broker: BrokerSection = Field(default_factory=BrokerSection)
