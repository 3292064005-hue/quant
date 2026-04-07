"""配置模型定义。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


_ALLOWED_DATA_PROVIDERS = {"csv", "tushare", "akshare"}
_ALLOWED_MISSING_PRICE_POLICIES = {"last_known", "avg_cost", "reject"}
_ALLOWED_DATA_ACCESS_MODES = {"preload", "stream"}
_ALLOWED_REBALANCE_MODES = {"close"}
_ALLOWED_BROKER_PROVIDERS = {"mock", "qmt", "ptrade"}
_ALLOWED_PATH_RESOLUTION_MODES = {"config_dir", "cwd"}
_ALLOWED_RUNTIME_MODES = {"research_backtest", "paper_trade", "live_trade"}


def _normalize_lower(value: str, *, field_name: str, allowed: set[str]) -> str:
    normalized = value.strip().lower()
    if normalized not in allowed:
        raise ValueError(f"{field_name} 不支持: {value}；允许值={sorted(allowed)}")
    return normalized


class AppSection(BaseModel):
    """应用级配置。"""

    name: str = "AShareQuantWorkstation"
    environment: str = "local"
    timezone: str = "Asia/Shanghai"
    logs_dir: str = "runtime/logs"
    path_resolution_mode: str = "config_dir"
    runtime_mode: str = "research_backtest"

    @field_validator("path_resolution_mode")
    @classmethod
    def _validate_path_resolution_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="app.path_resolution_mode", allowed=_ALLOWED_PATH_RESOLUTION_MODES)

    @field_validator("runtime_mode")
    @classmethod
    def _validate_runtime_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="app.runtime_mode", allowed=_ALLOWED_RUNTIME_MODES)


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
    allow_degraded_data: bool = True
    fail_on_degraded_data: bool = False

    @field_validator("provider")
    @classmethod
    def _validate_provider(cls, value: str) -> str:
        return _normalize_lower(value, field_name="data.provider", allowed=_ALLOWED_DATA_PROVIDERS)


class DatabaseSection(BaseModel):
    """数据库配置。"""

    path: str = "runtime/a_share_quant.db"


class BacktestMetricsSection(BaseModel):
    """绩效计算配置。"""

    annual_trading_days: int = 252
    risk_free_rate: float = 0.0


class BacktestValuationSection(BaseModel):
    """回测估值配置。"""

    missing_price_policy: str = "last_known"

    @field_validator("missing_price_policy")
    @classmethod
    def _validate_missing_price_policy(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.valuation.missing_price_policy", allowed=_ALLOWED_MISSING_PRICE_POLICIES)


class BacktestSection(BaseModel):
    """回测配置。"""

    initial_cash: float = 1_000_000.0
    fee_bps: float = 3.0
    tax_bps: float = 10.0
    slippage_bps: float = 5.0
    benchmark_symbol: str = "000300.SH"
    rebalance_mode: str = "close"
    report_name_template: str = "{strategy_id}_{run_id}_backtest.json"
    data_access_mode: str = "preload"
    metrics: BacktestMetricsSection = Field(default_factory=BacktestMetricsSection)
    valuation: BacktestValuationSection = Field(default_factory=BacktestValuationSection)

    @field_validator("rebalance_mode")
    @classmethod
    def _validate_rebalance_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.rebalance_mode", allowed=_ALLOWED_REBALANCE_MODES)

    @field_validator("data_access_mode")
    @classmethod
    def _validate_data_access_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.data_access_mode", allowed=_ALLOWED_DATA_ACCESS_MODES)


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
    class_path: str | None = None
    version: str = "1.0.0"
    params: dict[str, Any] = Field(default_factory=dict)
    lookback: int = 5
    top_n: int = 2
    holding_days: int = 3


class BrokerSection(BaseModel):
    """券商配置。"""

    provider: str = "mock"
    endpoint: str = ""
    account_id: str = ""
    operation_timeout_seconds: float | None = 15.0
    strict_contract_mapping: bool = True
    client_factory: str | None = None

    @field_validator("provider")
    @classmethod
    def _validate_provider(cls, value: str) -> str:
        return _normalize_lower(value, field_name="broker.provider", allowed=_ALLOWED_BROKER_PROVIDERS)


class AppConfig(BaseModel):
    """聚合配置对象。"""

    app: AppSection = Field(default_factory=AppSection)
    data: DataSection = Field(default_factory=DataSection)
    database: DatabaseSection = Field(default_factory=DatabaseSection)
    backtest: BacktestSection = Field(default_factory=BacktestSection)
    risk: RiskSection = Field(default_factory=RiskSection)
    strategy: StrategySection = Field(default_factory=StrategySection)
    broker: BrokerSection = Field(default_factory=BrokerSection)
