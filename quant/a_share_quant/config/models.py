"""配置模型定义。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from a_share_quant.core.broker_acceptance import readiness_level_name

_ALLOWED_DATA_PROVIDERS = {"csv", "tushare", "akshare"}
_ALLOWED_MISSING_PRICE_POLICIES = {"last_known", "avg_cost", "reject"}
_ALLOWED_DATA_ACCESS_MODES = {"preload", "stream"}
_ALLOWED_REBALANCE_MODES = {"close"}
_ALLOWED_EVENT_MODES = {"bus"}
_ALLOWED_FILL_MODELS = {"volume_share"}
_ALLOWED_SLIPPAGE_MODELS = {"bps"}
_ALLOWED_FEE_MODELS = {"broker_bps"}
_ALLOWED_TAX_MODELS = {"a_share_sell_tax"}
_ALLOWED_BROKER_PROVIDERS = {"mock", "qmt", "ptrade"}
_ALLOWED_BROKER_EVENT_SOURCE_MODES = {"auto", "poll", "subscribe"}
_ALLOWED_PATH_RESOLUTION_MODES = {"config_dir", "cwd"}
_ALLOWED_RUNTIME_MODES = {"research_backtest", "paper_trade", "live_trade"}
_ALLOWED_DISTRIBUTION_PROFILES = {"core", "workstation", "production"}
_ALLOWED_CALENDAR_POLICIES = {"demo", "derive", "strict"}


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
    distribution_profile: str = "workstation"

    @field_validator("path_resolution_mode")
    @classmethod
    def _validate_path_resolution_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="app.path_resolution_mode", allowed=_ALLOWED_PATH_RESOLUTION_MODES)

    @field_validator("runtime_mode")
    @classmethod
    def _validate_runtime_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="app.runtime_mode", allowed=_ALLOWED_RUNTIME_MODES)

    @field_validator("distribution_profile")
    @classmethod
    def _validate_distribution_profile(cls, value: str) -> str:
        return _normalize_lower(value, field_name="app.distribution_profile", allowed=_ALLOWED_DISTRIBUTION_PROFILES)


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
    calendar_policy: str = "demo"

    @field_validator("provider")
    @classmethod
    def _validate_provider(cls, value: str) -> str:
        return _normalize_lower(value, field_name="data.provider", allowed=_ALLOWED_DATA_PROVIDERS)

    @field_validator("calendar_policy")
    @classmethod
    def _validate_calendar_policy(cls, value: str) -> str:
        return _normalize_lower(value, field_name="data.calendar_policy", allowed=_ALLOWED_CALENDAR_POLICIES)


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


class BacktestExecutionSection(BaseModel):
    """执行模型配置。"""

    event_mode: str = "bus"
    fill_model: str = "volume_share"
    slippage_model: str = "bps"
    fee_model: str = "broker_bps"
    tax_model: str = "a_share_sell_tax"
    max_volume_share: float = 1.0
    allow_partial_fill: bool = True
    min_trade_lot: int = 100

    @field_validator("event_mode")
    @classmethod
    def _validate_event_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.execution.event_mode", allowed=_ALLOWED_EVENT_MODES)

    @field_validator("fill_model")
    @classmethod
    def _validate_fill_model(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.execution.fill_model", allowed=_ALLOWED_FILL_MODELS)

    @field_validator("slippage_model")
    @classmethod
    def _validate_slippage_model(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.execution.slippage_model", allowed=_ALLOWED_SLIPPAGE_MODELS)

    @field_validator("fee_model")
    @classmethod
    def _validate_fee_model(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.execution.fee_model", allowed=_ALLOWED_FEE_MODELS)

    @field_validator("tax_model")
    @classmethod
    def _validate_tax_model(cls, value: str) -> str:
        return _normalize_lower(value, field_name="backtest.execution.tax_model", allowed=_ALLOWED_TAX_MODELS)


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
    execution: BacktestExecutionSection = Field(default_factory=BacktestExecutionSection)

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
    research_signal_run_id: str | None = None


class BrokerSection(BaseModel):
    """券商配置。"""

    provider: str = "mock"
    endpoint: str = ""
    account_id: str = ""
    allowed_account_ids: list[str] = Field(default_factory=list)
    operation_timeout_seconds: float | None = 15.0
    strict_contract_mapping: bool = True
    client_factory: str | None = None
    event_source_mode: str = "auto"
    acceptance_manifest_path: str | None = None
    execute_acceptance_suite: bool = False
    required_readiness_level: str | None = None

    @field_validator("provider")
    @classmethod
    def _validate_provider(cls, value: str) -> str:
        return _normalize_lower(value, field_name="broker.provider", allowed=_ALLOWED_BROKER_PROVIDERS)

    @field_validator("event_source_mode")
    @classmethod
    def _validate_event_source_mode(cls, value: str) -> str:
        return _normalize_lower(value, field_name="broker.event_source_mode", allowed=_ALLOWED_BROKER_EVENT_SOURCE_MODES)

    @field_validator("allowed_account_ids")
    @classmethod
    def _dedupe_accounts(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            candidate = str(item).strip()
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
        return normalized

    @field_validator("required_readiness_level")
    @classmethod
    def _validate_required_readiness_level(cls, value: str | None) -> str | None:
        if value is None or not str(value).strip():
            return None
        return readiness_level_name(value)


class OperatorSection(BaseModel):
    """operator command plane 配置。"""

    require_approval: bool = False
    max_batch_orders: int = 20
    default_requested_by: str = "operator"
    fail_fast: bool = False
    supervisor_scan_interval_seconds: float = 1.0
    supervisor_lease_seconds: float = 15.0
    supervisor_heartbeat_interval_seconds: float = 5.0
    supervisor_idle_timeout_seconds: float = 2.0
    supervisor_max_sessions_per_pass: int = 10

    @field_validator(
        "supervisor_scan_interval_seconds",
        "supervisor_lease_seconds",
        "supervisor_heartbeat_interval_seconds",
        "supervisor_idle_timeout_seconds",
    )
    @classmethod
    def _validate_positive_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("operator supervisor 时间配置必须大于 0")
        return float(value)

    @field_validator("supervisor_max_sessions_per_pass")
    @classmethod
    def _validate_supervisor_batch_size(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("operator.supervisor_max_sessions_per_pass 必须大于 0")
        return int(value)

    @model_validator(mode="after")
    def _validate_supervisor_heartbeat_contract(self) -> "OperatorSection":
        if self.supervisor_heartbeat_interval_seconds >= self.supervisor_lease_seconds:
            raise ValueError("operator.supervisor_heartbeat_interval_seconds 必须小于 operator.supervisor_lease_seconds")
        return self


class ResearchSection(BaseModel):
    """research 缓存与批处理配置。"""

    enable_cache: bool = True
    cache_namespace: str = "default"
    cache_schema_version: str = "v3"
    dataset_scope_cache_invalidation: bool = True
    max_cached_entries: int = 500
    record_query_runs: bool = False


class PluginsSection(BaseModel):
    """插件启停与发现配置。"""

    enabled_builtin: list[str] = Field(default_factory=list)
    disabled: list[str] = Field(default_factory=list)
    external: list[str] = Field(default_factory=list)

    @field_validator("enabled_builtin", "disabled", "external")
    @classmethod
    def _dedupe_values(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            candidate = str(item).strip()
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
        return normalized


class AppConfig(BaseModel):
    """聚合配置对象。"""

    app: AppSection = Field(default_factory=AppSection)
    data: DataSection = Field(default_factory=DataSection)
    database: DatabaseSection = Field(default_factory=DatabaseSection)
    backtest: BacktestSection = Field(default_factory=BacktestSection)
    risk: RiskSection = Field(default_factory=RiskSection)
    strategy: StrategySection = Field(default_factory=StrategySection)
    broker: BrokerSection = Field(default_factory=BrokerSection)
    operator: OperatorSection = Field(default_factory=OperatorSection)
    research: ResearchSection = Field(default_factory=ResearchSection)
    plugins: PluginsSection = Field(default_factory=PluginsSection)

    @model_validator(mode="after")
    def _validate_distribution_profile_contract(self) -> "AppConfig":
        profile = self.app.distribution_profile
        if profile == "production":
            violations: list[str] = []
            if self.app.runtime_mode not in {"paper_trade", "live_trade"}:
                violations.append("production profile 仅支持 paper_trade/live_trade")
            if self.broker.provider == "mock":
                violations.append("production profile 禁止 broker.provider=mock")
            if self.data.calendar_policy != "strict":
                violations.append("production profile 要求 data.calendar_policy=strict")
            if self.data.allow_degraded_data:
                violations.append("production profile 要求 data.allow_degraded_data=false")
            if not self.data.fail_on_degraded_data:
                violations.append("production profile 要求 data.fail_on_degraded_data=true")
            if not self.broker.strict_contract_mapping:
                violations.append("production profile 要求 broker.strict_contract_mapping=true")
            if not self.operator.require_approval:
                violations.append("production profile 要求 operator.require_approval=true")
            if not self.broker.acceptance_manifest_path:
                violations.append("production profile 要求 broker.acceptance_manifest_path 指向已验证的 acceptance manifest")
            if violations:
                raise ValueError("；".join(violations))
        return self

    def distribution_capabilities(self) -> dict[str, Any]:
        from a_share_quant.app.distribution_profile_contract import get_distribution_profile_spec

        spec = get_distribution_profile_spec(self.app.distribution_profile)
        return dict(spec.capabilities)
