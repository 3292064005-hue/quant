"""领域模型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any


class OrderSide(str, Enum):
    """买卖方向。"""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """订单类型。"""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(str, Enum):
    """订单有效期。"""

    DAY = "DAY"
    GTC = "GTC"


class OrderStatus(str, Enum):
    """订单状态。"""

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELLED = "CANCELLED"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    EXPIRED = "EXPIRED"
    PRE_TRADE_REJECTED = "PRE_TRADE_REJECTED"
    EXECUTION_REJECTED = "EXECUTION_REJECTED"
    REJECTED = "REJECTED"


class BacktestRunStatus(str, Enum):
    """回测运行状态。

    Boundary Behavior:
        - ``ENGINE_COMPLETED`` 表示引擎侧业务结果已完整落库，但报告/sidecar 等产物尚未确认完成；
        - ``ARTIFACT_EXPORT_FAILED`` 表示业务结果可重建，但产物导出失败；
        - ``FAILED`` 保留给引擎执行本身失败的场景。
    """

    RUNNING = "RUNNING"
    ENGINE_COMPLETED = "ENGINE_COMPLETED"
    COMPLETED = "COMPLETED"
    ARTIFACT_EXPORT_FAILED = "ARTIFACT_EXPORT_FAILED"
    FAILED = "FAILED"

    @property
    def business_complete(self) -> bool:
        """返回业务侧是否已完成。"""
        return self in {self.ENGINE_COMPLETED, self.COMPLETED, self.ARTIFACT_EXPORT_FAILED}

    @property
    def rebuildable(self) -> bool:
        """返回当前运行是否可基于数据库结果重建产物。"""
        return self in {self.ENGINE_COMPLETED, self.COMPLETED, self.ARTIFACT_EXPORT_FAILED}


@dataclass(slots=True)
class DataLineage:
    """回测所使用数据快照的谱系信息。"""

    dataset_version_id: str | None = None
    import_run_id: str | None = None
    import_run_ids: list[str] = field(default_factory=list)
    data_source: str = "database_snapshot"
    data_start_date: str | None = None
    data_end_date: str | None = None
    dataset_digest: str | None = None
    degradation_flags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RunArtifacts:
    """回测运行关联的产物清单与 manifest。

    Boundary Behavior:
        - ``report_paths`` / ``event_log_path`` 优先存储为相对 ``reports_dir`` 的可迁移路径；
        - ``run_event_summary`` 与 ``run_events`` 共同构成事件产物契约：前者用于快速摘要，
          后者用于 sidecar 丢失时的数据库内生重建；
        - ``artifact_status`` 与回测 run.status 配合使用：前者描述产物阶段，后者描述整体运行阶段；
        - ``component_manifest`` 记录策略/因子/组合/执行等组件指纹，作为后续扩展的正式契约。
    """

    schema_version: int = 6
    entrypoint: str | None = None
    strategy_version: str | None = None
    runtime_mode: str | None = None
    benchmark_initial_value: float | None = None
    report_paths: list[str] = field(default_factory=list)
    report_artifacts: list[dict[str, Any]] = field(default_factory=list)
    event_log_path: str | None = None
    run_event_summary: dict[str, Any] = field(default_factory=dict)
    artifact_status: str = "PENDING"
    artifact_errors: list[str] = field(default_factory=list)
    engine_completed_at: str | None = None
    artifact_completed_at: str | None = None
    component_manifest: dict[str, Any] = field(default_factory=dict)
    promotion_package: dict[str, Any] = field(default_factory=dict)
    signal_source_run_id: str | None = None
    signal_source_artifact_type: str | None = None


@dataclass(slots=True)
class DataImportRun:
    """市场数据导入运行摘要。"""

    import_run_id: str
    source: str
    status: str
    request_context_json: str
    started_at: str
    finished_at: str | None = None
    securities_count: int = 0
    calendar_count: int = 0
    bars_count: int = 0
    degradation_flags_json: str = "[]"
    warnings_json: str = "[]"
    error_message: str | None = None


@dataclass(slots=True)
class DatasetVersion:
    """可复用的数据版本快照摘要。"""

    dataset_version_id: str
    version_fingerprint: str
    dataset_digest: str
    data_source: str
    data_start_date: str | None = None
    data_end_date: str | None = None
    scope_json: str = "{}"
    import_run_ids_json: str = "[]"
    degradation_flags_json: str = "[]"
    warnings_json: str = "[]"
    created_at: str = ""
    last_used_at: str = ""


@dataclass(slots=True)
class Security:
    """证券基础信息。"""

    ts_code: str
    name: str
    exchange: str
    board: str
    is_st: bool = False
    status: str = "L"
    list_date: date | None = None
    delist_date: date | None = None

    def is_active_on(self, trade_date: date) -> bool:
        """判断证券在给定日期是否应被纳入可交易证券池。"""
        if self.list_date is not None and trade_date < self.list_date:
            return False
        if self.delist_date is not None and trade_date > self.delist_date:
            return False
        return self.status in {"L", "P", "D"}


@dataclass(slots=True)
class TradingCalendarEntry:
    """交易日历项。"""

    exchange: str
    cal_date: date
    is_open: bool
    pretrade_date: date | None = None


@dataclass(slots=True)
class Bar:
    """日线/分钟线行情。"""

    ts_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    pre_close: float | None = None
    suspended: bool = False
    limit_up: bool = False
    limit_down: bool = False
    adj_type: str = "qfq"


@dataclass(slots=True)
class TargetIntent:
    """研究/策略运行时输出的中间目标契约。"""

    ts_code: str
    target_weight: float
    score: float
    reason: str
    source_signal: str
    runtime_lane: str = "research_backtest"
    source_run_id: str | None = None
    constraints: dict[str, Any] = field(default_factory=dict)

    def to_target_position(self) -> "TargetPosition":
        return TargetPosition(
            ts_code=self.ts_code,
            target_weight=self.target_weight,
            score=self.score,
            reason=self.reason,
        )


@dataclass(slots=True)
class TargetPosition:
    """策略输出的目标仓位。"""

    ts_code: str
    target_weight: float
    score: float
    reason: str


@dataclass(slots=True)
class ExecutionIntent:
    """研究/策略晋级到 operator lane 的统一执行意图。

    Boundary Behavior:
        - ``target_positions`` 是 strategy/research 对 operator lane 暴露的唯一目标仓位真相源；
        - ``promotion_package`` 保留 research 晋级合同，供 paper/live 入口复核兼容性；
        - ``metadata`` 仅承载追踪/显示信息，不参与正式风控与下单决策。
    """

    intent_id: str
    intent_type: str
    strategy_id: str
    trade_date: date
    runtime_mode: str
    source_run_id: str | None = None
    account_id: str | None = None
    target_positions: list[TargetPosition] = field(default_factory=list)
    promotion_package: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioDelta:
    """目标仓位与当前账户状态之间的差额快照。"""

    ts_code: str
    current_quantity: int
    target_quantity: int
    delta_quantity: int
    target_weight: float
    score: float
    price: float | None
    reason: str
    side: OrderSide | None = None


@dataclass(slots=True)
class ExecutionIntentPlan:
    """执行意图落到正式订单前的编排计划。"""

    intent: ExecutionIntent
    deltas: list[PortfolioDelta] = field(default_factory=list)
    orders: list[OrderRequest] = field(default_factory=list)
    source_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionIntentSubmissionResult:
    """执行意图提交后的组合结果。"""

    plan: ExecutionIntentPlan
    trade_session: TradeSessionResult


@dataclass(slots=True)
class OrderRequest:
    """待提交订单。

    Boundary Behavior:
        - ``quantity`` 始终表示原始申请数量，不会因为部分成交被原地缩小；
        - ``filled_quantity`` / ``avg_fill_price`` 追踪执行进度，供回测/回放/真实 broker 统一消费；
        - ``broker_order_id`` 允许真实 broker 或模拟 broker 在提交后补充映射。
    """

    order_id: str
    trade_date: date
    strategy_id: str
    ts_code: str
    side: OrderSide
    price: float
    quantity: int
    reason: str
    status: OrderStatus = OrderStatus.CREATED
    run_id: str | None = None
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.DAY
    broker_order_id: str | None = None
    filled_quantity: int = 0
    avg_fill_price: float | None = None
    last_error: str | None = None
    account_id: str | None = None

    @property
    def remaining_quantity(self) -> int:
        """返回剩余未成交数量。"""
        return max(self.quantity - self.filled_quantity, 0)

    def mark_submitted(self, broker_order_id: str | None = None) -> None:
        """将订单推进到已提交状态。"""
        self.status = OrderStatus.SUBMITTED
        if broker_order_id:
            self.broker_order_id = broker_order_id

    def mark_accepted(self, broker_order_id: str | None = None) -> None:
        """将订单推进到已接收状态。"""
        self.status = OrderStatus.ACCEPTED
        if broker_order_id:
            self.broker_order_id = broker_order_id

    def apply_fill(self, *, fill_quantity: int, fill_price: float, broker_order_id: str | None = None) -> None:
        """按成交结果推进订单状态。

        Args:
            fill_quantity: 本次成交数量。
            fill_price: 本次成交价格。
            broker_order_id: 可选 broker 订单号。

        Raises:
            ValueError: 当成交数量非法时抛出。
        """
        if fill_quantity <= 0:
            raise ValueError("fill_quantity 必须大于 0")
        total_filled = self.filled_quantity + fill_quantity
        if total_filled > self.quantity:
            raise ValueError("累计成交数量不能超过原始订单数量")
        if self.avg_fill_price is None or self.filled_quantity <= 0:
            self.avg_fill_price = fill_price
        else:
            notional = self.avg_fill_price * self.filled_quantity + fill_price * fill_quantity
            self.avg_fill_price = notional / total_filled
        self.filled_quantity = total_filled
        if broker_order_id:
            self.broker_order_id = broker_order_id
        self.status = OrderStatus.FILLED if self.filled_quantity >= self.quantity else OrderStatus.PARTIALLY_FILLED

    def mark_rejected(self, status: OrderStatus, reason: str) -> None:
        """记录拒单状态与原因。"""
        self.status = status
        self.last_error = reason


@dataclass(slots=True)
class Fill:
    """成交回报。

    Boundary Behavior:
        - ``order_id`` 表示系统内部领域订单 ID，用于本地持久化与回放关联；
        - ``broker_order_id`` 保留外部券商订单号，允许 query/reconciliation 阶段先携带外部 ID，
          再在回补时重新绑定回本地 ``order_id``。
    """

    fill_id: str
    order_id: str
    trade_date: date
    ts_code: str
    side: OrderSide
    fill_price: float
    fill_quantity: int
    fee: float
    tax: float
    run_id: str | None = None
    broker_order_id: str | None = None
    account_id: str | None = None


@dataclass(slots=True)
class ExecutionReport:
    """订单执行状态回报。"""

    report_id: str
    order_id: str
    trade_date: date
    status: OrderStatus
    requested_quantity: int
    filled_quantity: int
    remaining_quantity: int
    message: str = ""
    fill_price: float | None = None
    fee_estimate: float | None = None
    tax_estimate: float | None = None
    broker_order_id: str | None = None
    account_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderTicket:
    """订单执行跟踪票据。"""

    order_id: str
    requested_quantity: int
    status: OrderStatus = OrderStatus.CREATED
    broker_order_id: str | None = None
    filled_quantity: int = 0
    avg_fill_price: float | None = None
    reports: list[ExecutionReport] = field(default_factory=list)

    @classmethod
    def from_order(cls, order: OrderRequest) -> OrderTicket:
        """基于订单创建票据。"""
        return cls(
            order_id=order.order_id,
            requested_quantity=order.quantity,
            status=order.status,
            broker_order_id=order.broker_order_id,
            filled_quantity=order.filled_quantity,
            avg_fill_price=order.avg_fill_price,
        )

    @property
    def remaining_quantity(self) -> int:
        """返回票据剩余未成交数量。"""
        return max(self.requested_quantity - self.filled_quantity, 0)

    def append_report(self, report: ExecutionReport) -> None:
        """附加执行回报并刷新票据快照。"""
        self.reports.append(report)
        self.status = report.status
        self.filled_quantity = report.filled_quantity
        self.broker_order_id = report.broker_order_id or self.broker_order_id
        if report.fill_price is not None and report.filled_quantity > 0:
            self.avg_fill_price = report.fill_price


@dataclass(slots=True)
class LiveOrderSubmission:
    """paper/live broker 订单提交通知。

    Boundary Behavior:
        - ``ticket`` 是 live lane 的最小正式回执，允许 broker 仅返回受理态，再由 ``reports`` / ``fills`` 继续推进；
        - ``reports`` 与 ``fills`` 可为空，但至少其一应能描述 broker 已确认的生命周期推进；
        - 同步成交型 broker 可直接在一次提交里同时返回 ``ACCEPTED``/``FILLED`` 报告与成交明细。
    """

    ticket: OrderTicket
    reports: list[ExecutionReport] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)

    @property
    def latest_report(self) -> ExecutionReport | None:
        """返回最新一条执行回报。"""
        return self.reports[-1] if self.reports else None


@dataclass(slots=True)
class PositionSnapshot:
    """持仓快照。"""

    ts_code: str
    quantity: int
    available_quantity: int
    avg_cost: float
    market_value: float
    unrealized_pnl: float


@dataclass(slots=True)
class AccountSnapshot:
    """账户快照。"""

    cash: float
    available_cash: float
    market_value: float
    total_assets: float
    pnl: float
    cum_pnl: float | None = None
    daily_pnl: float | None = None
    drawdown: float = 0.0


@dataclass(slots=True)
class RiskResult:
    """风控结果。"""

    passed: bool
    rule_name: str
    severity: str
    reason: str
    stage: str = "PRE_TRADE"




class TradeSessionStatus(str, Enum):
    """operator 交易会话状态。

    Boundary Behavior:
        - ``RUNNING`` 表示命令意图已落库且已进入 broker 提交阶段，但本地结果未必全部持久化；
        - ``RECOVERY_REQUIRED`` 表示外部 broker 侧可能已经产生副作用，但本地账本未能确认闭环，
          需要后续执行显式 reconciliation/backfill；
        - ``FAILED`` 仅表示当前会话在本地侧终止失败，不再隐含“外部一定没有任何 side effect”。
    """

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    REPLAYED = "REPLAYED"


@dataclass(slots=True)
class TradeSessionSummary:
    """operator 交易会话摘要。"""

    session_id: str
    runtime_mode: str
    broker_provider: str
    command_type: str
    command_source: str
    requested_by: str
    status: TradeSessionStatus
    idempotency_key: str | None = None
    requested_trade_date: str | None = None
    risk_summary: dict[str, Any] = field(default_factory=dict)
    order_count: int = 0
    submitted_count: int = 0
    rejected_count: int = 0
    error_message: str | None = None
    account_id: str | None = None
    broker_event_cursor: str | None = None
    last_synced_at: str | None = None
    supervisor_owner: str | None = None
    supervisor_lease_expires_at: str | None = None
    supervisor_mode: str | None = None
    last_supervised_at: str | None = None
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class TradeCommandEvent:
    """operator 命令事件。"""

    event_id: str
    session_id: str
    event_type: str
    level: str
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


@dataclass(slots=True)
class TradeSessionResult:
    """operator 命令执行结果。"""

    summary: TradeSessionSummary
    orders: list[OrderRequest] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    events: list[TradeCommandEvent] = field(default_factory=list)
    replayed: bool = False

@dataclass(slots=True)
class BacktestResult:
    """回测输出。"""

    strategy_id: str
    run_id: str = ""
    report_path: str | None = None
    benchmark_symbol: str | None = None
    trade_dates: list[date] = field(default_factory=list)
    equity_curve: list[float] = field(default_factory=list)
    benchmark_curve: list[float] = field(default_factory=list)
    order_count: int = 0
    fill_count: int = 0
    metrics: dict[str, float] = field(default_factory=dict)
    data_lineage: DataLineage = field(default_factory=DataLineage)
    artifacts: RunArtifacts = field(default_factory=RunArtifacts)
    run_events: list[dict[str, Any]] = field(default_factory=list)
    data_quality_events: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class BacktestRun:
    """回测运行记录。"""

    run_id: str
    strategy_id: str
    status: BacktestRunStatus
    config_snapshot_json: str
    started_at: str
    finished_at: str | None = None
    error_message: str | None = None
    report_path: str | None = None
    dataset_version_id: str | None = None
    import_run_id: str | None = None
    import_run_ids_json: str = "[]"
    data_source: str | None = None
    data_start_date: str | None = None
    data_end_date: str | None = None
    dataset_digest: str | None = None
    degradation_flags_json: str = "[]"
    warnings_json: str = "[]"
    entrypoint: str | None = None
    strategy_version: str | None = None
    runtime_mode: str | None = None
    report_artifacts_json: str = "[]"
    run_manifest_json: str = "{}"
    run_events_json: str = "[]"
