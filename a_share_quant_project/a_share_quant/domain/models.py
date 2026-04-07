"""领域模型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class OrderSide(str, Enum):
    """买卖方向。"""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    """订单状态。"""

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PRE_TRADE_REJECTED = "PRE_TRADE_REJECTED"
    EXECUTION_REJECTED = "EXECUTION_REJECTED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class BacktestRunStatus(str, Enum):
    """回测运行状态。"""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


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
class TargetPosition:
    """策略输出的目标仓位。"""

    ts_code: str
    target_weight: float
    score: float
    reason: str


@dataclass(slots=True)
class OrderRequest:
    """待提交订单。"""

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


@dataclass(slots=True)
class Fill:
    """成交回报。"""

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


@dataclass(slots=True)
class BacktestResult:
    """回测输出。"""

    strategy_id: str
    run_id: str = ""
    report_path: str | None = None
    benchmark_symbol: str | None = None
    trade_dates: list[date] = field(default_factory=list)
    equity_curve: list[float] = field(default_factory=list)
    order_count: int = 0
    fill_count: int = 0
    metrics: dict[str, float] = field(default_factory=dict)


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
