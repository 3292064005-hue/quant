"""回测运行期协作组件。"""
from __future__ import annotations

import heapq
import logging
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import date

from a_share_quant.core.audit_actions import BacktestAuditAction
from a_share_quant.core.events import Event
from a_share_quant.core.utils import new_id
from a_share_quant.core.valuation import ValuationResult
from a_share_quant.domain.models import (
    BacktestRunStatus,
    Bar,
    DataLineage,
    OrderRequest,
    PositionSnapshot,
    RiskResult,
    RunArtifacts,
    Security,
    TargetPosition,
    TradingCalendarEntry,
)
from a_share_quant.engines.execution_engine import ExecutionOutcome
from a_share_quant.engines.portfolio_engine import PortfolioContext, PortfolioEngine
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.engines.risk_engine import RiskEngine
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.execution_service import ExecutionService
from a_share_quant.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TradeDayFrame:
    """单个交易日推进后的中间状态。"""

    trade_date: date
    day_bars: dict[str, Bar]
    current_prices: dict[str, float]
    last_known_prices: dict[str, float]
    active_securities: dict[str, Security]
    active_history: dict[str, list[Bar]]


@dataclass(slots=True)
class RebalanceDecision:
    """组合规划阶段输出。"""

    has_strategy_input: bool
    should_rebalance: bool
    eligible_trade_index: int
    targets: list[TargetPosition] = field(default_factory=list)
    orders: list = field(default_factory=list)
    target_weights: dict[str, float] = field(default_factory=dict)


class BenchmarkTracker:
    """跟踪 benchmark 资产曲线。"""

    def __init__(self, initial_cash: float, benchmark_symbol: str | None) -> None:
        self.initial_cash = initial_cash
        self.benchmark_symbol = benchmark_symbol
        self._base_price: float | None = None
        self._last_assets = initial_cash

    def advance(self, frame: TradeDayFrame) -> float | None:
        if not self.benchmark_symbol:
            return None
        bar = frame.day_bars.get(self.benchmark_symbol)
        if bar is None or bar.close <= 0:
            return self._last_assets if self._base_price is not None else None
        if self._base_price is None:
            self._base_price = bar.close
        self._last_assets = self.initial_cash * (bar.close / self._base_price)
        return self._last_assets


class RunEventCollector:
    """消费回测主链事件，避免 EventBus 只有生产没有消费。"""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def clear(self) -> None:
        """清空上一轮回测遗留事件。"""
        self.events.clear()

    def handle(self, event: Event) -> None:
        """记录事件。

        Args:
            event: 进程内事件总线发布的事件。

        Returns:
            None。

        Boundary Behavior:
            - 仅做轻量收集，不在此处执行业务副作用；
            - payload 会复制一份，避免外部后续原地修改污染审计结果。
        """
        self.events.append({"type": event.event_type, "payload": dict(event.payload)})


class RunLifecycleManager:
    """管理回测运行记录生命周期。"""

    def __init__(self, run_repository: BacktestRunRepository) -> None:
        self.run_repository = run_repository

    def start(
        self,
        strategy_id: str,
        config_snapshot: dict | None,
        *,
        data_lineage: DataLineage | None = None,
        artifacts: RunArtifacts | None = None,
    ) -> str:
        run_id = new_id("run")
        self.run_repository.create_run(
            run_id,
            strategy_id,
            config_snapshot or {},
            data_lineage=data_lineage,
            artifacts=artifacts,
        )
        return run_id

    def fail(self, run_id: str, error_message: str) -> None:
        self.run_repository.finish_run(run_id, BacktestRunStatus.FAILED, error_message=error_message)


class TradeDayCursor:
    """维护交易日推进、历史窗口与活跃证券池。"""

    def __init__(self, history_window: int) -> None:
        self.history_window = max(history_window, 1)
        self.history_by_symbol: dict[str, list[Bar]] = defaultdict(list)
        self.last_known_prices: dict[str, float] = {}

    def advance(self, current_date: date, day_bars: dict[str, Bar], securities: dict[str, Security]) -> TradeDayFrame:
        """推进到一个新交易日。

        Args:
            current_date: 当前交易日。
            day_bars: 当日行情。
            securities: 全量证券池。

        Returns:
            ``TradeDayFrame``，包含估值、策略与执行所需的当日视图。
        """
        current_prices = {code: bar.close for code, bar in day_bars.items()}
        self.last_known_prices.update({code: price for code, price in current_prices.items() if price > 0})
        for ts_code, bar in day_bars.items():
            symbol_history = self.history_by_symbol[ts_code]
            symbol_history.append(bar)
            if len(symbol_history) > self.history_window:
                del symbol_history[:-self.history_window]
        active_securities = self._select_active_securities(current_date, securities, day_bars)
        active_history = {code: self.history_by_symbol[code] for code in active_securities if code in self.history_by_symbol}
        return TradeDayFrame(
            trade_date=current_date,
            day_bars=day_bars,
            current_prices=current_prices,
            last_known_prices=dict(self.last_known_prices),
            active_securities=active_securities,
            active_history=active_history,
        )

    @staticmethod
    def _select_active_securities(current_date: date, securities: dict[str, Security], day_bars: dict[str, Bar]) -> dict[str, Security]:
        active: dict[str, Security] = {}
        for ts_code in day_bars:
            security = securities.get(ts_code)
            if security is None:
                continue
            if security.is_active_on(current_date):
                active[ts_code] = security
        return active


class RebalancePlanner:
    """负责策略触发与订单生成。"""

    def __init__(self, portfolio_engine: PortfolioEngine, execution_contract_service: SharedExecutionContractService | None = None) -> None:
        self.portfolio_engine = portfolio_engine
        self.execution_contract_service = execution_contract_service or SharedExecutionContractService()
        self.eligible_trade_index = 0

    def plan(self, strategy, frame: TradeDayFrame, account, positions: dict[str, PositionSnapshot]) -> RebalanceDecision:
        """生成当前交易日的调仓决策。

        Boundary Behavior:
            - 若策略已绑定 ``_execution_runtime``，则正式走组件执行合同；
            - 未绑定时保留历史 ``required_history_bars / should_rebalance / generate_targets`` 直调路径；
            - 任一活跃证券达到最小历史要求即视为策略输入已就绪。
        """
        required_history = self.execution_contract_service.required_history_bars(strategy)
        has_strategy_input = any(len(bars) >= required_history for bars in frame.active_history.values())
        should_rebalance = has_strategy_input and self.execution_contract_service.should_rebalance(strategy, eligible_trade_index=self.eligible_trade_index)
        targets: list[TargetPosition] = []
        if has_strategy_input:
            if should_rebalance:
                targets = self.execution_contract_service.generate_targets(strategy, frame)
            self.eligible_trade_index += 1
        target_weights = {item.ts_code: item.target_weight for item in targets}
        portfolio_context = PortfolioContext(
            strategy_id=strategy.strategy_id,
            trade_date=frame.trade_date,
            account=account,
            positions=positions,
            bars=frame.day_bars,
            securities=frame.active_securities,
        )
        orders = self.portfolio_engine.generate_orders(targets, portfolio_context) if should_rebalance else []
        return RebalanceDecision(
            has_strategy_input=has_strategy_input,
            should_rebalance=should_rebalance,
            eligible_trade_index=self.eligible_trade_index,
            targets=targets,
            orders=orders,
            target_weights=target_weights,
        )



@dataclass(slots=True)
class CoordinatedExecution:
    """风控与执行协调结果。"""

    accepted_orders: list[OrderRequest]
    audit_results: dict[str, list[RiskResult]]
    execution_outcome: ExecutionOutcome


class ExecutionCoordinator:
    """串联风控与执行层。"""

    def __init__(self, risk_engine: RiskEngine, execution_service: ExecutionService) -> None:
        self.risk_engine = risk_engine
        self.execution_service = execution_service

    def execute(
        self,
        orders: list[OrderRequest],
        frame: TradeDayFrame,
        positions: dict[str, PositionSnapshot],
        account,
        target_weights: dict[str, float],
    ) -> CoordinatedExecution:
        accepted_orders, audit = self.risk_engine.validate_orders(
            orders,
            frame.active_securities,
            frame.day_bars,
            positions,
            account,
            target_weights,
        )
        execution_outcome = self.execution_service.execute(accepted_orders, frame.day_bars, frame.trade_date)
        return CoordinatedExecution(accepted_orders=accepted_orders, audit_results=audit, execution_outcome=execution_outcome)


class DayPersistenceUnit:
    """负责单交易日持久化与审计写入。"""

    def __init__(
        self,
        store: SQLiteStore,
        order_repository: OrderRepository,
        account_repository: AccountRepository,
        audit_repository: AuditRepository,
    ) -> None:
        self.store = store
        self.order_repository = order_repository
        self.account_repository = account_repository
        self.audit_repository = audit_repository

    def persist(
        self,
        *,
        run_id: str,
        trace_id: str,
        strategy,
        frame: TradeDayFrame,
        decision: RebalanceDecision,
        execution: CoordinatedExecution,
        valuation: ValuationResult,
    ) -> None:
        """在单个事务中持久化当日数据与审计。

        Raises:
            Exception: 任一仓储写入失败时抛出，并回滚订单、成交、账户、持仓与审计的当日写入。
        """
        with self.store.transaction():
            self.order_repository.save_orders(run_id, decision.orders)
            self.order_repository.save_fills(run_id, execution.execution_outcome.fills)
            self.account_repository.save_account_snapshot(run_id, frame.trade_date, valuation.account)
            self.account_repository.save_position_snapshots(run_id, frame.trade_date, valuation.positions)
            self.audit_repository.write(
                run_id,
                trace_id,
                "backtest",
                (BacktestAuditAction.TARGETS_GENERATED if decision.should_rebalance else BacktestAuditAction.REBALANCE_SKIPPED).value,
                "strategy",
                strategy.strategy_id,
                {
                    "date": frame.trade_date.isoformat(),
                    "targets": [asdict(item) for item in decision.targets],
                    "eligible_trade_index": decision.eligible_trade_index,
                },
            )
            self._write_valuation_audit_if_needed(run_id, trace_id, frame.trade_date, valuation)
            for order in decision.orders:
                self.audit_repository.write(
                    run_id,
                    trace_id,
                    "risk",
                    BacktestAuditAction.ORDER_EVALUATED.value,
                    "order",
                    order.order_id,
                    {"results": [asdict(item) for item in execution.audit_results.get(order.order_id, [])], "status": order.status.value},
                )
            for order_id, reason in execution.execution_outcome.rejected.items():
                self.audit_repository.write(
                    run_id,
                    trace_id,
                    "execution",
                    BacktestAuditAction.ORDER_REJECTED.value,
                    "order",
                    order_id,
                    {"reason": reason},
                    level="ERROR",
                )

    def _write_valuation_audit_if_needed(self, run_id: str, trace_id: str, current_date: date, valuation: ValuationResult) -> None:
        if not (valuation.stale_quotes or valuation.fallback_quotes or valuation.missing_quotes):
            return
        self.audit_repository.write(
            run_id,
            trace_id,
            "valuation",
            BacktestAuditAction.QUOTE_DEGRADED.value,
            "portfolio",
            current_date.isoformat(),
            {
                "trade_date": current_date.isoformat(),
                "stale_quotes": valuation.stale_quotes,
                "fallback_quotes": valuation.fallback_quotes,
                "missing_quotes": valuation.missing_quotes,
                "price_sources": {code: mark.source for code, mark in valuation.price_marks.items()},
            },
            level="WARNING",
        )


class PreloadedDayBatchBuilder:
    """在不重复物化 ``bars_by_date`` 的前提下构造日级迭代器。"""

    @staticmethod
    def build(
        bars_by_symbol: dict[str, list[Bar]],
        trade_calendar: list[TradingCalendarEntry] | None,
    ) -> tuple[Iterator[tuple[date, dict[str, Bar]]], list[date]]:
        trade_dates_from_bars = {bar.trade_date for bars in bars_by_symbol.values() for bar in bars}
        if trade_calendar:
            calendar_trade_dates = {item.cal_date for item in trade_calendar if item.is_open}
            trade_dates = sorted(trade_dates_from_bars | calendar_trade_dates)
        else:
            trade_dates = sorted(trade_dates_from_bars)
        if not trade_dates:
            raise ValueError("没有可用行情数据")

        def iterator() -> Iterator[tuple[date, dict[str, Bar]]]:
            heap: list[tuple[date, str, int]] = []
            for ts_code, bars in bars_by_symbol.items():
                if bars:
                    heapq.heappush(heap, (bars[0].trade_date, ts_code, 0))
            for trade_date in trade_dates:
                day_bars: dict[str, Bar] = {}
                while heap and heap[0][0] <= trade_date:
                    bar_date, ts_code, index = heapq.heappop(heap)
                    bar = bars_by_symbol[ts_code][index]
                    if bar_date == trade_date:
                        day_bars[ts_code] = bar
                    next_index = index + 1
                    if next_index < len(bars_by_symbol[ts_code]):
                        next_bar = bars_by_symbol[ts_code][next_index]
                        heapq.heappush(heap, (next_bar.trade_date, ts_code, next_index))
                yield trade_date, day_bars

        return iterator(), trade_dates


