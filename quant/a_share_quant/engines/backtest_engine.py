"""事件驱动回测引擎。"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.events import Event, EventBus, EventType
from a_share_quant.core.metrics import PerformanceMetrics, compute_metrics, compute_relative_metrics
from a_share_quant.core.utils import new_id
from a_share_quant.core.valuation import PortfolioValuator, ValuationResult
from a_share_quant.domain.models import BacktestResult, Bar, DataLineage, RunArtifacts, Security, TradingCalendarEntry
from a_share_quant.engines.backtest_runtime import (
    BenchmarkTracker,
    DayPersistenceUnit,
    ExecutionCoordinator,
    PreloadedDayBatchBuilder,
    RebalancePlanner,
    RunEventCollector,
    RunLifecycleManager,
    TradeDayCursor,
)
from a_share_quant.engines.execution_engine import ExecutionEngine
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.engines.risk_engine import RiskEngine
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.execution_service import ExecutionService
from a_share_quant.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

class BacktestEngine:
    """驱动数据、策略、风控、执行与审计全链路。"""

    def __init__(
        self,
        broker: BrokerBase,
        risk_engine: RiskEngine,
        portfolio_engine: PortfolioEngine,
        order_repository: OrderRepository,
        account_repository: AccountRepository,
        audit_repository: AuditRepository,
        run_repository: BacktestRunRepository,
        store: SQLiteStore,
        initial_cash: float,
        annual_trading_days: int = 252,
        risk_free_rate: float = 0.0,
        slippage_bps: float = 0.0,
        missing_price_policy: str = "last_known",
        event_bus: EventBus | None = None,
        execution_engine: ExecutionEngine | None = None,
        execution_contract_service: SharedExecutionContractService | None = None,
    ) -> None:
        self.broker = broker
        self.risk_engine = risk_engine
        self.portfolio_engine = portfolio_engine
        self.order_repository = order_repository
        self.account_repository = account_repository
        self.audit_repository = audit_repository
        self.run_repository = run_repository
        self.store = store
        self.initial_cash = initial_cash
        self.annual_trading_days = annual_trading_days
        self.risk_free_rate = risk_free_rate
        self.event_bus = event_bus or EventBus()
        self.event_collector = RunEventCollector()
        self.event_bus.subscribe_many(
            [
                EventType.DAY_CLOSED,
                EventType.ORDER_SUBMITTED,
                EventType.ORDER_ACCEPTED,
                EventType.ORDER_PARTIALLY_FILLED,
                EventType.ORDER_FILLED,
                EventType.ORDER_REJECTED,
                EventType.EXECUTION_REPORT,
            ],
            self.event_collector.handle,
        )
        self.execution_contract_service = execution_contract_service or SharedExecutionContractService()
        self.execution_engine = execution_engine or ExecutionEngine(broker, self.event_bus, slippage_bps=slippage_bps)
        self.execution_service = ExecutionService(self.execution_engine)
        self.valuator = PortfolioValuator(initial_cash=initial_cash, missing_price_policy=missing_price_policy)
        self.lifecycle = RunLifecycleManager(run_repository)
        self.persistence_unit = DayPersistenceUnit(store, order_repository, account_repository, audit_repository)
        self.execution_coordinator = ExecutionCoordinator(risk_engine, self.execution_service)

    def run(
        self,
        strategy,
        bars_by_symbol: dict[str, list[Bar]],
        securities: dict[str, Security],
        config_snapshot: dict | None = None,
        benchmark_symbol: str | None = None,
        trade_calendar: list[TradingCalendarEntry] | None = None,
        data_lineage: DataLineage | None = None,
        artifacts: RunArtifacts | None = None,
    ) -> BacktestResult:
        """基于预加载行情运行回测。"""
        day_batches, trade_dates = PreloadedDayBatchBuilder.build(bars_by_symbol, trade_calendar)
        return self._run_over_trade_days(
            strategy=strategy,
            day_batches=day_batches,
            trade_dates=trade_dates,
            securities=securities,
            config_snapshot=config_snapshot,
            benchmark_symbol=benchmark_symbol,
            data_lineage=data_lineage,
            artifacts=artifacts,
        )

    def run_streaming(
        self,
        strategy,
        day_batches: Iterable[tuple[date, dict[str, Bar]]],
        trade_dates: list[date],
        securities: dict[str, Security],
        config_snapshot: dict | None = None,
        benchmark_symbol: str | None = None,
        data_lineage: DataLineage | None = None,
        artifacts: RunArtifacts | None = None,
    ) -> BacktestResult:
        """基于按交易日流式输入的行情运行回测。"""
        if not trade_dates:
            raise ValueError("没有可用交易日期")
        return self._run_over_trade_days(
            strategy=strategy,
            day_batches=day_batches,
            trade_dates=trade_dates,
            securities=securities,
            config_snapshot=config_snapshot,
            benchmark_symbol=benchmark_symbol,
            data_lineage=data_lineage,
            artifacts=artifacts,
        )

    def _resolve_history_window(self, strategy) -> int:
        """解析主链应保留的最小历史窗口。

        Boundary Behavior:
            - 若策略绑定 ``_execution_runtime``，必须以 runtime 合同为准；
            - 未绑定时回退历史 ``strategy.required_history_bars()``；
            - 返回值至少为 1，禁止产生空历史窗口。
        """
        return self.execution_contract_service.required_history_bars(strategy)

    def _run_over_trade_days(
        self,
        strategy,
        day_batches: Iterable[tuple[date, dict[str, Bar]]],
        trade_dates: list[date],
        securities: dict[str, Security],
        config_snapshot: dict | None,
        benchmark_symbol: str | None,
        data_lineage: DataLineage | None,
        artifacts: RunArtifacts | None,
    ) -> BacktestResult:
        history_window = self._resolve_history_window(strategy)
        manifest = artifacts or RunArtifacts()
        if manifest.benchmark_initial_value is None:
            manifest.benchmark_initial_value = self.initial_cash
        self.event_collector.clear()
        run_id = self.lifecycle.start(strategy.strategy_id, config_snapshot, data_lineage=data_lineage, artifacts=manifest)
        trace_id = new_id("bt")
        cursor = TradeDayCursor(history_window=history_window)
        planner = RebalancePlanner(self.portfolio_engine, execution_contract_service=self.execution_contract_service)
        result = BacktestResult(
            strategy_id=strategy.strategy_id,
            run_id=run_id,
            benchmark_symbol=benchmark_symbol,
            data_lineage=data_lineage or DataLineage(),
            artifacts=manifest,
        )
        benchmark_tracker = BenchmarkTracker(self.initial_cash, benchmark_symbol)
        previous_eod_total_assets: float | None = None
        peak_total_assets = self.initial_cash
        observed_any_bar = False
        logger.info("开始回测 run_id=%s strategy_id=%s trade_days=%s", run_id, strategy.strategy_id, len(trade_dates))
        try:
            for current_date, day_bars in day_batches:
                if day_bars:
                    observed_any_bar = True
                frame = cursor.advance(current_date, day_bars, securities)
                pre_trade_valuation = self._value_portfolio(
                    current_prices=frame.current_prices,
                    last_known_prices=frame.last_known_prices,
                    previous_eod_total_assets=previous_eod_total_assets,
                    peak_total_assets=peak_total_assets,
                    include_daily_pnl=False,
                )
                positions = {item.ts_code: item for item in pre_trade_valuation.positions}
                decision = planner.plan(strategy, frame, pre_trade_valuation.account, positions)
                for order in decision.orders:
                    order.run_id = run_id
                execution = self.execution_coordinator.execute(
                    decision.orders,
                    frame,
                    positions,
                    pre_trade_valuation.account,
                    decision.target_weights,
                )
                eod_valuation = self._value_portfolio(
                    current_prices=frame.current_prices,
                    last_known_prices=frame.last_known_prices,
                    previous_eod_total_assets=previous_eod_total_assets,
                    peak_total_assets=peak_total_assets,
                    include_daily_pnl=True,
                )
                self.persistence_unit.persist(
                    run_id=run_id,
                    trace_id=trace_id,
                    strategy=strategy,
                    frame=frame,
                    decision=decision,
                    execution=execution,
                    valuation=eod_valuation,
                )
                peak_total_assets = max(peak_total_assets, eod_valuation.account.total_assets)
                previous_eod_total_assets = eod_valuation.account.total_assets
                result.trade_dates.append(current_date)
                result.equity_curve.append(eod_valuation.account.total_assets)
                benchmark_assets = benchmark_tracker.advance(frame)
                if benchmark_assets is not None:
                    result.benchmark_curve.append(benchmark_assets)
                result.order_count += len(decision.orders)
                result.fill_count += len(execution.execution_outcome.fills)
                self.event_bus.publish(Event(EventType.DAY_CLOSED, {"trade_date": current_date.isoformat(), "equity": eod_valuation.account.total_assets, "run_id": run_id}))
            if not observed_any_bar:
                raise ValueError("回测期间没有任何可用行情 bar")
            result.metrics = self._build_metrics(result.equity_curve, result.benchmark_curve)
            result.run_events = list(self.event_collector.events)
            logger.info("回测完成 run_id=%s strategy_id=%s order_count=%s fill_count=%s", run_id, strategy.strategy_id, result.order_count, result.fill_count)
            return result
        except Exception as exc:
            logger.exception("回测失败 run_id=%s strategy_id=%s error=%s", run_id, strategy.strategy_id, exc)
            self.lifecycle.fail(run_id, str(exc))
            raise

    def _value_portfolio(
        self,
        *,
        current_prices: dict[str, float],
        last_known_prices: dict[str, float],
        previous_eod_total_assets: float | None,
        peak_total_assets: float,
        include_daily_pnl: bool,
    ) -> ValuationResult:
        raw_account = self.broker.get_account({})
        raw_positions = self.broker.get_positions({})
        return self.valuator.value(
            raw_account=raw_account,
            raw_positions=raw_positions,
            current_prices=current_prices,
            last_known_prices=last_known_prices,
            previous_eod_total_assets=previous_eod_total_assets,
            peak_total_assets=peak_total_assets,
            include_daily_pnl=include_daily_pnl,
        )

    def _build_metrics(self, equity_curve: list[float], benchmark_curve: list[float]) -> dict[str, float]:
        if len(equity_curve) < 2:
            metrics = PerformanceMetrics(total_return=0.0, annual_return=0.0, max_drawdown=0.0, sharpe=0.0, volatility=0.0)
        else:
            metrics = compute_metrics(equity_curve, annual_days=self.annual_trading_days, risk_free_rate=self.risk_free_rate)
        payload = {
            "total_return": metrics.total_return,
            "annual_return": metrics.annual_return,
            "max_drawdown": metrics.max_drawdown,
            "sharpe": metrics.sharpe,
            "volatility": metrics.volatility,
        }
        if len(benchmark_curve) == len(equity_curve) and len(benchmark_curve) >= 2:
            relative = compute_relative_metrics(
                equity_curve,
                benchmark_curve,
                annual_days=self.annual_trading_days,
                risk_free_rate=self.risk_free_rate,
            )
            payload.update(
                {
                    "benchmark_total_return": relative.benchmark_total_return,
                    "benchmark_annual_return": relative.benchmark_annual_return,
                    "excess_total_return": relative.excess_total_return,
                    "tracking_error": relative.tracking_error,
                    "information_ratio": relative.information_ratio,
                    "beta": relative.beta,
                    "alpha": relative.alpha,
                }
            )
        return payload
