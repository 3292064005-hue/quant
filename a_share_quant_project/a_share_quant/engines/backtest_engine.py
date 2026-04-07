"""事件驱动回测引擎。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.events import Event, EventBus
from a_share_quant.core.metrics import PerformanceMetrics, compute_metrics
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, Bar, RiskResult, Security, TargetPosition
from a_share_quant.engines.execution_engine import ExecutionEngine
from a_share_quant.engines.portfolio_engine import PortfolioContext, PortfolioEngine
from a_share_quant.engines.risk_engine import RiskEngine
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.order_repository import OrderRepository


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
        annual_trading_days: int = 252,
        risk_free_rate: float = 0.0,
        slippage_bps: float = 0.0,
        event_bus: EventBus | None = None,
    ) -> None:
        self.broker = broker
        self.risk_engine = risk_engine
        self.portfolio_engine = portfolio_engine
        self.order_repository = order_repository
        self.account_repository = account_repository
        self.audit_repository = audit_repository
        self.run_repository = run_repository
        self.annual_trading_days = annual_trading_days
        self.risk_free_rate = risk_free_rate
        self.event_bus = event_bus or EventBus()
        self.execution_engine = ExecutionEngine(broker, self.event_bus, slippage_bps=slippage_bps)

    def run(
        self,
        strategy,
        bars_by_symbol: dict[str, list[Bar]],
        securities: dict[str, Security],
        config_snapshot: dict | None = None,
        benchmark_symbol: str | None = None,
    ) -> BacktestResult:
        """运行回测。

        Args:
            strategy: 实现了策略接口的对象。
            bars_by_symbol: 全量行情，按证券分组且按日期升序。
            securities: 证券信息映射。
            config_snapshot: 当前生效配置快照，用于运行审计与复现。
            benchmark_symbol: 基准代码，仅用于报告记录。

        Returns:
            `BacktestResult`。

        Raises:
            ValueError: 当无可用交易日期时抛出。
            Exception: 任意未处理运行异常会在写入失败状态后继续向上抛出。
        """
        history_by_symbol: dict[str, list[Bar]] = defaultdict(list)
        trade_dates = sorted({bar.trade_date for bars in bars_by_symbol.values() for bar in bars})
        if not trade_dates:
            raise ValueError("没有可用行情数据")
        bars_by_date: dict[date, dict[str, Bar]] = defaultdict(dict)
        for ts_code, bars in bars_by_symbol.items():
            for bar in bars:
                bars_by_date[bar.trade_date][ts_code] = bar
        run_id = new_id("run")
        trace_id = new_id("bt")
        self.run_repository.create_run(run_id, strategy.strategy_id, config_snapshot or {})
        result = BacktestResult(strategy_id=strategy.strategy_id, run_id=run_id, benchmark_symbol=benchmark_symbol)
        eligible_trade_index = 0
        try:
            for current_date in trade_dates:
                day_bars = bars_by_date[current_date]
                for ts_code, bar in day_bars.items():
                    history_by_symbol[ts_code].append(bar)
                active_securities = self._select_active_securities(current_date, securities, day_bars)
                active_history = {code: history_by_symbol[code] for code in active_securities if code in history_by_symbol}
                last_prices = {code: bar.close for code, bar in day_bars.items()}
                account = self.broker.get_account(last_prices)
                positions_list = self.broker.get_positions(last_prices)
                positions = {item.ts_code: item for item in positions_list}
                has_strategy_input = any(len(bars) >= strategy.required_history_bars() for bars in active_history.values())
                should_rebalance = has_strategy_input and strategy.should_rebalance(eligible_trade_index)
                targets: list[TargetPosition] = []
                if has_strategy_input:
                    if should_rebalance:
                        targets = strategy.generate_targets(active_history, current_date, active_securities)
                    eligible_trade_index += 1
                target_weights = {item.ts_code: item.target_weight for item in targets}
                portfolio_context = PortfolioContext(
                    strategy_id=strategy.strategy_id,
                    trade_date=current_date,
                    account=account,
                    positions=positions,
                    bars=day_bars,
                    securities=active_securities,
                )
                orders = self.portfolio_engine.generate_orders(targets, portfolio_context) if should_rebalance else []
                for order in orders:
                    order.run_id = run_id
                accepted_orders, audit = self.risk_engine.validate_orders(orders, active_securities, day_bars, positions, account, target_weights)
                execution_outcome = self.execution_engine.execute(accepted_orders, day_bars, current_date)
                last_prices = {code: bar.close for code, bar in day_bars.items()}
                account = self.broker.get_account(last_prices)
                positions_list = self.broker.get_positions(last_prices)
                self.order_repository.save_orders(run_id, orders)
                self.order_repository.save_fills(run_id, execution_outcome.fills)
                self.account_repository.save_account_snapshot(run_id, current_date, account)
                self.account_repository.save_position_snapshots(run_id, current_date, positions_list)
                self.audit_repository.write(
                    run_id,
                    trace_id,
                    "backtest",
                    "targets_generated" if should_rebalance else "rebalance_skipped",
                    "strategy",
                    strategy.strategy_id,
                    {
                        "date": current_date.isoformat(),
                        "targets": [asdict(item) for item in targets],
                        "eligible_trade_index": eligible_trade_index,
                    },
                )
                for order in orders:
                    self.audit_repository.write(
                        run_id,
                        trace_id,
                        "risk",
                        "order_evaluated",
                        "order",
                        order.order_id,
                        {"results": [asdict(item) for item in audit.get(order.order_id, [])], "status": order.status.value},
                    )
                for order_id, reason in execution_outcome.rejected.items():
                    self.audit_repository.write(
                        run_id,
                        trace_id,
                        "execution",
                        "order_rejected",
                        "order",
                        order_id,
                        {"reason": reason},
                        level="ERROR",
                    )
                result.trade_dates.append(current_date)
                result.equity_curve.append(account.total_assets)
                result.order_count += len(orders)
                result.fill_count += len(execution_outcome.fills)
                self.event_bus.publish(Event("DAY_CLOSED", {"trade_date": current_date.isoformat(), "equity": account.total_assets, "run_id": run_id}))
            result.metrics = self._build_metrics(result.equity_curve)
            return result
        except Exception as exc:
            self.run_repository.finish_run(run_id, BacktestRunStatus.FAILED, error_message=str(exc))
            raise

    def _select_active_securities(self, current_date: date, securities: dict[str, Security], day_bars: dict[str, Bar]) -> dict[str, Security]:
        """按当前交易日过滤历史有效证券池。"""
        active: dict[str, Security] = {}
        for ts_code in day_bars:
            security = securities.get(ts_code)
            if security is None:
                continue
            if security.is_active_on(current_date):
                active[ts_code] = security
        return active

    def _build_metrics(self, equity_curve: list[float]) -> dict[str, float]:
        if len(equity_curve) < 2:
            metrics = PerformanceMetrics(total_return=0.0, annual_return=0.0, max_drawdown=0.0, sharpe=0.0, volatility=0.0)
        else:
            metrics = compute_metrics(equity_curve, annual_days=self.annual_trading_days, risk_free_rate=self.risk_free_rate)
        return {
            "total_return": metrics.total_return,
            "annual_return": metrics.annual_return,
            "max_drawdown": metrics.max_drawdown,
            "sharpe": metrics.sharpe,
            "volatility": metrics.volatility,
        }
