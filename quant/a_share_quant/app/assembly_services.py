"""服务栈装配。"""
from __future__ import annotations

from typing import cast

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.app.assembly_broker import build_execution_engine
from a_share_quant.app.assembly_core import AssemblyValidationError
from a_share_quant.app.assembly_registry import register_providers, register_strategy_components
from a_share_quant.app.context import AppContext
from a_share_quant.core.component_registry import ComponentDescriptor
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.operator_supervisor_service import OperatorSupervisorService
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.risk_service import RiskService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService
from a_share_quant.core.events import EventBus, EventJournal
from a_share_quant.repositories.runtime_event_repository import RuntimeEventRepository


def install_data_stack(context: AppContext) -> None:
    """装配数据服务与 provider 注册表。"""
    context.data_service = DataService(
        context.market_repository,
        context.config.data,
        data_import_repository=context.data_import_repository,
        dataset_version_repository=context.dataset_version_repository,
    )
    register_providers(context)


def install_strategy_stack(context: AppContext) -> None:
    """装配策略服务与相关 runtime component。"""
    register_strategy_components(context)
    context.strategy_service = StrategyService(
        context.config,
        context.strategy_repository,
        component_registry=context.component_registry,
        research_run_repository=context.research_run_repository,
    )


def install_report_stack(context: AppContext) -> None:
    """装配报表服务。"""
    config = context.config
    context.report_service = ReportService(
        config.data.reports_dir,
        config.backtest.report_name_template,
        account_repository=context.account_repository,
        order_repository=context.order_repository,
        run_repository=context.backtest_run_repository,
        market_repository=context.market_repository,
        data_import_repository=context.data_import_repository,
        annual_trading_days=config.backtest.metrics.annual_trading_days,
        risk_free_rate=config.backtest.metrics.risk_free_rate,
    )


def install_operator_trade_stack(context: AppContext) -> None:
    """装配 paper/live 正式交易编排链。"""
    if context.broker is None:
        raise AssemblyValidationError("operator trade 上下文装配失败：缺少 broker")
    if context.config.app.runtime_mode not in {"paper_trade", "live_trade"}:
        raise AssemblyValidationError(
            f"operator trade 仅支持 paper/live lane；收到 app.runtime_mode={context.config.app.runtime_mode}"
        )
    live_broker = cast(LiveBrokerPort, context.require_broker())
    risk_engine = RiskService(context.config.risk, context.config.backtest).build_engine()
    execution_contract_service = context.require_execution_contract_service()
    context.trade_reconciliation_service = TradeReconciliationService(
        broker=live_broker,
        order_repository=context.order_repository,
        audit_repository=context.audit_repository,
        execution_session_repository=context.execution_session_repository,
        account_repository=context.account_repository,
    )
    context.trade_orchestrator_service = TradeOrchestratorService(
        config=context.config,
        broker=live_broker,
        risk_engine=risk_engine,
        market_repository=context.market_repository,
        order_repository=context.order_repository,
        audit_repository=context.audit_repository,
        execution_session_repository=context.execution_session_repository,
        reconciliation_service=context.trade_reconciliation_service,
        account_repository=context.account_repository,
        execution_contract_service=execution_contract_service,
    )
    context.operator_supervisor_service = OperatorSupervisorService(
        config=context.config,
        broker=live_broker,
        orchestrator=context.trade_orchestrator_service,
        execution_session_repository=context.execution_session_repository,
        order_repository=context.order_repository,
        audit_repository=context.audit_repository,
    )


def install_backtest_stack(context: AppContext) -> None:
    """装配正式研究回测主链。"""
    if (
        context.data_service is None
        or context.strategy_service is None
        or context.report_service is None
        or context.broker is None
        or context.component_registry is None
    ):
        raise AssemblyValidationError("回测上下文装配失败：缺少 DataService/StrategyService/ReportService/Broker/ComponentRegistry")
    config = context.config
    research_broker = cast(BrokerBase, context.require_broker())
    risk_engine = RiskService(config.risk, config.backtest).build_engine()
    portfolio_engine = PortfolioEngine(
        enforce_lot_size=config.risk.rules.enforce_lot_size,
        rebalance_mode=config.backtest.rebalance_mode,
    )
    execution_engine = build_execution_engine(config, research_broker)
    context.component_registry.register(
        "builtin.execution_engine",
        execution_engine,
        metadata={"component_type": "execution_engine", "event_mode": config.backtest.execution.event_mode},
        descriptor=ComponentDescriptor(
            name="builtin.execution_engine",
            component_type="execution_engine",
            contract_kind="runtime_instance",
            input_contract="order_request + market_bar",
            output_contract="execution_outcome",
            callable_path="a_share_quant.engines.execution_engine:ExecutionEngine.execute",
            tags=("runtime", "execution"),
            metadata={"event_mode": config.backtest.execution.event_mode},
        ),
    )
    runtime_event_repository = RuntimeEventRepository(context.store)
    event_bus = EventBus(
        journal=EventJournal(
            sink=lambda event: runtime_event_repository.append_from_event(
                event,
                source_domain="backtest",
                stream_scope="run_event",
                stream_id=event.payload.get("run_id") if isinstance(event.payload, dict) else None,
            )
        )
    )
    execution_contract_service = context.require_execution_contract_service()
    backtest_engine = BacktestEngine(
        research_broker,
        risk_engine,
        portfolio_engine,
        context.order_repository,
        context.account_repository,
        context.audit_repository,
        context.backtest_run_repository,
        store=context.store,
        initial_cash=config.backtest.initial_cash,
        annual_trading_days=config.backtest.metrics.annual_trading_days,
        risk_free_rate=config.backtest.metrics.risk_free_rate,
        slippage_bps=config.backtest.slippage_bps,
        missing_price_policy=config.backtest.valuation.missing_price_policy,
        execution_engine=execution_engine,
        event_bus=event_bus,
        execution_contract_service=execution_contract_service,
    )
    context.backtest_service = BacktestService(
        context.config,
        backtest_engine,
        context.report_service,
        context.backtest_run_repository,
        data_service=context.data_service,
    )
