"""组件、provider 与 workflow 注册。"""
from __future__ import annotations

from typing import Any

from a_share_quant.app.context import AppContext
from a_share_quant.app.plugin_loader import build_plugin_manager
from a_share_quant.app.runtime_lane import RuntimeLaneProfile
from a_share_quant.core.component_registry import ComponentDescriptor
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.providers import BarProvider, CalendarProvider, DatasetProvider, FeatureProvider, InstrumentProvider
from a_share_quant.strategies.runtime_components import (
    AllActiveAShareUniverse,
    BypassedPortfolioComponent,
    DirectTargetsSignal,
    EqualWeightTopNPortfolio,
    MomentumFactorComponent,
    NoFactorComponent,
    ResearchSignalSnapshotComponent,
    TopNSelectionSignal,
)
from a_share_quant.workflows import BacktestWorkflow, OperatorTradeWorkflow, ReplayWorkflow, ReportWorkflow, ResearchWorkflow


def create_plugin_manager(context: AppContext) -> None:
    """按配置构造并注册 plugin manager，但暂不执行 configure。

    Notes:
        - plugin manager 需要在 service/workflow 构造前进入上下文，避免 plugin-aware service 在初始化时拿到 ``None``；
        - 具体 ``configure_all`` 延后到服务、provider、workflow 全部装配完成后执行。
    """
    if context.plugin_manager is None:
        context.plugin_manager = build_plugin_manager(context.config)



def configure_plugin_manager(context: AppContext) -> None:
    """在正式装配完成后执行插件 configure/context_ready。"""
    if context.plugin_manager is None:
        return
    context.plugin_manager.configure_all(context)



def install_plugin_manager(context: AppContext) -> None:
    """兼容旧入口：完整安装 plugin manager。"""
    create_plugin_manager(context)
    bind_plugin_manager_to_runtime(context)
    configure_plugin_manager(context)



def bind_plugin_manager_to_runtime(context: AppContext) -> None:
    """把 plugin manager 回填到 service/workflow 等 plugin-aware 运行时对象。

    Boundary Behavior:
        - 允许在 service/workflow 已构造后执行回填，避免官方装配路径因为顺序漂移导致 hook 失效；
        - 仅对显式声明 ``bind_plugin_manager`` 的对象执行绑定，不对普通组件做隐式魔法注入。
    """
    plugin_manager = context.plugin_manager
    if plugin_manager is None:
        return
    targets: list[Any] = []
    for candidate in (
        context.strategy_service,
        context.trade_orchestrator_service,
        context.operator_supervisor_service,
    ):
        if candidate is not None:
            targets.append(candidate)
    if context.workflow_registry is not None:
        for entry in context.workflow_registry.list_entries():
            targets.append(entry.component)
    for target in targets:
        binder = getattr(target, "bind_plugin_manager", None)
        if callable(binder):
            binder(plugin_manager)



def bind_plugin_manager_to_workflows(context: AppContext) -> None:
    """兼容旧命名；当前已升级为完整 runtime 绑定。"""
    bind_plugin_manager_to_runtime(context)



def register_providers(context: AppContext) -> None:
    """注册 provider 组件。"""
    if context.provider_registry is None or context.data_service is None:
        return
    feature_provider = FeatureProvider()
    dataset_provider = DatasetProvider(context.data_service)
    context.provider_registry.register(
        "provider.calendar",
        CalendarProvider(context.market_repository),
        metadata={"provider_type": "calendar", "provides": ["trade_calendar"]},
        descriptor=ComponentDescriptor(
            name="provider.calendar",
            component_type="provider",
            contract_kind="runtime_instance",
            input_contract="calendar_query",
            output_contract="trade_calendar",
            callable_path="a_share_quant.providers.calendar_provider:CalendarProvider",
            tags=("provider", "calendar"),
        ),
    )
    context.provider_registry.register(
        "provider.instrument",
        InstrumentProvider(context.market_repository),
        metadata={"provider_type": "instrument", "provides": ["security_master"]},
        descriptor=ComponentDescriptor(
            name="provider.instrument",
            component_type="provider",
            contract_kind="runtime_instance",
            input_contract="instrument_query",
            output_contract="security_master",
            callable_path="a_share_quant.providers.instrument_provider:InstrumentProvider",
            tags=("provider", "instrument"),
        ),
    )
    context.provider_registry.register(
        "provider.bar",
        BarProvider(context.market_repository),
        metadata={"provider_type": "bar", "provides": ["daily_bar"]},
        descriptor=ComponentDescriptor(
            name="provider.bar",
            component_type="provider",
            contract_kind="runtime_instance",
            input_contract="bar_query",
            output_contract="bars",
            callable_path="a_share_quant.providers.bar_provider:BarProvider",
            tags=("provider", "market_data"),
        ),
    )
    context.provider_registry.register(
        "provider.feature",
        feature_provider,
        metadata={
            "provider_type": "feature",
            "provides": [spec.name for spec in feature_provider.describe_features()],
        },
        descriptor=ComponentDescriptor(
            name="provider.feature",
            component_type="provider",
            contract_kind="runtime_instance",
            input_contract="feature_request",
            output_contract="feature_values",
            callable_path="a_share_quant.providers.feature_provider:FeatureProvider.compute_feature_batch",
            tags=("provider", "feature"),
        ),
    )
    context.provider_registry.register(
        "provider.dataset",
        dataset_provider,
        metadata={"provider_type": "dataset", "provides": ["snapshot", "stream", "summary"]},
        descriptor=ComponentDescriptor(
            name="provider.dataset",
            component_type="provider",
            contract_kind="runtime_instance",
            input_contract="dataset_request",
            output_contract="dataset_snapshot|dataset_summary",
            callable_path="a_share_quant.providers.dataset_provider:DatasetProvider.load_snapshot",
            tags=("provider", "dataset"),
        ),
    )



def register_strategy_components(context: AppContext) -> None:
    """注册策略/组合相关基础组件。"""
    if context.component_registry is None:
        return
    if context.component_registry.contains("builtin.portfolio_engine"):
        return
    context.component_registry.register(
        "builtin.portfolio_engine",
        PortfolioEngine,
        metadata={"component_type": "portfolio_engine_cls", "contract_kind": "executable"},
        descriptor=ComponentDescriptor(
            name="builtin.portfolio_engine",
            component_type="portfolio_engine_cls",
            contract_kind="executable",
            input_contract="target_positions + holdings",
            output_contract="order_requests",
            callable_path="a_share_quant.engines.portfolio_engine:PortfolioEngine.generate_orders",
            tags=("strategy", "portfolio"),
        ),
    )
    register_component_manifest(
        context,
        name="builtin.all_active_a_share",
        component=AllActiveAShareUniverse(),
        component_type="universe",
        input_contract="trade_day_frame",
        output_contract="eligible_histories + eligible_securities",
        contract_kind="runtime_instance",
        tags=("strategy", "universe"),
        callable_path="a_share_quant.strategies.runtime_components:AllActiveAShareUniverse.select",
        metadata={"selection_mode": "all_active", "market_scope": "a_share"},
    )
    register_component_manifest(
        context,
        name="builtin.top_n_selection",
        component=TopNSelectionSignal(),
        component_type="signal",
        input_contract="factor_values",
        output_contract="ranked_symbols",
        contract_kind="runtime_instance",
        tags=("strategy", "signal"),
        callable_path="a_share_quant.strategies.runtime_components:TopNSelectionSignal.select",
        metadata={"selection_mode": "rank_desc", "source": "factor_component"},
    )
    register_component_manifest(
        context,
        name="builtin.equal_weight_top_n",
        component=EqualWeightTopNPortfolio(),
        component_type="portfolio_construction",
        input_contract="selected_symbols",
        output_contract="target_positions",
        contract_kind="runtime_instance",
        tags=("strategy", "portfolio"),
        callable_path="a_share_quant.strategies.runtime_components:EqualWeightTopNPortfolio.build_targets",
        metadata={"construction_mode": "equal_weight_top_n"},
    )
    register_component_manifest(
        context,
        name="builtin.bypassed_portfolio",
        component=BypassedPortfolioComponent(),
        component_type="portfolio_construction",
        input_contract="target_positions",
        output_contract="target_positions",
        contract_kind="runtime_instance",
        tags=("strategy", "portfolio", "bypass"),
        callable_path="a_share_quant.strategies.runtime_components:BypassedPortfolioComponent.build_targets",
        metadata={"construction_mode": "bypassed"},
    )
    register_component_manifest(
        context,
        name="builtin.close_fill_mock",
        component={"execution_mode": "mock_close_fill"},
        component_type="execution_policy",
        input_contract="order_requests + daily_bar",
        output_contract="fills",
        contract_kind="declarative",
        tags=("execution",),
        metadata={"execution_mode": "mock_close_fill"},
    )
    register_component_manifest(
        context,
        name="builtin.pre_trade_risk",
        component={"risk_mode": "pre_trade_rules"},
        component_type="risk_gate",
        input_contract="order_requests + account_state",
        output_contract="accepted_orders|rejections",
        contract_kind="declarative",
        tags=("risk",),
        metadata={"risk_mode": "pre_trade_rules"},
    )
    register_component_manifest(
        context,
        name="builtin.daily_close_relative",
        component={"benchmark_mode": "daily_close_relative"},
        component_type="benchmark",
        input_contract="equity_curve + benchmark_bar",
        output_contract="benchmark_metrics",
        contract_kind="declarative",
        tags=("report", "benchmark"),
        metadata={"benchmark_mode": "daily_close_relative"},
    )
    register_component_manifest(
        context,
        name="builtin.momentum",
        component=MomentumFactorComponent(),
        component_type="factor",
        input_contract="bars_by_symbol + lookback",
        output_contract="factor_values",
        contract_kind="runtime_instance",
        tags=("factor", "momentum"),
        callable_path="a_share_quant.strategies.runtime_components:MomentumFactorComponent.compute",
        metadata={"feature_name": "momentum"},
    )
    register_component_manifest(
        context,
        name="builtin.none",
        component=NoFactorComponent(),
        component_type="factor",
        input_contract="none",
        output_contract="none",
        contract_kind="runtime_instance",
        tags=("factor", "none"),
        callable_path="a_share_quant.strategies.runtime_components:NoFactorComponent.compute",
        metadata={"feature_name": "none"},
    )
    register_component_manifest(
        context,
        name="builtin.direct_targets",
        component=DirectTargetsSignal(),
        component_type="signal",
        input_contract="strategy_targets",
        output_contract="target_positions",
        contract_kind="runtime_instance",
        tags=("strategy", "signal"),
        callable_path="a_share_quant.strategies.runtime_components:DirectTargetsSignal.build_targets",
        metadata={"signal_mode": "strategy_targets_direct"},
    )
    register_component_manifest(
        context,
        name="research.signal_snapshot",
        component=ResearchSignalSnapshotComponent(),
        component_type="signal",
        input_contract="research_run.signal_snapshot",
        output_contract="target_positions",
        contract_kind="runtime_instance",
        tags=("strategy", "signal", "research"),
        callable_path="a_share_quant.strategies.runtime_components:ResearchSignalSnapshotComponent.build_targets",
        metadata={"signal_mode": "research_signal_snapshot"},
    )



def register_component_manifest(
    context: AppContext,
    *,
    name: str,
    component: Any,
    component_type: str,
    input_contract: str,
    output_contract: str,
    contract_kind: str,
    tags: tuple[str, ...],
    callable_path: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """把组件对象与 descriptor 一并注册到 component registry。"""
    if context.component_registry is None:
        return
    merged_metadata = {"component_type": component_type, "contract_kind": contract_kind, **dict(metadata or {})}
    context.component_registry.register(
        name,
        component,
        metadata=merged_metadata,
        descriptor=ComponentDescriptor(
            name=name,
            component_type=component_type,
            contract_kind=contract_kind,
            input_contract=input_contract,
            output_contract=output_contract,
            callable_path=callable_path,
            tags=tags,
            metadata=dict(metadata or {}),
        ),
    )



def register_workflows(context: AppContext, profile: RuntimeLaneProfile) -> None:
    """按当前可用服务与 runtime lane 能力矩阵注册 workflow。"""
    if context.workflow_registry is None:
        return
    plugin_manager = context.plugin_manager
    if context.backtest_service is not None:
        context.workflow_registry.register(
            "workflow.backtest",
            BacktestWorkflow(context.backtest_service, context, plugin_manager=plugin_manager),
            metadata={"workflow_type": "backtest", "input_contract": "strategy + data", "output_contract": "backtest_result"},
            descriptor=ComponentDescriptor(
                name="workflow.backtest",
                component_type="workflow",
                contract_kind="runtime_instance",
                input_contract="strategy + data",
                output_contract="backtest_result",
                callable_path="a_share_quant.workflows.backtest_workflow:BacktestWorkflow.run_default",
                tags=("workflow", "backtest"),
            ),
        )
    if context.report_service is not None and profile.supports_report_rebuild:
        context.workflow_registry.register(
            "workflow.report",
            ReportWorkflow(context.report_service, context, plugin_manager=plugin_manager),
            metadata={"workflow_type": "report", "input_contract": "run_id", "output_contract": "report_path"},
            descriptor=ComponentDescriptor(
                name="workflow.report",
                component_type="workflow",
                contract_kind="runtime_instance",
                input_contract="run_id",
                output_contract="report_path",
                callable_path="a_share_quant.workflows.report_workflow:ReportWorkflow.rebuild",
                tags=("workflow", "report"),
            ),
        )
        context.workflow_registry.register(
            "workflow.replay",
            ReplayWorkflow(context.backtest_run_repository, context.report_service, context, plugin_manager=plugin_manager),
            metadata={"workflow_type": "replay", "input_contract": "latest_run", "output_contract": "run_summary|report_path"},
            descriptor=ComponentDescriptor(
                name="workflow.replay",
                component_type="workflow",
                contract_kind="runtime_instance",
                input_contract="latest_run|run_id",
                output_contract="run_summary|report_path",
                callable_path="a_share_quant.workflows.replay_workflow:ReplayWorkflow.summarize_latest",
                tags=("workflow", "replay"),
            ),
        )
    if context.trade_orchestrator_service is not None and profile.supports_operator_commands:
        context.workflow_registry.register(
            "workflow.operator_trade",
            OperatorTradeWorkflow(
                context.trade_orchestrator_service,
                context,
                supervisor_service=context.operator_supervisor_service,
                plugin_manager=plugin_manager,
            ),
            metadata={"workflow_type": "operator_trade", "input_contract": "order_requests|execution_intent", "output_contract": "trade_session_result|execution_intent_submission"},
            descriptor=ComponentDescriptor(
                name="workflow.operator_trade",
                component_type="workflow",
                contract_kind="runtime_instance",
                input_contract="order_requests|execution_intent",
                output_contract="trade_session_result|execution_intent_submission",
                callable_path="a_share_quant.workflows.operator_trade_workflow:OperatorTradeWorkflow.submit_orders",
                tags=("workflow", "operator", "trade"),
            ),
        )
    if not profile.supports_research_workflow or context.provider_registry is None:
        return
    try:
        dataset_provider = context.provider_registry.get("provider.dataset")
        feature_provider = context.provider_registry.get("provider.feature")
    except KeyError:
        return
    context.workflow_registry.register(
        "workflow.research",
        ResearchWorkflow(dataset_provider, feature_provider, context.research_run_repository, context, plugin_manager=plugin_manager),
        metadata={
            "workflow_type": "research",
            "input_contract": "dataset_request + feature_request",
            "output_contract": "dataset_summary|feature_snapshot|signal_snapshot|experiment_summary",
        },
        descriptor=ComponentDescriptor(
            name="workflow.research",
            component_type="workflow",
            contract_kind="runtime_instance",
            input_contract="dataset_request + feature_request",
            output_contract="dataset_summary|feature_snapshot|signal_snapshot|experiment_summary",
            callable_path="a_share_quant.workflows.research_workflow:ResearchWorkflow.summarize_experiment",
            tags=("workflow", "research"),
        ),
    )
