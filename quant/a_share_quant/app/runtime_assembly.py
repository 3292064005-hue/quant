"""按 runtime lane 装配上下文。"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from a_share_quant.app.assembly_steps import (
    AssemblyValidationError,
    bind_plugin_manager_to_runtime,
    build_base_context,
    build_broker,
    configure_plugin_manager,
    create_plugin_manager,
    install_backtest_stack,
    install_data_stack,
    install_operator_trade_stack,
    install_registries,
    install_report_stack,
    install_strategy_stack,
    register_strategy_components,
    register_workflows,
)
from a_share_quant.app.context import AppContext
from a_share_quant.app.runtime_lane import RuntimeLane, RuntimeLaneProfile, get_runtime_profile
from a_share_quant.storage.sqlite_store import SQLiteStore


@dataclass(slots=True, frozen=True)
class AssemblyRequest:
    """一次上下文装配请求。"""

    include_data_service: bool = False
    include_strategy_service: bool = False
    include_report_service: bool = False
    include_backtest_service: bool = False
    include_broker: bool = False
    require_operator_lane: bool = False
    include_trade_orchestrator: bool = False


@dataclass(slots=True, frozen=True)
class AssemblyPlanStep:
    """显式装配步骤。

    Notes:
        - 通过命名步骤把组合根从“散落的 if/else”收敛为正式安装计划；
        - 计划本身可以被测试与审计读取，从而验证某条 lane 到底装配了哪些能力。
    """

    name: str
    installer: Callable[[AppContext], None]


class RuntimeAssembly:
    """基于 runtime lane 的正式装配器。"""

    def __init__(self, profile: RuntimeLaneProfile) -> None:
        self.profile = profile

    def build(
        self,
        *,
        config,
        store: SQLiteStore,
        request: AssemblyRequest,
        broker_clients: dict[str, Any] | None = None,
        broker_client_factory: str | None = None,
    ) -> AppContext:
        """根据装配请求构建上下文。"""
        self._validate_request(config, request)
        context = build_base_context(config, store)
        for step in self._resolve_pre_broker_plan(request):
            step.installer(context)

        if request.include_broker:
            context.broker = build_broker(
                config,
                broker_clients=broker_clients,
                broker_client_factory=broker_client_factory,
            )
            context.broker.connect()

        for step in self._resolve_post_broker_plan(request):
            step.installer(context)
        register_workflows(context, self.profile)
        bind_plugin_manager_to_runtime(context)
        configure_plugin_manager(context)
        return context

    def describe_plan(self, request: AssemblyRequest) -> list[str]:
        """返回该请求的正式装配步骤名称。"""
        names = [step.name for step in self._resolve_pre_broker_plan(request)]
        if request.include_broker:
            names.append("install_broker")
        names.extend(step.name for step in self._resolve_post_broker_plan(request))
        names.extend(["register_workflows", "bind_plugin_manager_to_runtime", "configure_plugin_manager"])
        return names

    def _resolve_pre_broker_plan(self, request: AssemblyRequest) -> list[AssemblyPlanStep]:
        plan = [
            AssemblyPlanStep("install_registries", install_registries),
            AssemblyPlanStep("create_plugin_manager", create_plugin_manager),
        ]
        if request.include_data_service or request.include_backtest_service or request.include_trade_orchestrator:
            plan.append(AssemblyPlanStep("install_data_stack", install_data_stack))
        if request.include_data_service or request.include_strategy_service or request.include_backtest_service:
            plan.append(AssemblyPlanStep("register_strategy_components", register_strategy_components))
        if request.include_strategy_service or request.include_backtest_service:
            plan.append(AssemblyPlanStep("install_strategy_stack", install_strategy_stack))
        if request.include_report_service or request.include_backtest_service or request.include_trade_orchestrator:
            plan.append(AssemblyPlanStep("install_report_stack", install_report_stack))
        return plan

    @staticmethod
    def _resolve_post_broker_plan(request: AssemblyRequest) -> list[AssemblyPlanStep]:
        plan: list[AssemblyPlanStep] = []
        if request.include_backtest_service:
            plan.append(AssemblyPlanStep("install_backtest_stack", install_backtest_stack))
        if request.include_trade_orchestrator:
            plan.append(AssemblyPlanStep("install_operator_trade_stack", install_operator_trade_stack))
        return plan

    def _validate_request(self, config, request: AssemblyRequest) -> None:
        runtime_lane = get_runtime_profile(config.app.runtime_mode)
        if runtime_lane.lane != self.profile.lane:
            raise AssemblyValidationError(
                f"装配器与配置 runtime_mode 不匹配: assembly={self.profile.lane.value}, config={config.app.runtime_mode}"
            )
        if request.include_backtest_service and not self.profile.supports_backtest:
            raise AssemblyValidationError(
                f"当前 runtime_mode={config.app.runtime_mode} 不支持 BacktestService；请改用 research_backtest"
            )
        if request.include_report_service and not self.profile.supports_report_rebuild:
            raise AssemblyValidationError(
                f"当前 runtime_mode={config.app.runtime_mode} 不支持报表重建 workflow；请调整 runtime lane 配置"
            )
        if request.include_strategy_service and not self.profile.supports_research_read:
            raise AssemblyValidationError(
                f"当前 runtime_mode={config.app.runtime_mode} 不支持策略/研究 workflow；请调整 runtime lane 配置"
            )
        if request.require_operator_lane and not self.profile.supports_operator_broker:
            raise AssemblyValidationError(
                f"当前 runtime_mode={config.app.runtime_mode} 不是 operator broker lane；bootstrap_operator_context 仅支持 paper/live"
            )
        if request.include_trade_orchestrator and not self.profile.supports_operator_commands:
            raise AssemblyValidationError(
                f"当前 runtime_mode={config.app.runtime_mode} 不支持 operator command workflow；请切换到 paper_trade/live_trade"
            )
        if request.include_broker and not self.profile.allow_mock_broker and config.broker.provider.lower() == "mock":
            raise AssemblyValidationError(
                f"app.runtime_mode={config.app.runtime_mode} 时不允许使用 mock broker；请切回 research_backtest 或配置真实 broker"
            )


class ResearchBacktestAssembly(RuntimeAssembly):
    def __init__(self) -> None:
        super().__init__(get_runtime_profile(RuntimeLane.RESEARCH_BACKTEST))


class PaperTradeAssembly(RuntimeAssembly):
    def __init__(self) -> None:
        super().__init__(get_runtime_profile(RuntimeLane.PAPER_TRADE))


class LiveTradeAssembly(RuntimeAssembly):
    def __init__(self) -> None:
        super().__init__(get_runtime_profile(RuntimeLane.LIVE_TRADE))


_ASSEMBLIES: dict[RuntimeLane, RuntimeAssembly] = {
    RuntimeLane.RESEARCH_BACKTEST: ResearchBacktestAssembly(),
    RuntimeLane.PAPER_TRADE: PaperTradeAssembly(),
    RuntimeLane.LIVE_TRADE: LiveTradeAssembly(),
}



def resolve_runtime_assembly(runtime_mode: str) -> RuntimeAssembly:
    """根据配置解析正式装配器。"""
    profile = get_runtime_profile(runtime_mode)
    return _ASSEMBLIES[profile.lane]
