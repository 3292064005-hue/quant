"""按 runtime lane 装配上下文。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from a_share_quant.app.assembly_steps import (
    AssemblyValidationError,
    build_base_context,
    build_broker,
    install_backtest_stack,
    install_data_stack,
    install_operator_trade_stack,
    install_plugin_manager,
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
        install_registries(context)

        if request.include_data_service or request.include_backtest_service or request.include_trade_orchestrator:
            install_data_stack(context)

        if request.include_data_service or request.include_strategy_service or request.include_backtest_service:
            register_strategy_components(context)

        if request.include_strategy_service or request.include_backtest_service:
            install_strategy_stack(context)

        if request.include_report_service or request.include_backtest_service or request.include_trade_orchestrator:
            install_report_stack(context)

        if request.include_broker:
            context.broker = build_broker(
                config,
                broker_clients=broker_clients,
                broker_client_factory=broker_client_factory,
            )
            context.broker.connect()

        if request.include_backtest_service:
            install_backtest_stack(context)
        if request.include_trade_orchestrator:
            install_operator_trade_stack(context)

        register_workflows(context, self.profile)
        install_plugin_manager(context)
        return context

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
