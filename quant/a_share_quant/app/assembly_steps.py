"""兼容入口：应用装配步骤与可复用安装器。"""
from __future__ import annotations

from a_share_quant.app.assembly_broker import build_broker, build_execution_engine
from a_share_quant.app.assembly_core import AssemblyValidationError, build_base_context, install_registries
from a_share_quant.app.assembly_registry import (
    bind_plugin_manager_to_runtime,
    bind_plugin_manager_to_workflows,
    configure_plugin_manager,
    create_plugin_manager,
    install_plugin_manager,
    register_component_manifest,
    register_providers,
    register_strategy_components,
    register_workflows,
)
from a_share_quant.app.assembly_services import (
    install_backtest_stack,
    install_data_stack,
    install_operator_trade_stack,
    install_report_stack,
    install_strategy_stack,
)

__all__ = [
    "AssemblyValidationError",
    "bind_plugin_manager_to_runtime",
    "bind_plugin_manager_to_workflows",
    "configure_plugin_manager",
    "create_plugin_manager",
    "build_base_context",
    "build_broker",
    "build_execution_engine",
    "install_backtest_stack",
    "install_data_stack",
    "install_operator_trade_stack",
    "install_plugin_manager",
    "install_registries",
    "install_report_stack",
    "install_strategy_stack",
    "register_component_manifest",
    "register_providers",
    "register_strategy_components",
    "register_workflows",
]
