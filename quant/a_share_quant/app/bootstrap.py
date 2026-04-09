"""应用启动组装。"""
from __future__ import annotations

from typing import Any

from a_share_quant.app.assembly_steps import AssemblyValidationError
from a_share_quant.app.context import AppContext
from a_share_quant.app.runtime_assembly import AssemblyRequest, resolve_runtime_assembly
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.logging_utils import configure_logging
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.storage.sqlite_store import SQLiteStore


def bootstrap(
    config_path: str,
    broker_clients: dict[str, Any] | None = None,
    *,
    broker_client_factory: str | None = None,
) -> AppContext:
    """构建完整研究回测上下文。"""
    return _build_context(
        config_path,
        request=AssemblyRequest(
            include_data_service=True,
            include_strategy_service=True,
            include_report_service=True,
            include_backtest_service=True,
            include_broker=True,
        ),
        broker_clients=broker_clients,
        broker_client_factory=broker_client_factory,
    )



def bootstrap_storage_context(config_path: str) -> AppContext:
    """仅构建存储与 repository 上下文。"""
    return _build_context(config_path, request=AssemblyRequest())



def bootstrap_data_context(config_path: str) -> AppContext:
    """构建数据导入/同步所需上下文，不注入 broker。"""
    return _build_context(config_path, request=AssemblyRequest(include_data_service=True))



def bootstrap_report_context(config_path: str) -> AppContext:
    """构建报表重建所需上下文，不注入 broker。"""
    return _build_context(config_path, request=AssemblyRequest(include_report_service=True))



def bootstrap_operator_context(
    config_path: str,
    broker_clients: dict[str, Any] | None = None,
    *,
    broker_client_factory: str | None = None,
) -> AppContext:
    """构建 paper/live operator 所需上下文。

    Notes:
        - 该入口不会装配 ``BacktestService``；
        - 会装配 ``DataService`` / ``ReportService`` / broker，便于真实运行 lane 的读路径、运行时检查
          与 operator plane 演进。
    """
    return _build_context(
        config_path,
        request=AssemblyRequest(include_data_service=True, include_report_service=True, include_broker=True, require_operator_lane=True),
        broker_clients=broker_clients,
        broker_client_factory=broker_client_factory,
    )



def bootstrap_trade_operator_context(
    config_path: str,
    broker_clients: dict[str, Any] | None = None,
    *,
    broker_client_factory: str | None = None,
) -> AppContext:
    """构建 paper/live 正式 operator trade 写路径上下文。"""
    return _build_context(
        config_path,
        request=AssemblyRequest(
            include_data_service=True,
            include_report_service=True,
            include_broker=True,
            require_operator_lane=True,
            include_trade_orchestrator=True,
        ),
        broker_clients=broker_clients,
        broker_client_factory=broker_client_factory,
    )



def _build_context(
    config_path: str,
    *,
    request: AssemblyRequest,
    broker_clients: dict[str, Any] | None = None,
    broker_client_factory: str | None = None,
) -> AppContext:
    """按最小依赖与 runtime lane 装配上下文。"""
    config = ConfigLoader.load(config_path)
    configure_logging(config.app.logs_dir)
    store = SQLiteStore(config.database.path)
    context: AppContext | None = None
    try:
        store.init_schema(load_schema_sql())
        assembly = resolve_runtime_assembly(config.app.runtime_mode)
        context = assembly.build(
            config=config,
            store=store,
            request=request,
            broker_clients=broker_clients,
            broker_client_factory=broker_client_factory,
        )
        return context
    except Exception:
        if context is not None:
            context.close()
        else:
            store.close()
        raise


__all__ = [
    "AssemblyRequest",
    "AssemblyValidationError",
    "bootstrap",
    "bootstrap_data_context",
    "bootstrap_operator_context",
    "bootstrap_trade_operator_context",
    "bootstrap_report_context",
    "bootstrap_storage_context",
]
