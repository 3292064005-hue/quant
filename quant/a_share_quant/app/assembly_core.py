"""应用装配基础层。"""
from __future__ import annotations

from a_share_quant.app.context import AppContext, PersistenceContext
from a_share_quant.core.component_registry import ComponentRegistry
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.dataset_version_repository import DatasetVersionRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


class AssemblyValidationError(ValueError):
    """装配前校验失败。"""



def build_base_context(config, store: SQLiteStore) -> AppContext:
    """构建所有命令共享的持久化基础层。"""
    persistence = PersistenceContext(
        market_repository=MarketRepository(store),
        order_repository=OrderRepository(store),
        account_repository=AccountRepository(store),
        audit_repository=AuditRepository(store),
        strategy_repository=StrategyRepository(store),
        backtest_run_repository=BacktestRunRepository(store),
        data_import_repository=DataImportRepository(store),
        dataset_version_repository=DatasetVersionRepository(store),
        research_run_repository=ResearchRunRepository(store),
        execution_session_repository=ExecutionSessionRepository(store),
        execution_contract_service=SharedExecutionContractService(),
        store=store,
    )
    return AppContext(config=config, persistence=persistence)



def install_registries(context: AppContext) -> None:
    """为上下文安装组件/Provider/Workflow 注册表。"""
    registry_context = context.require_registry_context()
    registry_context.component_registry = ComponentRegistry()
    registry_context.provider_registry = ComponentRegistry()
    registry_context.workflow_registry = ComponentRegistry()
