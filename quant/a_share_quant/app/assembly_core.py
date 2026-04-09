"""应用装配基础层。"""
from __future__ import annotations

from a_share_quant.app.context import AppContext
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
    """构建所有命令共享的存储/repository 基础层。"""
    market_repository = MarketRepository(store)
    order_repository = OrderRepository(store)
    account_repository = AccountRepository(store)
    audit_repository = AuditRepository(store)
    strategy_repository = StrategyRepository(store)
    backtest_run_repository = BacktestRunRepository(store)
    data_import_repository = DataImportRepository(store)
    dataset_version_repository = DatasetVersionRepository(store)
    research_run_repository = ResearchRunRepository(store)
    execution_session_repository = ExecutionSessionRepository(store)
    execution_contract_service = SharedExecutionContractService()
    return AppContext(
        config=config,
        market_repository=market_repository,
        order_repository=order_repository,
        account_repository=account_repository,
        audit_repository=audit_repository,
        strategy_repository=strategy_repository,
        backtest_run_repository=backtest_run_repository,
        data_import_repository=data_import_repository,
        dataset_version_repository=dataset_version_repository,
        research_run_repository=research_run_repository,
        execution_session_repository=execution_session_repository,
        execution_contract_service=execution_contract_service,
        store=store,
    )



def install_registries(context: AppContext) -> None:
    """为上下文安装组件/Provider/Workflow 注册表。"""
    context.component_registry = ComponentRegistry()
    context.provider_registry = ComponentRegistry()
    context.workflow_registry = ComponentRegistry()
