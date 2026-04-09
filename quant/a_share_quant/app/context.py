"""应用上下文。"""
from __future__ import annotations

from dataclasses import dataclass

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.core.component_registry import ComponentRegistry
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.plugins.plugin_manager import PluginManager
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
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.operator_supervisor_service import OperatorSupervisorService
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService
from a_share_quant.storage.sqlite_store import SQLiteStore


@dataclass(slots=True)
class AppContext:
    """应用依赖容器。"""

    config: AppConfig
    market_repository: MarketRepository
    order_repository: OrderRepository
    account_repository: AccountRepository
    audit_repository: AuditRepository
    strategy_repository: StrategyRepository
    backtest_run_repository: BacktestRunRepository
    data_import_repository: DataImportRepository
    dataset_version_repository: DatasetVersionRepository
    research_run_repository: ResearchRunRepository
    store: SQLiteStore
    execution_session_repository: ExecutionSessionRepository | None = None
    execution_contract_service: SharedExecutionContractService | None = None
    component_registry: ComponentRegistry | None = None
    provider_registry: ComponentRegistry | None = None
    workflow_registry: ComponentRegistry | None = None
    plugin_manager: PluginManager | None = None
    broker: BrokerBase | LiveBrokerPort | None = None
    data_service: DataService | None = None
    strategy_service: StrategyService | None = None
    backtest_service: BacktestService | None = None
    report_service: ReportService | None = None
    trade_orchestrator_service: TradeOrchestratorService | None = None
    trade_reconciliation_service: TradeReconciliationService | None = None
    operator_supervisor_service: OperatorSupervisorService | None = None

    def require_execution_contract_service(self) -> SharedExecutionContractService:
        if self.execution_contract_service is None:
            raise RuntimeError("当前上下文未注入 SharedExecutionContractService；请使用完整装配入口")
        return self.execution_contract_service

    def require_broker(self) -> BrokerBase | LiveBrokerPort:
        if self.broker is None:
            raise RuntimeError("当前上下文未注入 broker；请使用完整回测/交易装配入口")
        return self.broker

    def require_data_service(self) -> DataService:
        if self.data_service is None:
            raise RuntimeError("当前上下文未注入 DataService；请使用数据装配入口")
        return self.data_service

    def require_strategy_service(self) -> StrategyService:
        if self.strategy_service is None:
            raise RuntimeError("当前上下文未注入 StrategyService；请使用回测装配入口")
        return self.strategy_service

    def require_backtest_service(self) -> BacktestService:
        if self.backtest_service is None:
            raise RuntimeError("当前上下文未注入 BacktestService；请使用回测装配入口")
        return self.backtest_service

    def require_report_service(self) -> ReportService:
        if self.report_service is None:
            raise RuntimeError("当前上下文未注入 ReportService；请使用报表装配入口")
        return self.report_service

    def require_trade_orchestrator_service(self) -> TradeOrchestratorService:
        if self.trade_orchestrator_service is None:
            raise RuntimeError("当前上下文未注入 TradeOrchestratorService；请使用 operator trade 装配入口")
        return self.trade_orchestrator_service


    def require_trade_reconciliation_service(self) -> TradeReconciliationService:
        if self.trade_reconciliation_service is None:
            raise RuntimeError("当前上下文未注入 TradeReconciliationService；请使用 operator trade 装配入口")
        return self.trade_reconciliation_service

    def require_operator_supervisor_service(self) -> OperatorSupervisorService:
        if self.operator_supervisor_service is None:
            raise RuntimeError("当前上下文未注入 OperatorSupervisorService；请使用 operator trade 装配入口")
        return self.operator_supervisor_service

    def require_provider_registry(self) -> ComponentRegistry:
        if self.provider_registry is None:
            raise RuntimeError("当前上下文未注入 provider_registry")
        return self.provider_registry

    def require_workflow_registry(self) -> ComponentRegistry:
        if self.workflow_registry is None:
            raise RuntimeError("当前上下文未注入 workflow_registry")
        return self.workflow_registry

    def require_component_registry(self) -> ComponentRegistry:
        if self.component_registry is None:
            raise RuntimeError("当前上下文未注入 component_registry")
        return self.component_registry

    def require_plugin_manager(self) -> PluginManager:
        if self.plugin_manager is None:
            raise RuntimeError("当前上下文未注入 plugin_manager")
        return self.plugin_manager

    def require_research_run_repository(self) -> ResearchRunRepository:
        if self.research_run_repository is None:
            raise RuntimeError("当前上下文未注入 ResearchRunRepository")
        return self.research_run_repository

    def close(self) -> None:
        """关闭底层资源。"""
        plugin_error: Exception | None = None
        if self.plugin_manager is not None:
            try:
                self.plugin_manager.shutdown(self)
            except Exception as exc:  # pragma: no cover
                plugin_error = exc
        broker_error: Exception | None = None
        if self.broker is not None:
            try:
                self.broker.close()
            except Exception as exc:  # pragma: no cover
                broker_error = exc
        self.store.close()
        if plugin_error is not None:
            raise plugin_error
        if broker_error is not None:
            raise broker_error

    def __enter__(self) -> AppContext:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
