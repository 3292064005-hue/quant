"""应用上下文与分层运行时能力视图。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
class PersistenceContext:
    """共享持久化依赖层。

    该层只承载 repository / store / contract service，不包含 runtime 行为对象，
    作为 research/operator 两条主链的共同底座。
    """

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


@dataclass(slots=True)
class RegistryContext:
    """注册表与插件基础设施层。"""

    component_registry: ComponentRegistry | None = None
    provider_registry: ComponentRegistry | None = None
    workflow_registry: ComponentRegistry | None = None
    plugin_manager: PluginManager | None = None


@dataclass(slots=True)
class ResearchRuntimeContext:
    """research/backtest 主链能力层。"""

    data_service: DataService | None = None
    strategy_service: StrategyService | None = None
    backtest_service: BacktestService | None = None
    report_service: ReportService | None = None


@dataclass(slots=True)
class OperatorRuntimeContext:
    """paper/live operator 主链能力层。"""

    broker: BrokerBase | LiveBrokerPort | None = None
    trade_orchestrator_service: TradeOrchestratorService | None = None
    trade_reconciliation_service: TradeReconciliationService | None = None
    operator_supervisor_service: OperatorSupervisorService | None = None




@dataclass(slots=True)
class ResearchCapabilityView:
    """research/backtest CLI 与 workflow 可消费的窄能力视图。"""

    config: AppConfig
    registries: RegistryContext
    runtime: ResearchRuntimeContext
    persistence: PersistenceContext

    def require_data_service(self) -> DataService:
        if self.runtime.data_service is None:
            raise RuntimeError("当前上下文未注入 DataService；请使用数据装配入口")
        return self.runtime.data_service

    def require_workflow_registry(self) -> ComponentRegistry:
        if self.registries.workflow_registry is None:
            raise RuntimeError("当前上下文未注入 workflow_registry")
        return self.registries.workflow_registry


@dataclass(slots=True)
class OperatorCapabilityView:
    """paper/live operator CLI 可消费的窄能力视图。"""

    config: AppConfig
    registries: RegistryContext
    runtime: OperatorRuntimeContext
    persistence: PersistenceContext

    def require_broker(self) -> BrokerBase | LiveBrokerPort:
        if self.runtime.broker is None:
            raise RuntimeError("当前上下文未注入 broker；请使用完整回测/交易装配入口")
        return self.runtime.broker

    def require_workflow_registry(self) -> ComponentRegistry:
        if self.registries.workflow_registry is None:
            raise RuntimeError("当前上下文未注入 workflow_registry")
        return self.registries.workflow_registry

@dataclass(slots=True)
class AppContext:
    """应用依赖容器。

    Notes:
        - 通过 ``persistence`` / ``registries`` / ``research_runtime`` / ``operator_runtime``
          把历史“大一统”上下文拆成显式分层；
        - 仍保留旧字段名兼容属性，避免现有 bootstrap / workflow / CLI 调用方一次性全部断裂；
        - 新代码应优先通过 ``require_*_context`` 进入对应能力层，而不是把 ``AppContext`` 当作巨型 service locator。
    """

    config: AppConfig
    persistence: PersistenceContext
    registries: RegistryContext = field(default_factory=RegistryContext)
    research_runtime: ResearchRuntimeContext = field(default_factory=ResearchRuntimeContext)
    operator_runtime: OperatorRuntimeContext = field(default_factory=OperatorRuntimeContext)

    def require_persistence_context(self) -> PersistenceContext:
        return self.persistence

    def require_registry_context(self) -> RegistryContext:
        return self.registries

    def require_research_runtime_context(self) -> ResearchRuntimeContext:
        return self.research_runtime

    def require_operator_runtime_context(self) -> OperatorRuntimeContext:
        return self.operator_runtime

    def research_capabilities(self) -> ResearchCapabilityView:
        return ResearchCapabilityView(config=self.config, registries=self.registries, runtime=self.research_runtime, persistence=self.persistence)

    def operator_capabilities(self) -> OperatorCapabilityView:
        return OperatorCapabilityView(config=self.config, registries=self.registries, runtime=self.operator_runtime, persistence=self.persistence)

    @property
    def market_repository(self) -> MarketRepository:
        return self.persistence.market_repository

    @market_repository.setter
    def market_repository(self, value: MarketRepository) -> None:
        self.persistence.market_repository = value

    @property
    def order_repository(self) -> OrderRepository:
        return self.persistence.order_repository

    @order_repository.setter
    def order_repository(self, value: OrderRepository) -> None:
        self.persistence.order_repository = value

    @property
    def account_repository(self) -> AccountRepository:
        return self.persistence.account_repository

    @account_repository.setter
    def account_repository(self, value: AccountRepository) -> None:
        self.persistence.account_repository = value

    @property
    def audit_repository(self) -> AuditRepository:
        return self.persistence.audit_repository

    @audit_repository.setter
    def audit_repository(self, value: AuditRepository) -> None:
        self.persistence.audit_repository = value

    @property
    def strategy_repository(self) -> StrategyRepository:
        return self.persistence.strategy_repository

    @strategy_repository.setter
    def strategy_repository(self, value: StrategyRepository) -> None:
        self.persistence.strategy_repository = value

    @property
    def backtest_run_repository(self) -> BacktestRunRepository:
        return self.persistence.backtest_run_repository

    @backtest_run_repository.setter
    def backtest_run_repository(self, value: BacktestRunRepository) -> None:
        self.persistence.backtest_run_repository = value

    @property
    def data_import_repository(self) -> DataImportRepository:
        return self.persistence.data_import_repository

    @data_import_repository.setter
    def data_import_repository(self, value: DataImportRepository) -> None:
        self.persistence.data_import_repository = value

    @property
    def dataset_version_repository(self) -> DatasetVersionRepository:
        return self.persistence.dataset_version_repository

    @dataset_version_repository.setter
    def dataset_version_repository(self, value: DatasetVersionRepository) -> None:
        self.persistence.dataset_version_repository = value

    @property
    def research_run_repository(self) -> ResearchRunRepository:
        return self.persistence.research_run_repository

    @research_run_repository.setter
    def research_run_repository(self, value: ResearchRunRepository) -> None:
        self.persistence.research_run_repository = value

    @property
    def store(self) -> SQLiteStore:
        return self.persistence.store

    @store.setter
    def store(self, value: SQLiteStore) -> None:
        self.persistence.store = value

    @property
    def execution_session_repository(self) -> ExecutionSessionRepository | None:
        return self.persistence.execution_session_repository

    @execution_session_repository.setter
    def execution_session_repository(self, value: ExecutionSessionRepository | None) -> None:
        self.persistence.execution_session_repository = value

    @property
    def execution_contract_service(self) -> SharedExecutionContractService | None:
        return self.persistence.execution_contract_service

    @execution_contract_service.setter
    def execution_contract_service(self, value: SharedExecutionContractService | None) -> None:
        self.persistence.execution_contract_service = value

    @property
    def component_registry(self) -> ComponentRegistry | None:
        return self.registries.component_registry

    @component_registry.setter
    def component_registry(self, value: ComponentRegistry | None) -> None:
        self.registries.component_registry = value

    @property
    def provider_registry(self) -> ComponentRegistry | None:
        return self.registries.provider_registry

    @provider_registry.setter
    def provider_registry(self, value: ComponentRegistry | None) -> None:
        self.registries.provider_registry = value

    @property
    def workflow_registry(self) -> ComponentRegistry | None:
        return self.registries.workflow_registry

    @workflow_registry.setter
    def workflow_registry(self, value: ComponentRegistry | None) -> None:
        self.registries.workflow_registry = value

    @property
    def plugin_manager(self) -> PluginManager | None:
        return self.registries.plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value: PluginManager | None) -> None:
        self.registries.plugin_manager = value

    @property
    def broker(self) -> BrokerBase | LiveBrokerPort | None:
        return self.operator_runtime.broker

    @broker.setter
    def broker(self, value: BrokerBase | LiveBrokerPort | None) -> None:
        self.operator_runtime.broker = value

    @property
    def data_service(self) -> DataService | None:
        return self.research_runtime.data_service

    @data_service.setter
    def data_service(self, value: DataService | None) -> None:
        self.research_runtime.data_service = value

    @property
    def strategy_service(self) -> StrategyService | None:
        return self.research_runtime.strategy_service

    @strategy_service.setter
    def strategy_service(self, value: StrategyService | None) -> None:
        self.research_runtime.strategy_service = value

    @property
    def backtest_service(self) -> BacktestService | None:
        return self.research_runtime.backtest_service

    @backtest_service.setter
    def backtest_service(self, value: BacktestService | None) -> None:
        self.research_runtime.backtest_service = value

    @property
    def report_service(self) -> ReportService | None:
        return self.research_runtime.report_service

    @report_service.setter
    def report_service(self, value: ReportService | None) -> None:
        self.research_runtime.report_service = value

    @property
    def trade_orchestrator_service(self) -> TradeOrchestratorService | None:
        return self.operator_runtime.trade_orchestrator_service

    @trade_orchestrator_service.setter
    def trade_orchestrator_service(self, value: TradeOrchestratorService | None) -> None:
        self.operator_runtime.trade_orchestrator_service = value

    @property
    def trade_reconciliation_service(self) -> TradeReconciliationService | None:
        return self.operator_runtime.trade_reconciliation_service

    @trade_reconciliation_service.setter
    def trade_reconciliation_service(self, value: TradeReconciliationService | None) -> None:
        self.operator_runtime.trade_reconciliation_service = value

    @property
    def operator_supervisor_service(self) -> OperatorSupervisorService | None:
        return self.operator_runtime.operator_supervisor_service

    @operator_supervisor_service.setter
    def operator_supervisor_service(self, value: OperatorSupervisorService | None) -> None:
        self.operator_runtime.operator_supervisor_service = value

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
        """关闭底层资源。

        Boundary Behavior:
            - plugin shutdown 先于 broker close，避免插件在 broker 关闭后丢失收尾上下文；
            - broker close 先于 store close，避免关闭数据库前仍有 broker 事件需要写入；
            - 关闭顺序错误会优先抛出 plugin 异常，其次 broker 异常，store 始终最后关闭。
        """
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
