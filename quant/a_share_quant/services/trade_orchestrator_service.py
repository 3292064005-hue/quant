"""paper/live 正式交易编排服务。"""
from __future__ import annotations

from datetime import date

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import (
    ExecutionIntentSubmissionResult,
    ExecutionReport,
    Fill,
    OrderRequest,
    RiskResult,
    TradeCommandEvent,
    TradeSessionResult,
)
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.engines.risk_engine import RiskEngine
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.services.operator_account_capture_service import OperatorAccountCaptureService
from a_share_quant.services.operator_execution_intent_service import OperatorExecutionIntentService
from a_share_quant.services.operator_recovery_service import OperatorRecoveryService
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService
from a_share_quant.services.operator_session_progress_service import OperatorSessionProgressService
from a_share_quant.services.operator_session_query_service import OperatorSessionQueryService
from a_share_quant.services.operator_session_sync_service import OperatorSessionSyncService
from a_share_quant.services.operator_session_write_service import OperatorSessionWriteService
from a_share_quant.services.operator_submission_execution_service import OperatorSubmissionExecutionService
from a_share_quant.services.operator_submission_preparation_service import OperatorSubmissionPreparationService
from a_share_quant.services.operator_submission_service import OperatorSubmissionService
from a_share_quant.services.operator_trade_audit_service import OperatorTradeAuditService
from a_share_quant.services.operator_trade_validation_service import OperatorTradeValidationService
from a_share_quant.services.risk_service import RiskService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService
from a_share_quant.services.trade_orchestrator_use_cases import (
    OrderSubmissionUseCase,
    ResearchSignalSubmissionUseCase,
    SessionReconcileUseCase,
    SessionSyncUseCase,
)


class TradeOrchestratorService:
    """paper/live lane 正式交易编排 facade。

    Notes:
        - 当前类不再直接承载全部 submit/sync/recovery 细节；
        - 它的职责被收敛为 session coordinator：负责暴露稳定 API，并把具体动作委托给
          preparation/execution/sync/recovery 等子域服务；
        - 旧私有 helper 仍保留兼容别名，但内部实现均已下沉到对应子服务。
    """

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        risk_engine: RiskEngine | None = None,
        market_repository: MarketRepository,
        order_repository: OrderRepository,
        audit_repository: AuditRepository,
        execution_session_repository: ExecutionSessionRepository,
        reconciliation_service: TradeReconciliationService,
        account_repository: AccountRepository | None = None,
        research_run_repository: ResearchRunRepository | None = None,
        execution_contract_service: SharedExecutionContractService | None = None,
        plugin_manager=None,
        plugin_context=None,
    ) -> None:
        self.config = config
        self.broker = broker
        self.risk_engine = risk_engine or RiskService(config.risk, config.backtest).build_engine()
        self.market_repository = market_repository
        self.order_repository = order_repository
        self.audit_repository = audit_repository
        self.execution_session_repository = execution_session_repository
        self.reconciliation_service = reconciliation_service
        self.account_repository = account_repository
        self.research_run_repository = research_run_repository
        self.execution_contract_service = execution_contract_service or SharedExecutionContractService()
        self.plugin_manager = plugin_manager
        self.plugin_context = plugin_context

        self.account_capture_service = OperatorAccountCaptureService(account_repository=account_repository)
        self.submission_service = OperatorSubmissionService(broker)
        self.event_service = OperatorSessionEventService(broker_provider=config.broker.provider)
        self.query_service = OperatorSessionQueryService(order_repository)
        self.progress_service = OperatorSessionProgressService(
            event_service=self.event_service,
            plugin_manager=self.plugin_manager,
            plugin_context=self.plugin_context,
        )
        self.validation_service = OperatorTradeValidationService(
            config=config,
            broker=broker,
            risk_engine=self.risk_engine,
            market_repository=market_repository,
            execution_contract_service=self.execution_contract_service,
        )
        self.session_write_service = OperatorSessionWriteService(
            execution_session_repository=execution_session_repository,
            order_repository=order_repository,
            account_capture_service=self.account_capture_service,
        )
        self.audit_service = OperatorTradeAuditService(
            audit_repository=audit_repository,
            execution_session_repository=execution_session_repository,
            event_service=self.event_service,
        )
        self.recovery_service = OperatorRecoveryService(
            execution_session_repository=execution_session_repository,
            order_repository=order_repository,
            submission_service=self.submission_service,
            reconciliation_service=reconciliation_service,
            runtime_mode=config.app.runtime_mode,
            broker_provider=config.broker.provider,
        )
        self.preparation_service = OperatorSubmissionPreparationService(
            config=config,
            broker=broker,
            order_repository=order_repository,
            execution_session_repository=execution_session_repository,
            validation_service=self.validation_service,
            event_service=self.event_service,
            query_service=self.query_service,
            submission_service=self.submission_service,
        )
        self.execution_service = OperatorSubmissionExecutionService(
            config=config,
            broker=broker,
            submission_service=self.submission_service,
            event_service=self.event_service,
            session_write_service=self.session_write_service,
            account_capture_service=self.account_capture_service,
            recovery_service=self.recovery_service,
            audit_service=self.audit_service,
            plugin_manager=self.plugin_manager,
            plugin_context=self.plugin_context,
        )
        self.sync_service = OperatorSessionSyncService(
            config=config,
            broker=broker,
            execution_session_repository=execution_session_repository,
            order_repository=order_repository,
            query_service=self.query_service,
            progress_service=self.progress_service,
            event_service=self.event_service,
            session_write_service=self.session_write_service,
            submission_service=self.submission_service,
            submission_execution_service=self.execution_service,
            audit_service=self.audit_service,
        )
        self.execution_intent_service = None
        if self.research_run_repository is not None:
            self.execution_intent_service = OperatorExecutionIntentService(
                config=config,
                broker=broker,
                market_repository=market_repository,
                research_run_repository=self.research_run_repository,
                portfolio_engine=PortfolioEngine(
                    enforce_lot_size=config.risk.rules.enforce_lot_size,
                    rebalance_mode=config.backtest.rebalance_mode,
                ),
            )
        self.order_submission_use_case = OrderSubmissionUseCase(self)
        self.research_signal_submission_use_case = ResearchSignalSubmissionUseCase(self)
        self.session_reconcile_use_case = SessionReconcileUseCase(self)
        self.session_sync_use_case = SessionSyncUseCase(self)

    def bind_plugin_manager(self, plugin_manager, plugin_context=None) -> None:
        """在正式装配完成后回填 plugin manager 到 orchestrator 及其子服务。"""
        self.plugin_manager = plugin_manager
        if plugin_context is not None:
            self.plugin_context = plugin_context
        self.progress_service.bind_plugin_manager(plugin_manager, plugin_context=self.plugin_context)
        self.execution_service.bind_plugin_manager(plugin_manager, plugin_context=self.plugin_context)
        self.sync_service.bind_plugin_manager(plugin_manager, plugin_context=self.plugin_context)

    def submit_orders(
        self,
        orders: list[OrderRequest],
        *,
        command_source: str,
        command_type: str = "submit_orders",
        requested_by: str | None = None,
        idempotency_key: str | None = None,
        approved: bool = False,
        account_id: str | None = None,
    ) -> TradeSessionResult:
        return self.order_submission_use_case.execute(
            orders,
            command_source=command_source,
            command_type=command_type,
            requested_by=requested_by,
            idempotency_key=idempotency_key,
            approved=approved,
            account_id=account_id,
        )

    def submit_research_signal(
        self,
        *,
        research_run_id: str,
        trade_date: date | None = None,
        command_source: str,
        requested_by: str | None = None,
        idempotency_key: str | None = None,
        approved: bool = False,
        account_id: str | None = None,
        strategy_id: str | None = None,
    ) -> ExecutionIntentSubmissionResult:
        return self.research_signal_submission_use_case.execute(
            research_run_id=research_run_id,
            trade_date=trade_date,
            command_source=command_source,
            requested_by=requested_by,
            idempotency_key=idempotency_key,
            approved=approved,
            account_id=account_id,
            strategy_id=strategy_id,
        )

    def reconcile_session(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.session_reconcile_use_case.reconcile_session(session_id, requested_by=requested_by)

    def reconcile_latest_recovery_required(self, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.session_reconcile_use_case.reconcile_latest_recovery_required(requested_by=requested_by)

    def sync_session_events(
        self,
        session_id: str,
        *,
        requested_by: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        return self.session_sync_use_case.sync_session_events(
            session_id,
            requested_by=requested_by,
            supervisor_mode=supervisor_mode,
        )

    def advance_session_from_reports(
        self,
        session_id: str,
        *,
        reports: list[ExecutionReport],
        external_fills: list[Fill],
        requested_by: str | None = None,
        source: str = "poll",
        broker_event_cursor: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        return self.sync_service.advance_session_from_reports(
            session_id,
            reports=reports,
            external_fills=external_fills,
            requested_by=requested_by,
            source=source,
            broker_event_cursor=broker_event_cursor,
            supervisor_mode=supervisor_mode,
        )

    def sync_latest_open_session(self, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.session_sync_use_case.sync_latest_open_session(requested_by=requested_by)

    @staticmethod
    def _now_iso() -> str:
        from a_share_quant.core.utils import now_iso
        return now_iso()

    def _resolve_account_id(self, account_id: str | None) -> str | None:
        return self.preparation_service.resolve_account_id(account_id)

    def _apply_polled_progress(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        reports: list[ExecutionReport],
        external_fills: list[Fill],
    ):
        return self.sync_service.apply_polled_progress(
            session_id,
            orders=orders,
            reports=reports,
            external_fills=external_fills,
        )

    @staticmethod
    def _apply_submission_order_payload(order: OrderRequest, payload: dict) -> None:
        OperatorSubmissionExecutionService.apply_submission_order_payload(order, payload)

    def _attempt_recovery(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        submitted_orders: list[OrderRequest],
        fills: list[Fill],
        requested_by: str,
        terminal_message: str,
    ) -> TradeSessionResult | None:
        return self.recovery_service.attempt_recovery(
            session_id,
            orders=orders,
            submitted_orders=submitted_orders,
            fills=fills,
            requested_by=requested_by,
            terminal_message=terminal_message,
        )

    def _persist_order_intents(self, session_id: str, orders: list[OrderRequest], rejected_orders: list[OrderRequest], *, persist: bool = True):
        events = list(self.event_service.build_order_intent_events(session_id, orders, rejected_orders))
        if persist:
            self.execution_session_repository.append_events(events)
        return events

    def _record_submission_events(self, session_id: str, order: OrderRequest, submission, *, sequence: int):
        return self.event_service.build_submission_events(session_id, order, submission, sequence=sequence)

    def _order_to_event_payload(self, order: OrderRequest):
        return self.event_service.order_to_event_payload(order)

    def _normalize_operator_order_ids(self, orders: list[OrderRequest]) -> None:
        self.preparation_service.normalize_operator_order_ids(orders)

    def _resolve_trade_date(self, orders: list[OrderRequest]):
        return self.validation_service.resolve_trade_date(orders)

    def _pre_trade_validate(self, orders: list[OrderRequest]):
        return self.validation_service.pre_trade_validate(orders)

    def _load_trade_date_bars(self, *, trade_date: date, ts_codes: list[str]):
        return self.validation_service._load_trade_date_bars(trade_date=trade_date, ts_codes=ts_codes)

    @staticmethod
    def _resolve_rejection_reason(results: list[RiskResult]) -> str | None:
        return OperatorTradeValidationService.resolve_rejection_reason(results)

    def _resolve_session_status(self, orders: list[OrderRequest]):
        return self.execution_service.resolve_session_status(orders)

    def list_session_orders(self, session_id: str) -> list[OrderRequest]:
        return self.query_service.list_session_orders(session_id)

    def list_session_fills(self, session_id: str) -> list[Fill]:
        return self.query_service.list_session_fills(session_id)

    def _list_session_orders(self, session_id: str) -> list[OrderRequest]:
        return self.query_service.list_session_orders(session_id)

    def _list_session_fills(self, session_id: str) -> list[Fill]:
        return self.query_service.list_session_fills(session_id)

    @staticmethod
    def derive_broker_event_cursor(previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        return OperatorSessionSyncService.derive_broker_event_cursor(previous_cursor, reports)

    def _derive_broker_event_cursor(self, previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        return self.sync_service.derive_broker_event_cursor(previous_cursor, reports)

    def _write_trade_session_audit_best_effort(
        self,
        *,
        action: str,
        entity_id: str,
        payload: dict,
        operator: str,
        level: str,
        session_id: str | None = None,
        lifecycle_events: list[TradeCommandEvent] | None = None,
    ) -> None:
        self.audit_service.write_best_effort(
            action=action,
            entity_id=entity_id,
            payload=payload,
            operator=operator,
            level=level,
            session_id=session_id,
            lifecycle_events=lifecycle_events,
        )
