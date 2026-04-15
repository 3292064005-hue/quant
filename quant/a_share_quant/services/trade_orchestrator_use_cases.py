"""trade orchestrator use-case 拆分。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import ExecutionIntentSubmissionResult, OrderRequest, TradeSessionResult


class OrderSubmissionUseCase:
    def __init__(self, orchestrator) -> None:
        self.orchestrator = orchestrator

    def execute(
        self,
        orders: list[OrderRequest],
        *,
        command_source: str,
        command_type: str,
        requested_by: str | None,
        idempotency_key: str | None,
        approved: bool,
        account_id: str | None,
    ) -> TradeSessionResult:
        existing = self.orchestrator.preparation_service.resolve_idempotent_session(idempotency_key)
        if existing is not None:
            return existing
        prepared = self.orchestrator.preparation_service.prepare_submission(
            orders,
            command_source=command_source,
            command_type=command_type,
            requested_by=requested_by,
            idempotency_key=idempotency_key,
            approved=approved,
            account_id=account_id,
            plugin_manager=self.orchestrator.plugin_manager,
            plugin_context=self.orchestrator.plugin_context,
            now_iso_value=self.orchestrator._now_iso(),
        )
        return self.orchestrator.execution_service.execute_submit(prepared)


class ResearchSignalSubmissionUseCase:
    def __init__(self, orchestrator) -> None:
        self.orchestrator = orchestrator

    def execute(
        self,
        *,
        research_run_id: str,
        trade_date: date | None,
        command_source: str,
        requested_by: str | None,
        idempotency_key: str | None,
        approved: bool,
        account_id: str | None,
        strategy_id: str | None,
    ) -> ExecutionIntentSubmissionResult:
        if self.orchestrator.execution_intent_service is None:
            raise RuntimeError("当前上下文未装配 OperatorExecutionIntentService；请使用完整 operator trade 装配入口")
        if not str(research_run_id).strip():
            raise ValueError("operator signal 提交必须显式提供 research_run_id，禁止隐式消费最近一次 signal_snapshot")
        plan = self.orchestrator.execution_intent_service.build_research_signal_plan(
            research_run_id=research_run_id,
            trade_date=trade_date,
            account_id=account_id,
            strategy_id=strategy_id,
        )
        if not plan.orders:
            raise ValueError(
                f"research signal {plan.intent.source_run_id or research_run_id} 在 trade_date={plan.intent.trade_date.isoformat()} 没有可执行订单"
            )
        trade_session = self.orchestrator.submit_orders(
            list(plan.orders),
            command_source=command_source,
            command_type="submit_execution_intent",
            requested_by=requested_by,
            idempotency_key=idempotency_key,
            approved=approved,
            account_id=plan.intent.account_id,
        )
        return ExecutionIntentSubmissionResult(plan=plan, trade_session=trade_session)


class SessionReconcileUseCase:
    def __init__(self, orchestrator) -> None:
        self.orchestrator = orchestrator

    def reconcile_session(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.orchestrator.reconciliation_service.reconcile_session(
            session_id,
            requested_by=(requested_by or self.orchestrator.config.operator.default_requested_by),
        )

    def reconcile_latest_recovery_required(self, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.orchestrator.reconciliation_service.reconcile_latest_recovery_required(
            requested_by=(requested_by or self.orchestrator.config.operator.default_requested_by),
        )


class SessionSyncUseCase:
    def __init__(self, orchestrator) -> None:
        self.orchestrator = orchestrator

    def sync_session_events(
        self,
        session_id: str,
        *,
        requested_by: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        return self.orchestrator.sync_service.sync_session_events(
            session_id,
            requested_by=requested_by,
            supervisor_mode=supervisor_mode,
        )

    def sync_latest_open_session(self, *, requested_by: str | None = None) -> TradeSessionResult:
        return self.orchestrator.sync_service.sync_latest_open_session(requested_by=requested_by)
