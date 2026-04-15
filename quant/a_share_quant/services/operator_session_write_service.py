"""operator session 原子写入服务。"""
from __future__ import annotations

from dataclasses import replace

from a_share_quant.domain.models import Fill, OrderRequest, OrderStatus, TradeCommandEvent, TradeSessionStatus, TradeSessionSummary
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_account_capture_service import OperatorAccountCapturePlan, OperatorAccountCaptureService
from a_share_quant.services.operator_submission_service import OperatorSubmissionService

class OperatorSessionWriteService:
    """把 operator 会话、事件、订单、成交与快照收敛到单个本地事务。"""

    def __init__(
        self,
        *,
        execution_session_repository: ExecutionSessionRepository,
        order_repository: OrderRepository,
        account_capture_service: OperatorAccountCaptureService,
    ) -> None:
        self.execution_session_repository = execution_session_repository
        self.order_repository = order_repository
        self.account_capture_service = account_capture_service
        self.store = order_repository.store
        self.submission_service = OperatorSubmissionService.count_submitted_orders
        self.rejection_counter = OperatorSubmissionService.count_rejected_orders

    def persist_submit_result(
        self,
        *,
        initial_summary: TradeSessionSummary,
        final_status: TradeSessionStatus,
        risk_summary: dict[str, object],
        error_message: str | None,
        orders: list[OrderRequest],
        fills: list[Fill],
        events: list[TradeCommandEvent],
        account_capture_plan: OperatorAccountCapturePlan,
    ) -> TradeSessionSummary:
        """原子落库 submit 主链最终结果。"""
        final_summary = replace(
            initial_summary,
            status=final_status,
            risk_summary=dict(risk_summary),
            submitted_count=self.submission_service(orders),
            rejected_count=self.rejection_counter(orders),
            error_message=error_message,
            updated_at=account_capture_plan.captured_at,
        )
        with self.store.transaction():
            self.execution_session_repository.insert_session(initial_summary)
            self.execution_session_repository.append_events(events)
            self.order_repository.save_execution_batch(None, orders, fills, execution_session_id=initial_summary.session_id)
            self.execution_session_repository.update_session(
                initial_summary.session_id,
                status=final_summary.status,
                submitted_count=final_summary.submitted_count,
                rejected_count=final_summary.rejected_count,
                risk_summary=final_summary.risk_summary,
                error_message=final_summary.error_message,
            )
            self.account_capture_service.persist_plan(account_capture_plan, self.execution_session_repository)
        stored = self.execution_session_repository.get(initial_summary.session_id)
        assert stored is not None
        return stored

    def persist_sync_result(
        self,
        *,
        session_id: str,
        final_status: TradeSessionStatus,
        risk_summary: dict[str, object],
        error_message: str | None,
        broker_event_cursor: str | None,
        last_synced_at: str,
        supervisor_mode: str | None,
        last_supervised_at: str | None,
        orders: list[OrderRequest],
        fills: list[Fill],
        events: list[TradeCommandEvent],
        account_capture_plan: OperatorAccountCapturePlan,
    ) -> TradeSessionSummary:
        """原子落库 sync/poll 主链结果。"""
        with self.store.transaction():
            self.order_repository.save_execution_batch(None, orders, fills, execution_session_id=session_id)
            self.execution_session_repository.update_session(
                session_id,
                status=final_status,
                submitted_count=self.submission_service(orders),
                rejected_count=self.rejection_counter(orders),
                risk_summary=dict(risk_summary),
                error_message=error_message,
                broker_event_cursor=broker_event_cursor,
                last_synced_at=last_synced_at,
                supervisor_mode=supervisor_mode,
                last_supervised_at=last_supervised_at,
            )
            self.execution_session_repository.append_events(events)
            self.account_capture_service.persist_plan(account_capture_plan, self.execution_session_repository)
        stored = self.execution_session_repository.get(session_id)
        assert stored is not None
        return stored
    @staticmethod
    def _count_rejected_orders(orders: list[OrderRequest]) -> int:
        """兼容旧调用方的拒单计数入口；实际逻辑委托给 OperatorSubmissionService。"""
        return OperatorSubmissionService.count_rejected_orders(orders)

    @staticmethod
    def _count_submitted_orders(orders: list[OrderRequest]) -> int:
        """兼容旧调用方的 submitted 计数入口；实际逻辑委托给 OperatorSubmissionService。"""
        return OperatorSubmissionService.count_submitted_orders(orders)

