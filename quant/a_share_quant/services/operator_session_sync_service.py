"""operator session 同步推进服务。"""
from __future__ import annotations

from datetime import date

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import ExecutionReport, Fill, OrderRequest, TradeSessionResult, TradeSessionStatus
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_submission_execution_service import OperatorSubmissionExecutionService
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService
from a_share_quant.services.operator_session_progress_service import OperatorSessionProgressService
from a_share_quant.services.operator_session_query_service import OperatorSessionQueryService
from a_share_quant.services.operator_session_write_service import OperatorSessionWriteService
from a_share_quant.services.operator_submission_service import OperatorSubmissionService
from a_share_quant.services.operator_trade_audit_service import OperatorTradeAuditService


class OperatorSessionSyncService:
    """负责 poll/report 驱动的 session 推进。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        execution_session_repository: ExecutionSessionRepository,
        order_repository: OrderRepository,
        query_service: OperatorSessionQueryService,
        progress_service: OperatorSessionProgressService,
        event_service: OperatorSessionEventService,
        session_write_service: OperatorSessionWriteService,
        submission_service: OperatorSubmissionService,
        submission_execution_service: OperatorSubmissionExecutionService,
        audit_service: OperatorTradeAuditService,
    ) -> None:
        self.config = config
        self.broker = broker
        self.execution_session_repository = execution_session_repository
        self.order_repository = order_repository
        self.query_service = query_service
        self.progress_service = progress_service
        self.event_service = event_service
        self.session_write_service = session_write_service
        self.submission_service = submission_service
        self.submission_execution_service = submission_execution_service
        self.audit_service = audit_service

    def bind_plugin_manager(self, plugin_manager, plugin_context=None) -> None:
        self.progress_service.bind_plugin_manager(plugin_manager, plugin_context=plugin_context)

    def sync_session_events(
        self,
        session_id: str,
        *,
        requested_by: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        session = self.execution_session_repository.get(session_id)
        if session is None:
            raise ValueError(f"未找到交易会话: {session_id}")
        orders = self.query_service.list_session_orders(session_id)
        if not orders:
            raise ValueError(f"交易会话 {session_id} 不存在可同步订单")
        broker_ids = [item.broker_order_id or item.order_id for item in orders if item.broker_order_id or item.order_id]
        poll_reports = getattr(self.broker, "poll_execution_reports", None)
        reports = list(poll_reports(account_id=session.account_id, broker_order_ids=broker_ids)) if callable(poll_reports) else []
        external_fills = self.query_trades_scoped(session.account_id)
        broker_event_cursor = self.derive_broker_event_cursor(session.broker_event_cursor, reports)
        return self.advance_session_from_reports(
            session_id,
            reports=reports,
            external_fills=external_fills,
            requested_by=requested_by,
            source="poll",
            broker_event_cursor=broker_event_cursor,
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
        session = self.execution_session_repository.get(session_id)
        if session is None:
            raise ValueError(f"未找到交易会话: {session_id}")
        effective_requested_by = (requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by
        orders = self.query_service.list_session_orders(session_id)
        if not orders:
            raise ValueError(f"交易会话 {session_id} 不存在可同步订单")
        new_fills, generated_events = self.apply_polled_progress(session_id, orders=orders, reports=reports, external_fills=external_fills)
        final_status = self.submission_execution_service.resolve_session_status(orders)
        risk_summary = dict(session.risk_summary)
        risk_summary.update({
            "last_sync_source": source,
            "last_sync_report_count": len(reports),
            "last_sync_new_fill_count": len(new_fills),
        })
        pending_follow_up_count = self.submission_service.count_pending_follow_up_orders(orders)
        now_value = self.now_iso()
        final_error_message = self.submission_service.resolve_final_error_message(
            final_status,
            risk_summary=risk_summary,
            pending_follow_up_count=pending_follow_up_count,
        )
        generated_events.append(
            self.event_service.new_session_event(
                session_id,
                event_type="SESSION_SYNC_COMPLETED",
                level="INFO",
                payload={
                    "source": source,
                    "report_count": len(reports),
                    "new_fill_count": len(new_fills),
                    "account_id": session.account_id,
                    "broker_event_cursor": broker_event_cursor,
                    "supervisor_mode": supervisor_mode,
                },
                created_at=now_value,
            )
        )
        capture_plan = self.submission_execution_service.collect_account_capture_plan(
            session_id,
            trade_date=date.fromisoformat(session.requested_trade_date) if session.requested_trade_date else date.today(),
            account_id=session.account_id,
            source=f"sync:{source}",
            captured_at=now_value,
        )
        final_summary = self.session_write_service.persist_sync_result(
            session_id=session_id,
            final_status=final_status,
            risk_summary=risk_summary,
            error_message=final_error_message,
            broker_event_cursor=broker_event_cursor,
            last_synced_at=now_value,
            supervisor_mode=supervisor_mode,
            last_supervised_at=now_value if supervisor_mode else None,
            orders=orders,
            fills=new_fills,
            events=generated_events,
            account_capture_plan=capture_plan,
        )
        self.audit_service.write_best_effort(
            action="session_synced",
            entity_id=session_id,
            payload={
                "status": final_summary.status.value,
                "source": source,
                "report_count": len(reports),
                "new_fill_count": len(new_fills),
                "account_id": session.account_id,
                "broker_event_cursor": broker_event_cursor,
                "supervisor_mode": supervisor_mode,
            },
            operator=effective_requested_by,
            level="INFO",
            session_id=session_id,
            lifecycle_events=generated_events,
        )
        return TradeSessionResult(
            summary=final_summary,
            orders=orders,
            fills=new_fills,
            events=self.execution_session_repository.list_events(session_id),
            replayed=False,
        )

    def sync_latest_open_session(self, *, requested_by: str | None = None) -> TradeSessionResult:
        sessions = self.execution_session_repository.list_sessions(
            statuses=[TradeSessionStatus.RUNNING, TradeSessionStatus.RECOVERY_REQUIRED],
            limit=1,
        )
        if not sessions:
            raise ValueError("当前没有需要同步事件的交易会话")
        return self.sync_session_events(sessions[0].session_id, requested_by=requested_by)

    def apply_polled_progress(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        reports: list[ExecutionReport],
        external_fills: list[Fill],
    ):
        existing_fill_ids = {row["fill_id"] for row in self.order_repository.list_fills(execution_session_id=session_id, limit=2000)}
        existing_events = self.execution_session_repository.list_events(session_id, limit=5000)
        return self.progress_service.synthesize_session_progress(
            session_id,
            orders=orders,
            reports=reports,
            external_fills=external_fills,
            existing_session_events=existing_events,
            existing_fill_ids=existing_fill_ids,
        )

    def query_trades_scoped(self, account_id: str | None) -> list[Fill]:
        query_scoped = getattr(self.broker, "query_trades_scoped", None)
        if callable(query_scoped):
            return list(query_scoped(account_id=account_id))
        trades = list(self.broker.query_trades())
        if account_id is None:
            return trades
        return [item for item in trades if getattr(item, "account_id", None) in {None, "", account_id}]

    @staticmethod
    def derive_broker_event_cursor(previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        cursor = previous_cursor
        for report in reports:
            metadata_cursor = report.metadata.get("cursor") if isinstance(report.metadata, dict) else None
            cursor = str(metadata_cursor) if metadata_cursor else report.report_id or cursor
        return cursor

    @staticmethod
    def now_iso() -> str:
        from a_share_quant.core.utils import now_iso
        return now_iso()
