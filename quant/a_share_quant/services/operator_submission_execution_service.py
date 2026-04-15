"""operator submit 执行与持久化服务。"""
from __future__ import annotations

from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import Fill, OrderRequest, OrderStatus, TradeSessionResult, TradeSessionStatus
from a_share_quant.services.operator_account_capture_service import OperatorAccountCapturePlan, OperatorAccountCaptureService
from a_share_quant.services.operator_recovery_service import OperatorRecoveryService
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService
from a_share_quant.services.operator_session_write_service import OperatorSessionWriteService
from a_share_quant.services.operator_submission_preparation_service import PreparedOperatorSubmission
from a_share_quant.services.operator_submission_service import OperatorSubmissionService
from a_share_quant.services.operator_trade_audit_service import OperatorTradeAuditService


class OperatorSubmissionExecutionService:
    """执行 prepared submit，负责 broker 交互、落库、恢复与审计。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        submission_service: OperatorSubmissionService,
        event_service: OperatorSessionEventService,
        session_write_service: OperatorSessionWriteService,
        account_capture_service: OperatorAccountCaptureService,
        recovery_service: OperatorRecoveryService,
        audit_service: OperatorTradeAuditService,
        plugin_manager=None,
        plugin_context=None,
    ) -> None:
        self.config = config
        self.broker = broker
        self.submission_service = submission_service
        self.event_service = event_service
        self.session_write_service = session_write_service
        self.account_capture_service = account_capture_service
        self.recovery_service = recovery_service
        self.audit_service = audit_service
        self.plugin_manager = plugin_manager
        self.plugin_context = plugin_context

    def bind_plugin_manager(self, plugin_manager, plugin_context=None) -> None:
        self.plugin_manager = plugin_manager
        if plugin_context is not None:
            self.plugin_context = plugin_context

    def execute_submit(self, prepared: PreparedOperatorSubmission) -> TradeSessionResult:
        orders = prepared.orders
        accepted_orders = prepared.accepted_orders
        session_id = prepared.session_id
        events = list(prepared.initial_events)
        fills: list[Fill] = []
        try:
            if accepted_orders:
                events.append(
                    self.event_service.new_session_event(
                        session_id,
                        event_type="BROKER_SUBMISSION_STARTED",
                        level="INFO",
                        payload={
                            "accepted_order_count": len(accepted_orders),
                            "account_id": prepared.account_id,
                        },
                    )
                )
            for index, order in enumerate(accepted_orders):
                try:
                    if self.plugin_manager is not None:
                        transformed_payload = self.plugin_manager.transform_submission_order(
                            self.plugin_context,
                            self.event_service.order_to_event_payload(order),
                        )
                        self.apply_submission_order_payload(order, transformed_payload)
                    submission = self.submission_service.submit_order_lifecycle(order)
                    fills.extend(submission.fills)
                    events.extend(self.event_service.build_submission_events(session_id, order, submission, sequence=index))
                    self.submission_service.apply_submission_to_order(order, submission)
                except Exception as exc:
                    order.mark_rejected(OrderStatus.EXECUTION_REJECTED, str(exc))
                    events.append(
                        self.event_service.new_session_event(
                            session_id,
                            event_type="ORDER_SUBMIT_FAILED",
                            level="ERROR",
                            payload={"order_id": order.order_id, "error": str(exc), "sequence": index},
                        )
                    )
                    if self.config.operator.fail_fast:
                        break
            pending_follow_up_count = self.submission_service.count_pending_follow_up_orders(orders)
            final_status = self.resolve_session_status(orders)
            final_error_message = self.submission_service.resolve_final_error_message(
                final_status,
                risk_summary=prepared.risk_summary,
                pending_follow_up_count=pending_follow_up_count,
            )
            capture_plan = self.collect_account_capture_plan(
                session_id,
                trade_date=prepared.trade_date,
                account_id=prepared.account_id,
                source="submit_orders",
                captured_at=prepared.captured_at,
            )
            final_summary = self.session_write_service.persist_submit_result(
                initial_summary=prepared.initial_summary,
                final_status=final_status,
                risk_summary=prepared.risk_summary,
                error_message=final_error_message,
                orders=orders,
                fills=fills,
                events=events,
                account_capture_plan=capture_plan,
            )
            self.audit_service.write_best_effort(
                action="session_completed",
                entity_id=final_summary.session_id,
                payload={
                    "status": final_summary.status.value,
                    "risk_summary": final_summary.risk_summary,
                    "submitted_count": final_summary.submitted_count,
                    "rejected_count": final_summary.rejected_count,
                    "account_id": final_summary.account_id,
                },
                operator=prepared.requested_by,
                level="INFO" if final_summary.status in {TradeSessionStatus.COMPLETED, TradeSessionStatus.PARTIALLY_COMPLETED} else "ERROR",
                session_id=final_summary.session_id,
                lifecycle_events=events,
            )
            return TradeSessionResult(
                summary=final_summary,
                orders=list(orders),
                fills=fills,
                events=self.session_write_service.execution_session_repository.list_events(session_id),
                replayed=False,
            )
        except Exception as exc:
            terminal_message = str(exc)
            risk_summary = dict(prepared.risk_summary)
            risk_summary.setdefault("terminal_error", terminal_message)
            recovery_status = TradeSessionStatus.RECOVERY_REQUIRED if fills or any(order.broker_order_id for order in orders) else TradeSessionStatus.FAILED
            failure_events = list(events)
            failure_events.append(
                self.event_service.new_session_event(
                    session_id,
                    event_type="RECOVERY_REQUIRED" if recovery_status == TradeSessionStatus.RECOVERY_REQUIRED else "SESSION_ABORTED",
                    level="ERROR",
                    payload={"error": terminal_message, "recovery_required": recovery_status == TradeSessionStatus.RECOVERY_REQUIRED},
                )
            )
            self.recovery_service.persist_recovery_seed(
                session_id=session_id,
                command_type=prepared.command_type,
                command_source=prepared.command_source,
                requested_by=prepared.requested_by,
                requested_trade_date=prepared.trade_date.isoformat(),
                idempotency_key=prepared.idempotency_key,
                risk_summary=risk_summary,
                orders=orders,
                fills=fills,
                events=failure_events,
                status=recovery_status,
                error_message=terminal_message,
                account_id=prepared.account_id,
                created_at=prepared.captured_at,
            )
            recovered = self.recovery_service.attempt_recovery(
                session_id,
                orders=orders,
                submitted_orders=orders,
                fills=fills,
                requested_by=prepared.requested_by,
                terminal_message=terminal_message,
            )
            if recovered is not None:
                return recovered
            self.audit_service.write_best_effort(
                action="session_failed" if recovery_status == TradeSessionStatus.FAILED else "session_recovery_required",
                entity_id=session_id,
                payload={
                    "error": terminal_message,
                    "submitted_count": self.submission_service.count_submitted_orders(orders),
                    "rejected_count": self.submission_service.count_rejected_orders(orders),
                    "account_id": prepared.account_id,
                },
                operator=prepared.requested_by,
                level="ERROR",
                session_id=session_id,
                lifecycle_events=failure_events,
            )
            raise

    def collect_account_capture_plan(
        self,
        session_id: str,
        *,
        trade_date: date,
        account_id: str | None,
        source: str,
        captured_at: str,
    ) -> OperatorAccountCapturePlan:
        if not self.account_capture_service.is_enabled():
            return self.account_capture_service.disabled_plan(
                session_id=session_id,
                trade_date=trade_date,
                account_id=account_id,
                source=source,
                captured_at=captured_at,
            )
        try:
            account = self.get_account_snapshot_scoped(account_id)
            positions = tuple(self.get_positions_scoped(account_id))
            return OperatorAccountCapturePlan(
                session_id=session_id,
                trade_date=trade_date,
                account_id=account_id,
                source=source,
                captured_at=captured_at,
                account=account,
                positions=positions,
            )
        except Exception as exc:
            return OperatorAccountCapturePlan(
                session_id=session_id,
                trade_date=trade_date,
                account_id=account_id,
                source=source,
                captured_at=captured_at,
                error_message=str(exc),
            )

    def get_account_snapshot_scoped(self, account_id: str | None):
        getter = getattr(self.broker, "get_account_snapshot", None)
        if callable(getter):
            return getter(account_id=account_id, last_prices=None)
        return self.broker.get_account(last_prices=None)

    def get_positions_scoped(self, account_id: str | None):
        getter = getattr(self.broker, "get_position_snapshots", None)
        if callable(getter):
            return list(getter(account_id=account_id, last_prices=None))
        return list(self.broker.get_positions(last_prices=None))

    @staticmethod
    def apply_submission_order_payload(order: OrderRequest, payload: dict[str, Any]) -> None:
        if "price" in payload and payload["price"] is not None:
            order.price = float(payload["price"])
        if "quantity" in payload and payload["quantity"] is not None:
            order.quantity = int(payload["quantity"])
        if "reason" in payload and payload["reason"] is not None:
            order.reason = str(payload["reason"])
        if "account_id" in payload and payload["account_id"] is not None:
            order.account_id = str(payload["account_id"])

    def resolve_session_status(self, orders: list[OrderRequest]) -> TradeSessionStatus:
        order_count = len(orders)
        rejected_count = self.submission_service.count_rejected_orders(orders)
        filled_count = len([order for order in orders if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}])
        terminal_non_fill_count = len([order for order in orders if order.status in {OrderStatus.CANCELLED, OrderStatus.CANCEL_REJECTED, OrderStatus.EXPIRED}])
        pending_follow_up_count = self.submission_service.count_pending_follow_up_orders(orders)
        if order_count <= 0:
            return TradeSessionStatus.FAILED
        if rejected_count >= order_count:
            return TradeSessionStatus.REJECTED
        if pending_follow_up_count > 0:
            return TradeSessionStatus.RECOVERY_REQUIRED
        if filled_count > 0 and (rejected_count > 0 or terminal_non_fill_count > 0):
            return TradeSessionStatus.PARTIALLY_COMPLETED
        if terminal_non_fill_count > 0 and rejected_count > 0:
            return TradeSessionStatus.PARTIALLY_COMPLETED
        if filled_count > 0 or terminal_non_fill_count > 0:
            return TradeSessionStatus.COMPLETED
        return TradeSessionStatus.FAILED
