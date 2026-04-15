"""operator session 事件组装服务。"""
from __future__ import annotations

from dataclasses import replace
from typing import Any

from a_share_quant.domain.models import ExecutionReport, LiveOrderSubmission, OrderRequest, TradeCommandEvent
from a_share_quant.execution.order_lifecycle_service import OrderLifecycleEventService


class OperatorSessionEventService:
    def __init__(self, lifecycle_service: OrderLifecycleEventService | None = None, *, broker_provider: str = "broker") -> None:
        self.lifecycle_service = lifecycle_service or OrderLifecycleEventService()
        self.broker_provider = broker_provider

    def new_session_event(self, session_id: str, *, event_type: str, level: str, payload: dict[str, Any], created_at: str | None = None) -> TradeCommandEvent:
        return self.lifecycle_service.new_session_event(session_id, event_type=event_type, level=level, payload=payload, created_at=created_at)

    def build_order_intent_events(self, session_id: str, orders: list[OrderRequest], rejected_orders: list[OrderRequest]) -> list[TradeCommandEvent]:
        events: list[TradeCommandEvent] = []
        for order in orders:
            state = self.lifecycle_service.build_initial_state(order, runtime_lane="operator_trade")
            state.status = "CREATED"
            state.stage = "INTENT"
            state.last_error = None
            intent_event = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_INTENT_REGISTERED", level="INFO", order=order, payload={**self.lifecycle_service.build_order_payload(order, session_id=session_id, runtime_lane="operator_trade"), "status": "CREATED", "filled_quantity": 0, "remaining_quantity": order.quantity, "last_error": None}, runtime_lane="operator_trade", broker_provider=self.broker_provider, session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, intent_event))
            state = intent_event.state_after
            rejected = order in rejected_orders
            risk_decision = self.lifecycle_service.build_risk_decision(order, passed=not rejected, stage="PRE_TRADE", reason=order.last_error or "passed pre-trade validation", severity="WARN" if rejected else "INFO", runtime_lane="operator_trade")
            risk_event = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_RISK_DECISION", level="WARN" if rejected else "INFO", order=order, payload={**self.lifecycle_service.build_order_payload(order, session_id=session_id, runtime_lane="operator_trade"), "status": ("PRE_TRADE_REJECTED" if rejected else "CREATED"), "filled_quantity": 0, "remaining_quantity": order.quantity, "passed": risk_decision.passed, "reason": risk_decision.reason, "severity": risk_decision.severity}, runtime_lane="operator_trade", broker_provider=self.broker_provider, risk_decision=risk_decision, session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, risk_event))
        return events

    def build_submission_events(self, session_id: str, order: OrderRequest, submission: LiveOrderSubmission, *, sequence: int) -> list[TradeCommandEvent]:
        events: list[TradeCommandEvent] = []
        state = self.lifecycle_service.build_initial_state(order, runtime_lane="operator_trade")
        submit_command = self.lifecycle_service.build_broker_command(order, broker_provider=self.broker_provider, command_type="SUBMIT_ORDER", runtime_lane="operator_trade", broker_order_id=submission.ticket.broker_order_id, sequence=sequence)
        shadow_order = replace(order, status=submission.ticket.status, broker_order_id=submission.ticket.broker_order_id or order.broker_order_id, filled_quantity=submission.ticket.filled_quantity, avg_fill_price=submission.ticket.avg_fill_price)
        submit_payload = {**self.lifecycle_service.build_order_payload(shadow_order, session_id=session_id, runtime_lane="operator_trade"), "status": "SUBMITTED", "broker_order_id": submission.ticket.broker_order_id, "ticket_status": submission.ticket.status.value, "filled_quantity": submission.ticket.filled_quantity, "avg_fill_price": submission.ticket.avg_fill_price, "sequence": sequence}
        for event_type in ("ORDER_SUBMITTED", "ORDER_TICKET_RECEIVED"):
            event = self.lifecycle_service.build_lifecycle_event(event_type=event_type, level="INFO", order=order, payload={**submit_payload, "status": "SUBMITTED"}, runtime_lane="operator_trade", broker_provider=self.broker_provider, broker_command=submit_command, session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, event))
            state = event.state_after
        if submission.ticket.status.name == "ACCEPTED":
            accepted = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_ACCEPTED", level="INFO", order=order, payload={**submit_payload, "status": "ACCEPTED"}, runtime_lane="operator_trade", broker_provider=self.broker_provider, broker_command=submit_command, session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, accepted))
            state = accepted.state_after
        for report in submission.reports:
            level = "WARN" if report.status.name in {"REJECTED", "EXECUTION_REJECTED", "PRE_TRADE_REJECTED"} else "INFO"
            report_payload = self.execution_report_payload(order, report, sequence=sequence)
            report_command = self.lifecycle_service.build_broker_command(order, broker_provider=self.broker_provider, command_type="REPORT", runtime_lane="operator_trade", broker_order_id=report.broker_order_id, sequence=sequence)
            report_event = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_REPORT", level=level, order=order, payload=report_payload, runtime_lane="operator_trade", broker_provider=self.broker_provider, broker_command=report_command, session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, report_event))
            state = report_event.state_after
            if report.status.name == "ACCEPTED":
                accepted = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_ACCEPTED", level="INFO", order=order, payload=report_payload, runtime_lane="operator_trade", broker_provider=self.broker_provider, session_id=session_id, previous_state=state)
                events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, accepted))
                state = accepted.state_after
            elif report.status.name in {"PARTIALLY_FILLED", "FILLED"}:
                progressed = self.lifecycle_service.build_lifecycle_event(event_type=self.lifecycle_service.status_to_event_type(report.status.value), level="INFO", order=order, payload=report_payload, runtime_lane="operator_trade", broker_provider=self.broker_provider, session_id=session_id, previous_state=state)
                events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, progressed))
                state = progressed.state_after
        for fill in submission.fills:
            fill_payload = self.lifecycle_service.build_fill_payload(order=shadow_order, fill=fill, sequence=sequence, runtime_lane="operator_trade")
            fill_payload["status"] = "FILLED" if fill.fill_quantity >= shadow_order.quantity else "PARTIALLY_FILLED"
            fill_event = self.lifecycle_service.build_lifecycle_event(event_type="ORDER_FILL", level="INFO", order=order, payload=fill_payload, runtime_lane="operator_trade", broker_provider=self.broker_provider, position_delta=self.lifecycle_service.build_position_delta(order=order, fill=fill), account_delta=self.lifecycle_service.build_account_delta(order=order, fill=fill), session_id=session_id, previous_state=state)
            events.append(self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, fill_event))
            state = fill_event.state_after
        return events

    def execution_report_payload(self, order: OrderRequest, report: ExecutionReport, *, sequence: int) -> dict[str, Any]:
        return self.lifecycle_service.build_execution_report_payload(order=order, report=report, sequence=sequence, broker_provider=self.broker_provider, runtime_lane="operator_trade")

    def order_to_event_payload(self, order: OrderRequest) -> dict[str, Any]:
        return self.lifecycle_service.build_order_payload(order, runtime_lane="operator_trade")

    @staticmethod
    def derive_broker_event_cursor(previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        return reports[-1].report_id if reports else previous_cursor
