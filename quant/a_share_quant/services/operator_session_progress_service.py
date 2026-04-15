"""operator continuation 生命周期推进服务。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Iterable

from a_share_quant.domain.models import ExecutionReport, Fill, OrderRequest, OrderStatus, TradeCommandEvent
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService


@dataclass(slots=True)
class OrderProgressDelta:
    """单笔订单 continuation 推进结果。"""

    order: OrderRequest
    new_fills: list[Fill]
    events: list[TradeCommandEvent]


class OperatorSessionProgressService:
    """把 poll/subscription 结果收口为可 replay 的统一 lifecycle 事件链。"""

    def __init__(
        self,
        *,
        event_service: OperatorSessionEventService,
        plugin_manager=None,
        plugin_context=None,
    ) -> None:
        self.event_service = event_service
        self.lifecycle_service = event_service.lifecycle_service
        self.plugin_manager = plugin_manager
        self.plugin_context = plugin_context

    def bind_plugin_manager(self, plugin_manager, *, plugin_context=None) -> None:
        """在 orchestrator 完成正式装配后回填 plugin manager。"""
        self.plugin_manager = plugin_manager
        if plugin_context is not None:
            self.plugin_context = plugin_context

    def synthesize_session_progress(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        reports: list[ExecutionReport],
        external_fills: list[Fill],
        existing_session_events: Iterable[TradeCommandEvent],
        existing_fill_ids: set[str],
    ) -> tuple[list[Fill], list[TradeCommandEvent]]:
        event_history = list(existing_session_events)
        report_map: dict[str, list[ExecutionReport]] = defaultdict(list)
        order_by_broker_id = {item.broker_order_id: item for item in orders if item.broker_order_id}
        for raw_report in reports:
            report = self._normalize_report(raw_report)
            matched_order = next((item for item in orders if report.order_id == item.order_id), None)
            if matched_order is None and report.broker_order_id:
                matched_order = order_by_broker_id.get(report.broker_order_id)
            if matched_order is None:
                continue
            if report.account_id is None:
                report.account_id = matched_order.account_id
            report_map[matched_order.order_id].append(report)

        all_new_fills: list[Fill] = []
        all_events: list[TradeCommandEvent] = []
        known_fill_ids = set(existing_fill_ids)
        for order in orders:
            delta = self._synthesize_order_progress(
                session_id,
                order=order,
                reports=report_map.get(order.order_id, []),
                external_fills=external_fills,
                prior_session_events=event_history,
                existing_fill_ids=known_fill_ids,
            )
            order.status = delta.order.status
            order.broker_order_id = delta.order.broker_order_id
            order.filled_quantity = delta.order.filled_quantity
            order.avg_fill_price = delta.order.avg_fill_price
            order.last_error = delta.order.last_error
            order.account_id = delta.order.account_id
            all_new_fills.extend(delta.new_fills)
            all_events.extend(delta.events)
            known_fill_ids.update(fill.fill_id for fill in delta.new_fills)
            event_history.extend(delta.events)
        return all_new_fills, all_events

    def _synthesize_order_progress(
        self,
        session_id: str,
        *,
        order: OrderRequest,
        reports: list[ExecutionReport],
        external_fills: list[Fill],
        prior_session_events: list[TradeCommandEvent],
        existing_fill_ids: set[str],
    ) -> OrderProgressDelta:
        order_history = self._filter_order_events(prior_session_events, order_id=order.order_id)
        state = self.lifecycle_service.replay_lifecycle_events(order_history, runtime_lane="operator_trade")
        if state is None:
            state = self.lifecycle_service.build_initial_state(order, runtime_lane="operator_trade")
            state.status = OrderStatus.CREATED.value
            state.stage = "INTENT"

        shadow_order = replace(order)
        latest_report = reports[-1] if reports else None
        if latest_report is not None:
            shadow_order.account_id = shadow_order.account_id or latest_report.account_id
            if latest_report.broker_order_id:
                shadow_order.broker_order_id = latest_report.broker_order_id
            if latest_report.status == OrderStatus.ACCEPTED:
                shadow_order.mark_accepted(shadow_order.broker_order_id)
            elif latest_report.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
                shadow_order.status = latest_report.status
                shadow_order.filled_quantity = max(int(latest_report.filled_quantity), shadow_order.filled_quantity)
                if latest_report.fill_price is not None:
                    shadow_order.avg_fill_price = latest_report.fill_price
            elif latest_report.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}:
                shadow_order.mark_rejected(latest_report.status, latest_report.message or "broker rejected order")
            elif latest_report.status == OrderStatus.CANCELLED:
                shadow_order.status = OrderStatus.CANCELLED
                shadow_order.last_error = latest_report.message or shadow_order.last_error
            elif latest_report.status == OrderStatus.CANCEL_REJECTED:
                shadow_order.status = OrderStatus.CANCEL_REJECTED
                shadow_order.last_error = latest_report.message or shadow_order.last_error
            elif latest_report.status == OrderStatus.EXPIRED:
                shadow_order.status = OrderStatus.EXPIRED
                shadow_order.last_error = latest_report.message or shadow_order.last_error

        matched_fills: list[Fill] = []
        for fill in external_fills:
            if shadow_order.account_id and fill.account_id not in {None, '', shadow_order.account_id}:
                continue
            if fill.order_id == shadow_order.order_id or (
                shadow_order.broker_order_id and fill.broker_order_id == shadow_order.broker_order_id
            ) or (
                shadow_order.broker_order_id and fill.order_id == shadow_order.broker_order_id
            ):
                matched_fills.append(fill)
        matched_fills.sort(key=lambda item: (item.trade_date.isoformat(), item.fill_id))
        total_quantity = sum(int(item.fill_quantity) for item in matched_fills)
        if matched_fills and total_quantity >= shadow_order.filled_quantity:
            shadow_order.filled_quantity = min(total_quantity, shadow_order.quantity)
            total_notional = sum(float(item.fill_price) * int(item.fill_quantity) for item in matched_fills)
            shadow_order.avg_fill_price = total_notional / total_quantity if total_quantity > 0 else shadow_order.avg_fill_price
            shadow_order.status = OrderStatus.FILLED if shadow_order.filled_quantity >= shadow_order.quantity else OrderStatus.PARTIALLY_FILLED
            if shadow_order.broker_order_id is None:
                shadow_order.broker_order_id = next((item.broker_order_id for item in matched_fills if item.broker_order_id), None)

        events: list[TradeCommandEvent] = []
        if state.status == OrderStatus.CREATED.value and shadow_order.broker_order_id:
            submit_command = self.lifecycle_service.build_broker_command(
                shadow_order,
                broker_provider=self.event_service.broker_provider,
                command_type="SUBMIT_ORDER",
                runtime_lane="operator_trade",
                broker_order_id=shadow_order.broker_order_id,
                sequence=0,
            )
            submit_payload = {
                **self.lifecycle_service.build_order_payload(shadow_order, session_id=session_id, runtime_lane="operator_trade"),
                "status": OrderStatus.SUBMITTED.value,
                "broker_order_id": shadow_order.broker_order_id,
                "filled_quantity": max(int(state.filled_quantity), 0),
                "remaining_quantity": max(int(shadow_order.quantity) - int(state.filled_quantity), 0),
            }
            for event_type in ("ORDER_SUBMITTED", "ORDER_TICKET_RECEIVED"):
                lifecycle_event = self.lifecycle_service.build_lifecycle_event(
                    event_type=event_type,
                    level="INFO",
                    order=shadow_order,
                    payload=submit_payload,
                    runtime_lane="operator_trade",
                    broker_provider=self.event_service.broker_provider,
                    broker_command=submit_command,
                    session_id=session_id,
                    previous_state=state,
                )
                trade_event = self._enrich_event(session_id, lifecycle_event)
                events.append(trade_event)
                state = lifecycle_event.state_after

        if latest_report is not None:
            report_payload = self.event_service.execution_report_payload(shadow_order, latest_report, sequence=0)
            report_payload["report_status"] = latest_report.status.value
            report_payload["status"] = state.status
            report_payload["filled_quantity"] = int(state.filled_quantity)
            report_payload["remaining_quantity"] = int(state.remaining_quantity)
            report_event = self.lifecycle_service.build_lifecycle_event(
                event_type="ORDER_REPORT",
                level="ERROR" if latest_report.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED} else "INFO",
                order=shadow_order,
                payload=report_payload,
                runtime_lane="operator_trade",
                broker_provider=self.event_service.broker_provider,
                session_id=session_id,
                previous_state=state,
            )
            events.append(self._enrich_event(session_id, report_event))
            state = report_event.state_after

        if shadow_order.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED} and state.status in {OrderStatus.CREATED.value, OrderStatus.SUBMITTED.value}:
            accepted_payload = {
                **self.lifecycle_service.build_order_payload(shadow_order, session_id=session_id, runtime_lane="operator_trade"),
                "status": OrderStatus.ACCEPTED.value,
                "broker_order_id": shadow_order.broker_order_id,
                "filled_quantity": max(int(state.filled_quantity), 0),
                "remaining_quantity": max(shadow_order.quantity - int(state.filled_quantity), 0),
            }
            accepted_event = self.lifecycle_service.build_lifecycle_event(
                event_type="ORDER_ACCEPTED",
                level="INFO",
                order=shadow_order,
                payload=accepted_payload,
                runtime_lane="operator_trade",
                broker_provider=self.event_service.broker_provider,
                session_id=session_id,
                previous_state=state,
            )
            events.append(self._enrich_event(session_id, accepted_event))
            state = accepted_event.state_after

        status_event_type = self._resolve_status_event_type(shadow_order.status)
        if status_event_type is not None and (
            state.status != shadow_order.status.value or int(state.filled_quantity) != int(shadow_order.filled_quantity)
        ):
            state_payload = {
                **self.lifecycle_service.build_order_payload(shadow_order, session_id=session_id, runtime_lane="operator_trade"),
                "status": shadow_order.status.value,
                "filled_quantity": int(shadow_order.filled_quantity),
                "remaining_quantity": int(shadow_order.remaining_quantity),
                "avg_fill_price": shadow_order.avg_fill_price,
                "broker_order_id": shadow_order.broker_order_id,
                "account_id": shadow_order.account_id,
                "reason": shadow_order.last_error,
            }
            status_event = self.lifecycle_service.build_lifecycle_event(
                event_type=status_event_type,
                level="ERROR" if shadow_order.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED} else "INFO",
                order=shadow_order,
                payload=state_payload,
                runtime_lane="operator_trade",
                broker_provider=self.event_service.broker_provider,
                session_id=session_id,
                previous_state=state,
            )
            events.append(self._enrich_event(session_id, status_event))
            state = status_event.state_after

        new_fills: list[Fill] = []
        cumulative_filled = int(state.filled_quantity)
        for fill in matched_fills:
            fill.account_id = fill.account_id or shadow_order.account_id
            fill.order_id = shadow_order.order_id
            if fill.fill_id in existing_fill_ids:
                continue
            new_fills.append(fill)
            cumulative_filled = min(cumulative_filled + int(fill.fill_quantity), shadow_order.quantity)
            fill_order = replace(
                shadow_order,
                filled_quantity=cumulative_filled,
                avg_fill_price=fill.fill_price,
                broker_order_id=fill.broker_order_id or shadow_order.broker_order_id,
            )
            fill_payload = self.lifecycle_service.build_fill_payload(
                order=fill_order,
                fill=fill,
                sequence=0,
                runtime_lane="operator_trade",
            )
            fill_payload["status"] = state.status
            lifecycle_event = self.lifecycle_service.build_lifecycle_event(
                event_type="ORDER_FILL",
                level="INFO",
                order=fill_order,
                payload=fill_payload,
                runtime_lane="operator_trade",
                broker_provider=self.event_service.broker_provider,
                position_delta=self.lifecycle_service.build_position_delta(order=fill_order, fill=fill),
                account_delta=self.lifecycle_service.build_account_delta(order=fill_order, fill=fill),
                session_id=session_id,
                previous_state=state,
            )
            events.append(self._enrich_event(session_id, lifecycle_event))
            state = lifecycle_event.state_after
        return OrderProgressDelta(order=shadow_order, new_fills=new_fills, events=events)

    def _normalize_report(self, report: ExecutionReport) -> ExecutionReport:
        if self.plugin_manager is None:
            return report
        return self.plugin_manager.normalize_execution_report(self.plugin_context, report)

    def _enrich_event(self, session_id: str, lifecycle_event) -> TradeCommandEvent:
        trade_event = self.lifecycle_service.lifecycle_event_to_trade_command_event(session_id, lifecycle_event)
        if self.plugin_manager is None:
            return trade_event
        return self.plugin_manager.enrich_lifecycle_event(self.plugin_context, trade_event)

    @staticmethod
    def _resolve_status_event_type(status: OrderStatus) -> str | None:
        return {
            OrderStatus.ACCEPTED: "ORDER_ACCEPTED",
            OrderStatus.PARTIALLY_FILLED: "ORDER_PARTIALLY_FILLED",
            OrderStatus.FILLED: "ORDER_FILLED",
            OrderStatus.PRE_TRADE_REJECTED: "ORDER_REJECTED",
            OrderStatus.EXECUTION_REJECTED: "ORDER_REJECTED",
            OrderStatus.REJECTED: "ORDER_REJECTED",
            OrderStatus.CANCELLED: "ORDER_CANCELLED",
            OrderStatus.CANCEL_REJECTED: "ORDER_CANCEL_REJECTED",
            OrderStatus.EXPIRED: "ORDER_EXPIRED",
        }.get(status)

    @staticmethod
    def _filter_order_events(events: Iterable[TradeCommandEvent], *, order_id: str) -> list[TradeCommandEvent]:
        matched: list[TradeCommandEvent] = []
        for event in events:
            payload = event.payload or {}
            lifecycle = payload.get("lifecycle") or {}
            intent = lifecycle.get("order_intent") or {}
            if str(payload.get("order_id") or intent.get("order_id") or "").strip() == order_id:
                matched.append(event)
        return matched
