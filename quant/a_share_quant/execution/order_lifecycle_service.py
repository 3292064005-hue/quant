"""统一订单生命周期核心服务。

该服务把 backtest 与 operator 双 lane 的订单推进语义收口到同一套
领域对象、状态机约束与事件 envelope 上，避免不同调用链各自手拼 payload。
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from a_share_quant.core.events import EventType
from a_share_quant.core.utils import new_id, now_iso
from a_share_quant.domain.models import ExecutionReport, Fill, OrderRequest, OrderStatus, TradeCommandEvent


@dataclass(slots=True)
class OrderIntent:
    order_id: str
    trade_date: str
    strategy_id: str
    ts_code: str
    side: str
    requested_quantity: int
    price: float
    order_type: str
    time_in_force: str
    reason: str
    runtime_lane: str
    account_id: str | None = None
    run_id: str | None = None
    session_id: str | None = None


@dataclass(slots=True)
class RiskDecision:
    order_id: str
    passed: bool
    stage: str
    severity: str
    reason: str
    runtime_lane: str
    account_id: str | None = None


@dataclass(slots=True)
class BrokerCommand:
    order_id: str
    command_type: str
    broker_provider: str
    runtime_lane: str
    requested_quantity: int
    broker_order_id: str | None = None
    account_id: str | None = None
    sequence: int | None = None


@dataclass(slots=True)
class PositionDelta:
    order_id: str
    ts_code: str
    side: str
    delta_quantity: int
    fill_quantity: int
    fill_price: float
    account_id: str | None = None


@dataclass(slots=True)
class AccountDelta:
    order_id: str
    gross_notional: float
    fee: float
    tax: float
    cash_delta: float
    account_id: str | None = None


@dataclass(slots=True)
class LifecycleState:
    order_id: str
    runtime_lane: str
    status: str
    stage: str
    requested_quantity: int
    filled_quantity: int = 0
    remaining_quantity: int = 0
    avg_fill_price: float | None = None
    broker_order_id: str | None = None
    event_count: int = 0
    last_event_type: str | None = None
    last_error: str | None = None


@dataclass(slots=True)
class LifecycleEvent:
    event_id: str
    event_type: str
    level: str
    created_at: str
    runtime_lane: str
    order_intent: OrderIntent
    state_before: LifecycleState
    state_after: LifecycleState
    risk_decision: RiskDecision | None = None
    broker_command: BrokerCommand | None = None
    position_delta: PositionDelta | None = None
    account_delta: AccountDelta | None = None
    payload: dict[str, Any] = field(default_factory=dict)


class OrderLifecycleEventService:
    _STATUS_TO_EVENT_TYPE = {
        "CREATED": "ORDER_CREATED",
        "SUBMITTED": EventType.ORDER_SUBMITTED,
        "ACCEPTED": EventType.ORDER_ACCEPTED,
        "PARTIALLY_FILLED": EventType.ORDER_PARTIALLY_FILLED,
        "FILLED": EventType.ORDER_FILLED,
        "PRE_TRADE_REJECTED": "ORDER_REJECTED_PRE_TRADE",
        "EXECUTION_REJECTED": EventType.ORDER_REJECTED,
        "REJECTED": EventType.ORDER_REJECTED,
        "CANCELLED": "ORDER_CANCELLED",
        "CANCEL_REJECTED": "ORDER_CANCEL_REJECTED",
        "EXPIRED": "ORDER_EXPIRED",
    }
    _EVENT_TYPE_TO_STAGE = {
        "ORDER_CREATED": "INTENT",
        "ORDER_INTENT_REGISTERED": "INTENT",
        "ORDER_RISK_DECISION": "RISK",
        EventType.ORDER_SUBMITTED: "BROKER_SUBMIT",
        "ORDER_TICKET_RECEIVED": "BROKER_ACK",
        EventType.ORDER_ACCEPTED: "BROKER_ACCEPT",
        "ORDER_REPORT": "REPORT",
        EventType.EXECUTION_REPORT: "REPORT",
        EventType.ORDER_PARTIALLY_FILLED: "FILL",
        EventType.ORDER_FILLED: "FILL",
        "ORDER_FILL": "FILL",
        "ORDER_REJECTED_PRE_TRADE": "RISK",
        EventType.ORDER_REJECTED: "REPORT",
        "ORDER_SUBMIT_FAILED": "BROKER_SUBMIT",
        "ORDER_CANCELLED": "CANCEL",
        "ORDER_CANCEL_REJECTED": "REPORT",
        "ORDER_EXPIRED": "REPORT",
    }
    _EVENT_TYPE_TO_STATUS = {
        "ORDER_CREATED": OrderStatus.CREATED.value,
        "ORDER_INTENT_REGISTERED": OrderStatus.CREATED.value,
        "ORDER_RISK_DECISION": None,
        EventType.ORDER_SUBMITTED: OrderStatus.SUBMITTED.value,
        "ORDER_TICKET_RECEIVED": OrderStatus.SUBMITTED.value,
        EventType.ORDER_ACCEPTED: OrderStatus.ACCEPTED.value,
        "ORDER_REPORT": None,
        EventType.EXECUTION_REPORT: None,
        EventType.ORDER_PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED.value,
        EventType.ORDER_FILLED: OrderStatus.FILLED.value,
        "ORDER_FILL": None,
        "ORDER_REJECTED_PRE_TRADE": OrderStatus.PRE_TRADE_REJECTED.value,
        EventType.ORDER_REJECTED: OrderStatus.EXECUTION_REJECTED.value,
        "ORDER_SUBMIT_FAILED": OrderStatus.EXECUTION_REJECTED.value,
        "ORDER_CANCELLED": OrderStatus.CANCELLED.value,
        "ORDER_CANCEL_REJECTED": OrderStatus.CANCEL_REJECTED.value,
        "ORDER_EXPIRED": OrderStatus.EXPIRED.value,
    }
    _ALLOWED_TRANSITIONS = {
        OrderStatus.CREATED.value: {"ORDER_CREATED", "ORDER_INTENT_REGISTERED", "ORDER_RISK_DECISION", EventType.ORDER_SUBMITTED, "ORDER_TICKET_RECEIVED", "ORDER_REJECTED_PRE_TRADE", EventType.ORDER_REJECTED, "ORDER_SUBMIT_FAILED"},
        OrderStatus.SUBMITTED.value: {"ORDER_TICKET_RECEIVED", EventType.ORDER_ACCEPTED, EventType.ORDER_REJECTED, "ORDER_SUBMIT_FAILED", "ORDER_REPORT", EventType.ORDER_FILLED, EventType.ORDER_PARTIALLY_FILLED},
        OrderStatus.ACCEPTED.value: {"ORDER_REPORT", EventType.ORDER_ACCEPTED, EventType.ORDER_PARTIALLY_FILLED, EventType.ORDER_FILLED, "ORDER_FILL", EventType.ORDER_REJECTED, "ORDER_CANCELLED", "ORDER_CANCEL_REJECTED", "ORDER_EXPIRED"},
        OrderStatus.PARTIALLY_FILLED.value: {"ORDER_REPORT", EventType.ORDER_PARTIALLY_FILLED, EventType.ORDER_FILLED, "ORDER_FILL", "ORDER_CANCELLED", "ORDER_CANCEL_REJECTED", "ORDER_EXPIRED"},
        OrderStatus.FILLED.value: {"ORDER_REPORT", EventType.ORDER_FILLED, "ORDER_FILL"},
        OrderStatus.PRE_TRADE_REJECTED.value: {"ORDER_REPORT", "ORDER_REJECTED_PRE_TRADE", "ORDER_RISK_DECISION"},
        OrderStatus.EXECUTION_REJECTED.value: {"ORDER_REPORT", EventType.ORDER_REJECTED, "ORDER_SUBMIT_FAILED"},
        OrderStatus.CANCELLED.value: {"ORDER_REPORT", "ORDER_CANCELLED"},
        OrderStatus.CANCEL_REJECTED.value: {"ORDER_REPORT", "ORDER_CANCEL_REJECTED"},
        OrderStatus.EXPIRED.value: {"ORDER_REPORT", "ORDER_EXPIRED"},
    }

    def status_to_event_type(self, status: str | None, *, default: str = "ORDER_STATUS_UPDATED") -> str:
        return self._STATUS_TO_EVENT_TYPE.get(str(status).upper(), default) if status else default

    def event_type_to_status(self, event_type: str) -> str | None:
        return self._EVENT_TYPE_TO_STATUS.get(str(event_type))

    def infer_runtime_lane(self, order: OrderRequest) -> str:
        return "research_backtest" if order.run_id else "operator_trade"

    def build_order_intent(self, order: OrderRequest, *, session_id: str | None = None, runtime_lane: str | None = None) -> OrderIntent:
        lane = runtime_lane or self.infer_runtime_lane(order)
        return OrderIntent(order_id=order.order_id, trade_date=order.trade_date.isoformat(), strategy_id=order.strategy_id, ts_code=order.ts_code, side=order.side.value, requested_quantity=order.quantity, price=order.price, order_type=order.order_type.value, time_in_force=order.time_in_force.value, reason=order.reason, runtime_lane=lane, account_id=order.account_id, run_id=order.run_id, session_id=session_id)

    def build_initial_state(self, order: OrderRequest, *, runtime_lane: str | None = None) -> LifecycleState:
        lane = runtime_lane or self.infer_runtime_lane(order)
        return LifecycleState(order_id=order.order_id, runtime_lane=lane, status=order.status.value, stage="INTENT", requested_quantity=int(order.quantity), filled_quantity=int(order.filled_quantity), remaining_quantity=int(order.remaining_quantity), avg_fill_price=order.avg_fill_price, broker_order_id=order.broker_order_id, last_error=order.last_error)

    def build_risk_decision(self, order: OrderRequest, *, passed: bool, stage: str, reason: str, severity: str = "INFO", runtime_lane: str | None = None) -> RiskDecision:
        lane = runtime_lane or self.infer_runtime_lane(order)
        return RiskDecision(order_id=order.order_id, passed=bool(passed), stage=stage, severity=severity, reason=reason, runtime_lane=lane, account_id=order.account_id)

    def build_broker_command(self, order: OrderRequest, *, broker_provider: str, command_type: str, runtime_lane: str | None = None, broker_order_id: str | None = None, sequence: int | None = None) -> BrokerCommand:
        lane = runtime_lane or self.infer_runtime_lane(order)
        return BrokerCommand(order_id=order.order_id, command_type=command_type, broker_provider=broker_provider, runtime_lane=lane, requested_quantity=order.quantity, broker_order_id=broker_order_id or order.broker_order_id, account_id=order.account_id, sequence=sequence)

    def build_position_delta(self, *, order: OrderRequest, fill: Fill) -> PositionDelta:
        signed_quantity = int(fill.fill_quantity) if fill.side.value == "BUY" else -int(fill.fill_quantity)
        return PositionDelta(order_id=order.order_id, ts_code=fill.ts_code, side=fill.side.value, delta_quantity=signed_quantity, fill_quantity=int(fill.fill_quantity), fill_price=float(fill.fill_price), account_id=fill.account_id or order.account_id)

    def build_account_delta(self, *, order: OrderRequest, fill: Fill) -> AccountDelta:
        gross_notional = float(fill.fill_price) * int(fill.fill_quantity)
        cash_delta = (-gross_notional if fill.side.value == "BUY" else gross_notional) - float(fill.fee) - float(fill.tax)
        return AccountDelta(order_id=order.order_id, gross_notional=gross_notional, fee=float(fill.fee), tax=float(fill.tax), cash_delta=cash_delta, account_id=fill.account_id or order.account_id)

    def transition_state(self, previous: LifecycleState, *, event_type: str, payload: dict[str, Any] | None = None, risk_decision: RiskDecision | None = None, broker_command: BrokerCommand | None = None, position_delta: PositionDelta | None = None, account_delta: AccountDelta | None = None) -> LifecycleState:
        allowed = self._ALLOWED_TRANSITIONS.get(previous.status, {event_type})
        if event_type not in allowed and event_type != "ORDER_REPORT":
            raise ValueError(f"生命周期非法跳转: status={previous.status}, event_type={event_type}")
        payload = dict(payload or {})
        next_status = payload.get("status") or self.event_type_to_status(event_type) or previous.status
        filled_quantity = int(payload.get("filled_quantity", previous.filled_quantity) or 0)
        remaining_quantity = int(payload.get("remaining_quantity", previous.remaining_quantity) or 0)
        if position_delta is not None:
            filled_quantity = max(filled_quantity, int(position_delta.fill_quantity))
            remaining_quantity = max(previous.requested_quantity - filled_quantity, 0)
        avg_fill_price = payload.get("avg_fill_price", previous.avg_fill_price)
        if payload.get("fill_price") is not None:
            avg_fill_price = float(payload["fill_price"])
        if next_status == OrderStatus.FILLED.value:
            remaining_quantity = 0
            filled_quantity = max(filled_quantity, previous.requested_quantity)
        broker_order_id = payload.get("broker_order_id") or (broker_command.broker_order_id if broker_command else None) or previous.broker_order_id
        last_error = payload.get("reason") or payload.get("message") or previous.last_error
        if risk_decision is not None and risk_decision.passed:
            last_error = previous.last_error
        if next_status in {OrderStatus.CREATED.value, OrderStatus.SUBMITTED.value, OrderStatus.ACCEPTED.value, OrderStatus.PARTIALLY_FILLED.value, OrderStatus.FILLED.value}:
            last_error = None if event_type not in {"ORDER_SUBMIT_FAILED", EventType.ORDER_REJECTED, "ORDER_REJECTED_PRE_TRADE"} else last_error
        return LifecycleState(order_id=previous.order_id, runtime_lane=previous.runtime_lane, status=str(next_status), stage=self._EVENT_TYPE_TO_STAGE.get(event_type, previous.stage), requested_quantity=previous.requested_quantity, filled_quantity=filled_quantity, remaining_quantity=remaining_quantity, avg_fill_price=avg_fill_price, broker_order_id=broker_order_id, event_count=previous.event_count + 1, last_event_type=event_type, last_error=last_error)

    def build_order_payload(self, order: OrderRequest, *, session_id: str | None = None, runtime_lane: str | None = None) -> dict[str, Any]:
        payload = asdict(order)
        payload["trade_date"] = order.trade_date.isoformat()
        payload["side"] = order.side.value
        payload["status"] = order.status.value
        payload["order_type"] = order.order_type.value
        payload["time_in_force"] = order.time_in_force.value
        payload["requested_quantity"] = order.quantity
        payload["remaining_quantity"] = order.remaining_quantity
        payload["lifecycle"] = {"order_intent": asdict(self.build_order_intent(order, session_id=session_id, runtime_lane=runtime_lane))}
        return payload

    def build_execution_report_payload(self, *, order: OrderRequest, report: ExecutionReport, sequence: int | None = None, broker_provider: str = "broker", runtime_lane: str | None = None) -> dict[str, Any]:
        metadata = dict(report.metadata or {})
        lane = runtime_lane or self.infer_runtime_lane(order)
        broker_command = self.build_broker_command(order, broker_provider=broker_provider, command_type="REPORT", runtime_lane=lane, broker_order_id=report.broker_order_id, sequence=sequence)
        risk_decision = None
        if report.status.value in {"PRE_TRADE_REJECTED", "EXECUTION_REJECTED", "REJECTED"}:
            risk_decision = self.build_risk_decision(order, passed=False, stage="REPORT", reason=report.message or "broker rejected order", severity="ERROR", runtime_lane=lane)
        payload = {"order_id": order.order_id, "report_id": report.report_id, "trade_date": report.trade_date.isoformat(), "status": report.status.value, "ts_code": metadata.get("ts_code") or order.ts_code, "side": metadata.get("side") or order.side.value, "requested_quantity": report.requested_quantity, "filled_quantity": report.filled_quantity, "remaining_quantity": report.remaining_quantity, "message": report.message, "fill_price": report.fill_price, "avg_fill_price": report.fill_price, "fee_estimate": report.fee_estimate, "tax_estimate": report.tax_estimate, "broker_order_id": report.broker_order_id, "account_id": report.account_id or metadata.get("account_id") or order.account_id, "metadata": metadata, "lifecycle": {"order_intent": asdict(self.build_order_intent(order, runtime_lane=lane)), "broker_command": asdict(broker_command), "risk_decision": asdict(risk_decision) if risk_decision else None}}
        if sequence is not None: payload["sequence"] = sequence
        return payload

    def build_fill_payload(self, *, order: OrderRequest, fill: Fill, sequence: int | None = None, runtime_lane: str | None = None) -> dict[str, Any]:
        lane = runtime_lane or self.infer_runtime_lane(order)
        position_delta = self.build_position_delta(order=order, fill=fill)
        account_delta = self.build_account_delta(order=order, fill=fill)
        payload = {"fill_id": fill.fill_id, "order_id": order.order_id, "trade_date": fill.trade_date.isoformat(), "ts_code": fill.ts_code, "side": fill.side.value, "fill_quantity": fill.fill_quantity, "filled_quantity": max(order.filled_quantity, fill.fill_quantity), "remaining_quantity": max(order.remaining_quantity, 0), "fill_price": fill.fill_price, "avg_fill_price": fill.fill_price, "fee": fill.fee, "tax": fill.tax, "broker_order_id": fill.broker_order_id, "account_id": fill.account_id or order.account_id, "lifecycle": {"order_intent": asdict(self.build_order_intent(order, runtime_lane=lane)), "position_delta": asdict(position_delta), "account_delta": asdict(account_delta)}}
        if sequence is not None: payload["sequence"] = sequence
        return payload

    def build_lifecycle_event(self, *, event_type: str, level: str, order: OrderRequest, payload: dict[str, Any] | None = None, created_at: str | None = None, runtime_lane: str | None = None, broker_provider: str = "broker", risk_decision: RiskDecision | None = None, broker_command: BrokerCommand | None = None, position_delta: PositionDelta | None = None, account_delta: AccountDelta | None = None, session_id: str | None = None, previous_state: LifecycleState | None = None) -> LifecycleEvent:
        lane = runtime_lane or self.infer_runtime_lane(order)
        order_intent = self.build_order_intent(order, session_id=session_id, runtime_lane=lane)
        state_before = previous_state or self.build_initial_state(order, runtime_lane=lane)
        effective_payload = dict(payload or {})
        lifecycle = dict(effective_payload.get("lifecycle") or {})
        lifecycle.setdefault("order_intent", asdict(order_intent))
        if risk_decision is not None: lifecycle["risk_decision"] = asdict(risk_decision)
        if broker_command is not None: lifecycle["broker_command"] = asdict(broker_command)
        if position_delta is not None: lifecycle["position_delta"] = asdict(position_delta)
        if account_delta is not None: lifecycle["account_delta"] = asdict(account_delta)
        effective_payload["lifecycle"] = lifecycle
        effective_payload.setdefault("stage", self._EVENT_TYPE_TO_STAGE.get(event_type, "LIFECYCLE"))
        effective_payload.setdefault("runtime_lane", lane)
        if broker_command is not None: effective_payload.setdefault("broker_provider", broker_provider)
        state_after = self.transition_state(state_before, event_type=event_type, payload=effective_payload, risk_decision=risk_decision, broker_command=broker_command, position_delta=position_delta, account_delta=account_delta)
        lifecycle["state_before"] = asdict(state_before)
        lifecycle["state_after"] = asdict(state_after)
        effective_payload.setdefault("status", state_after.status)
        effective_payload.setdefault("filled_quantity", state_after.filled_quantity)
        effective_payload.setdefault("remaining_quantity", state_after.remaining_quantity)
        effective_payload.setdefault("broker_order_id", state_after.broker_order_id)
        return LifecycleEvent(event_id=new_id("event"), event_type=event_type, level=level, created_at=created_at or now_iso(), runtime_lane=lane, order_intent=order_intent, state_before=state_before, state_after=state_after, risk_decision=risk_decision, broker_command=broker_command, position_delta=position_delta, account_delta=account_delta, payload=effective_payload)

    def lifecycle_event_to_payload(self, event: LifecycleEvent) -> dict[str, Any]:
        payload = dict(event.payload)
        payload.setdefault("order_id", event.order_intent.order_id)
        payload.setdefault("trade_date", event.order_intent.trade_date)
        payload.setdefault("ts_code", event.order_intent.ts_code)
        payload.setdefault("side", event.order_intent.side)
        payload.setdefault("requested_quantity", event.order_intent.requested_quantity)
        payload.setdefault("account_id", event.order_intent.account_id)
        payload.setdefault("lifecycle", {})
        payload["lifecycle"].setdefault("state_before", asdict(event.state_before))
        payload["lifecycle"].setdefault("state_after", asdict(event.state_after))
        return payload

    def new_session_event(self, session_id: str, *, event_type: str, level: str, payload: dict[str, Any], created_at: str | None = None) -> TradeCommandEvent:
        return TradeCommandEvent(event_id=new_id("event"), session_id=session_id, event_type=event_type, payload=dict(payload), level=level, created_at=created_at or now_iso())

    def lifecycle_event_to_trade_command_event(self, session_id: str, event: LifecycleEvent) -> TradeCommandEvent:
        return TradeCommandEvent(event_id=event.event_id, session_id=session_id, event_type=event.event_type, level=event.level, payload=self.lifecycle_event_to_payload(event), created_at=event.created_at)

    def replay_lifecycle_events(self, events: Iterable[dict[str, Any] | TradeCommandEvent], *, runtime_lane: str | None = None) -> LifecycleState | None:
        state: LifecycleState | None = None
        for item in events:
            if isinstance(item, TradeCommandEvent):
                normalized = self.normalize_trade_command_event(item)
            else:
                normalized = self.normalize_runtime_event(self._coerce_runtime_row(item))
            payload = dict(normalized.get("payload") or {})
            lifecycle = payload.get("lifecycle") or {}
            intent = lifecycle.get("order_intent") or {}
            event_type = str(normalized.get("event_type") or "")
            if not (payload.get("order_id") or intent.get("order_id") or lifecycle.get("state_after") or lifecycle.get("state_before")):
                continue
            if state is None:
                state = LifecycleState(order_id=str(intent.get("order_id") or payload.get("order_id") or ""), runtime_lane=str(normalized.get("runtime_lane") or runtime_lane or intent.get("runtime_lane") or "unknown"), status=str((lifecycle.get("state_before") or {}).get("status") or payload.get("status") or OrderStatus.CREATED.value), stage=str((lifecycle.get("state_before") or {}).get("stage") or "INTENT"), requested_quantity=int(intent.get("requested_quantity") or payload.get("requested_quantity") or payload.get("quantity") or 0), filled_quantity=int(payload.get("filled_quantity") or 0), remaining_quantity=int(payload.get("remaining_quantity") or 0), avg_fill_price=payload.get("avg_fill_price"), broker_order_id=payload.get("broker_order_id"), last_error=payload.get("reason") or payload.get("message"))
            state_before = lifecycle.get("state_before")
            state_after = lifecycle.get("state_after")
            if isinstance(state_before, dict): state = self._state_from_mapping(state_before, fallback=state)
            if isinstance(state_after, dict): state = self._state_from_mapping(state_after, fallback=state)
            elif event_type in self._EVENT_TYPE_TO_STAGE: state = self.transition_state(state, event_type=event_type, payload=payload)
        return state

    @staticmethod
    def _coerce_runtime_row(row: dict[str, Any]) -> dict[str, Any]:
        if "payload" in row or "event_type" not in row:
            return row
        return {
            "event_id": row.get("event_id"),
            "event_type": row.get("event_type"),
            "level": row.get("level") or "INFO",
            "payload": {
                key: value
                for key, value in row.items()
                if key not in {"event_id", "event_type", "level", "occurred_at", "created_at", "source_domain", "stream_scope", "stream_id"}
            },
            "occurred_at": row.get("occurred_at") or row.get("created_at"),
            "source_domain": row.get("source_domain"),
            "stream_scope": row.get("stream_scope"),
            "stream_id": row.get("stream_id"),
        }

    def normalize_trade_command_event(self, event: TradeCommandEvent) -> dict[str, Any]:
        return self.normalize_runtime_event({"event_id": event.event_id, "event_type": event.event_type, "level": event.level, "payload": event.payload, "occurred_at": event.created_at, "source_domain": "operator", "stream_scope": "trade_session", "stream_id": event.session_id})

    def normalize_runtime_event(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row.get("payload") or {})
        lifecycle = payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
        payload["lifecycle"] = lifecycle
        if payload.get("order_id") and "order_intent" not in lifecycle:
            lifecycle["order_intent"] = {"order_id": payload.get("order_id"), "trade_date": payload.get("trade_date"), "strategy_id": payload.get("strategy_id"), "ts_code": payload.get("ts_code"), "side": payload.get("side"), "requested_quantity": payload.get("requested_quantity") or payload.get("quantity"), "price": payload.get("price"), "order_type": payload.get("order_type"), "time_in_force": payload.get("time_in_force"), "reason": payload.get("reason"), "runtime_lane": payload.get("runtime_lane") or row.get("runtime_lane") or "unknown", "account_id": payload.get("account_id"), "run_id": payload.get("run_id"), "session_id": row.get("stream_id") or payload.get("session_id")}
        state_after = lifecycle.get("state_after")
        return {"event_id": row.get("event_id"), "event_type": row.get("event_type"), "level": row.get("level") or "INFO", "payload": payload, "created_at": row.get("occurred_at") or row.get("created_at"), "occurred_at": row.get("occurred_at") or row.get("created_at"), "source_domain": row.get("source_domain"), "stream_scope": row.get("stream_scope"), "stream_id": row.get("stream_id"), "stage": payload.get("stage") or self._EVENT_TYPE_TO_STAGE.get(str(row.get("event_type") or ""), "LIFECYCLE"), "runtime_lane": payload.get("runtime_lane") or lifecycle.get("order_intent", {}).get("runtime_lane"), "status": state_after.get("status") if isinstance(state_after, dict) else payload.get("status")}

    def summarize_lifecycle_events(self, events: Iterable[dict[str, Any] | TradeCommandEvent], *, runtime_lane: str | None = None) -> dict[str, Any]:
        """汇总生命周期事件流，供 report / audit / operator snapshot 共用。"""
        normalized_events: list[dict[str, Any]] = []
        by_type: dict[str, int] = {}
        by_stage: dict[str, int] = {}
        by_level: dict[str, int] = {}
        runtime_lanes: set[str] = set()
        order_groups: dict[str, list[dict[str, Any]]] = {}
        for item in events:
            if isinstance(item, TradeCommandEvent):
                normalized = self.normalize_trade_command_event(item)
            else:
                normalized = self.normalize_runtime_event(self._coerce_runtime_row(dict(item)))
            payload = dict(normalized.get("payload") or {})
            lifecycle = payload.get("lifecycle") or {}
            intent = lifecycle.get("order_intent") or {}
            order_id = str(intent.get("order_id") or payload.get("order_id") or "").strip()
            if not order_id:
                continue
            normalized_events.append(normalized)
            event_type = str(normalized.get("event_type") or "UNKNOWN")
            stage = str(normalized.get("stage") or payload.get("stage") or "LIFECYCLE")
            level = str(normalized.get("level") or "INFO")
            lane = str(normalized.get("runtime_lane") or runtime_lane or intent.get("runtime_lane") or "unknown")
            by_type[event_type] = by_type.get(event_type, 0) + 1
            by_stage[stage] = by_stage.get(stage, 0) + 1
            by_level[level] = by_level.get(level, 0) + 1
            runtime_lanes.add(lane)
            order_groups.setdefault(order_id, []).append(normalized)
        terminal_statuses: dict[str, int] = {}
        terminal_stages: dict[str, int] = {}
        rejected_orders: list[str] = []
        for order_id, group in order_groups.items():
            state = self.replay_lifecycle_events(group, runtime_lane=runtime_lane)
            if state is None:
                continue
            terminal_statuses[state.status] = terminal_statuses.get(state.status, 0) + 1
            terminal_stages[state.stage] = terminal_stages.get(state.stage, 0) + 1
            if state.status in {OrderStatus.PRE_TRADE_REJECTED.value, OrderStatus.EXECUTION_REJECTED.value, OrderStatus.REJECTED.value}:
                rejected_orders.append(order_id)
        return {
            "event_count": len(normalized_events),
            "order_count": len(order_groups),
            "by_type": by_type,
            "by_stage": by_stage,
            "by_level": by_level,
            "terminal_statuses": terminal_statuses,
            "terminal_stages": terminal_stages,
            "runtime_lanes": sorted(runtime_lanes),
            "rejected_order_ids": sorted(rejected_orders),
        }

    def build_audit_payload(self, *, action: str, base_payload: dict[str, Any], lifecycle_events: Iterable[dict[str, Any] | TradeCommandEvent] | None = None, runtime_lane: str | None = None) -> dict[str, Any]:
        """基于 lifecycle 骨干构造审计载荷。"""
        payload = dict(base_payload)
        payload.setdefault("action", action)
        if lifecycle_events is not None:
            summary = self.summarize_lifecycle_events(lifecycle_events, runtime_lane=runtime_lane)
            if summary.get("event_count", 0) > 0:
                payload["lifecycle_summary"] = summary
        return payload

    def assert_cross_lane_invariant(self, *event_groups: Iterable[dict[str, Any] | TradeCommandEvent]) -> LifecycleState:
        snapshots = [self.replay_lifecycle_events(group) for group in event_groups]
        snapshots = [item for item in snapshots if item is not None]
        if not snapshots: raise ValueError("缺少可比较的生命周期事件流")
        base = snapshots[0]
        for snapshot in snapshots[1:]:
            if (snapshot.status, snapshot.stage, snapshot.requested_quantity, snapshot.filled_quantity, snapshot.remaining_quantity) != (base.status, base.stage, base.requested_quantity, base.filled_quantity, base.remaining_quantity):
                raise ValueError("cross-lane lifecycle invariant 失败")
        return base

    def order_to_snapshot(self, order: OrderRequest) -> dict[str, Any]:
        payload = self.build_order_payload(order)
        return {"order_id": payload.get("order_id"), "trade_date": payload.get("trade_date"), "ts_code": payload.get("ts_code"), "side": payload.get("side"), "price": payload.get("price"), "quantity": payload.get("quantity"), "requested_quantity": payload.get("requested_quantity"), "filled_quantity": payload.get("filled_quantity"), "remaining_quantity": payload.get("remaining_quantity"), "status": payload.get("status"), "broker_order_id": payload.get("broker_order_id"), "account_id": payload.get("account_id"), "reason": payload.get("reason"), "last_error": payload.get("last_error"), "lifecycle": payload.get("lifecycle") or {}}

    @staticmethod
    def _state_from_mapping(mapping: dict[str, Any], *, fallback: LifecycleState | None) -> LifecycleState:
        return LifecycleState(order_id=str(mapping.get("order_id") or (fallback.order_id if fallback else "")), runtime_lane=str(mapping.get("runtime_lane") or (fallback.runtime_lane if fallback else "unknown")), status=str(mapping.get("status") or (fallback.status if fallback else OrderStatus.CREATED.value)), stage=str(mapping.get("stage") or (fallback.stage if fallback else "INTENT")), requested_quantity=int(mapping.get("requested_quantity") or (fallback.requested_quantity if fallback else 0) or 0), filled_quantity=int(mapping.get("filled_quantity") or (fallback.filled_quantity if fallback else 0) or 0), remaining_quantity=int(mapping.get("remaining_quantity") or (fallback.remaining_quantity if fallback else 0) or 0), avg_fill_price=mapping.get("avg_fill_price", fallback.avg_fill_price if fallback else None), broker_order_id=mapping.get("broker_order_id") or (fallback.broker_order_id if fallback else None), event_count=int(mapping.get("event_count") or (fallback.event_count if fallback else 0) or 0), last_event_type=mapping.get("last_event_type") or (fallback.last_event_type if fallback else None), last_error=mapping.get("last_error") or (fallback.last_error if fallback else None))
