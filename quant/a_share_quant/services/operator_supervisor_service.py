"""跨进程 operator supervisor 与 broker 订阅推进服务。"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock

from a_share_quant.adapters.broker.base import ExecutionReportSubscription, LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.core.utils import new_id, now_iso
from a_share_quant.domain.models import ExecutionReport, TradeSessionResult, TradeSessionStatus
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_session_advancement import SessionAdvancementPort

_OPEN_SESSION_STATUSES = [
    TradeSessionStatus.RUNNING,
    TradeSessionStatus.RECOVERY_REQUIRED,
]
_TERMINAL_SESSION_STATUSES = {
    TradeSessionStatus.COMPLETED,
    TradeSessionStatus.PARTIALLY_COMPLETED,
    TradeSessionStatus.REJECTED,
    TradeSessionStatus.FAILED,
    TradeSessionStatus.REPLAYED,
}


@dataclass(slots=True)
class OperatorSupervisorRunSummary:
    """一次 supervisor 运行摘要。"""

    owner_id: str
    requested_by: str
    mode: str
    iterations: int = 0
    claimed_session_ids: list[str] = field(default_factory=list)
    processed_session_ids: list[str] = field(default_factory=list)
    completed_session_ids: list[str] = field(default_factory=list)
    fallback_polled_session_ids: list[str] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    stopped_reason: str = ""


@dataclass(slots=True)
class _ClaimedSessionState:
    """单个已领取交易会话的监督态。"""

    session_id: str
    account_id: str | None
    broker_order_ids: list[str]
    requested_by: str
    mode: str
    last_cursor: str | None
    started_monotonic: float
    last_activity_monotonic: float
    last_heartbeat_monotonic: float
    queue: deque[tuple[list[ExecutionReport], str | None]] = field(default_factory=deque)
    queue_lock: Lock = field(default_factory=Lock)
    subscription: ExecutionReportSubscription | None = None
    last_result: TradeSessionResult | None = None
    fallback_used: bool = False
    finalized: bool = False

    def push_reports(self, reports: list[ExecutionReport], cursor: str | None) -> None:
        if not reports:
            return
        with self.queue_lock:
            self.queue.append((list(reports), cursor))
        self.last_activity_monotonic = time.monotonic()

    def pop_reports(self) -> tuple[list[ExecutionReport], str | None] | None:
        with self.queue_lock:
            if not self.queue:
                return None
            return self.queue.popleft()


class OperatorSupervisorService:
    """在独立进程中持续扫描/领取 open session，并用订阅或轮询推进状态。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        orchestrator: SessionAdvancementPort,
        execution_session_repository: ExecutionSessionRepository,
        order_repository: OrderRepository,
        audit_repository: AuditRepository,
    ) -> None:
        self.config = config
        self.broker = broker
        self.orchestrator = orchestrator
        self.execution_session_repository = execution_session_repository
        self.order_repository = order_repository
        self.audit_repository = audit_repository

    def run_once(
        self,
        *,
        requested_by: str | None = None,
        owner_id: str | None = None,
        account_id: str | None = None,
        session_id: str | None = None,
    ) -> OperatorSupervisorRunSummary:
        """执行一次 supervisor 扫描。

        Boundary Behavior:
            - 该方法不持有永久线程；适合 cron/守护进程外层循环调用；
            - 若 broker 支持订阅，会优先启动 subscription，并在空闲/软超时后回退到 poll；
            - supervisor 领取后会周期性续租；若续租失败，则立即停止推进并报告 ownership_lost；
            - 同一次 pass 可监督多个 session，但仍在单线程事件循环内推进，避免 SQLite 连接跨线程污染。
        """
        requested_by = (requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by
        owner_id = (owner_id or new_id("supervisor")).strip()
        mode = self._resolve_supervisor_mode()
        summary = OperatorSupervisorRunSummary(owner_id=owner_id, requested_by=requested_by, mode=mode, iterations=1)
        claimed = self.execution_session_repository.claim_sessions_for_supervisor(
            owner_id,
            statuses=list(_OPEN_SESSION_STATUSES),
            lease_expires_at=self._lease_expires_at(),
            account_id=account_id,
            session_id=session_id,
            limit=self.config.operator.supervisor_max_sessions_per_pass,
            now=now_iso(),
            supervisor_mode=mode,
        )
        summary.claimed_session_ids = [item.session_id for item in claimed]
        session_states: dict[str, _ClaimedSessionState] = {}
        try:
            for session in claimed:
                self.execution_session_repository.append_event(
                    session.session_id,
                    event_type="SUPERVISOR_CLAIMED",
                    level="INFO",
                    payload={"owner_id": owner_id, "mode": mode, "account_id": session.account_id},
                )
                session_states[session.session_id] = self._build_claimed_state(session.session_id, requested_by=requested_by, mode=mode)
            self._drive_claimed_sessions(
                session_states=session_states,
                requested_by=requested_by,
                owner_id=owner_id,
                summary=summary,
            )
        except Exception as exc:
            failing_session_id = self._find_active_session_id(session_states)
            if failing_session_id is not None:
                summary.errors.append({"session_id": failing_session_id, "error": str(exc)})
                self.execution_session_repository.append_event(
                    failing_session_id,
                    event_type="SUPERVISOR_ERROR",
                    level="ERROR",
                    payload={"owner_id": owner_id, "mode": mode, "error": str(exc)},
                )
                self.audit_repository.write(
                    run_id=None,
                    trace_id=new_id("trace"),
                    module="operator_supervisor",
                    action="session_supervision_failed",
                    entity_type="trade_session",
                    entity_id=failing_session_id,
                    payload={"owner_id": owner_id, "mode": mode, "error": str(exc)},
                    operator=requested_by,
                    level="ERROR",
                )
            raise
        finally:
            for state in session_states.values():
                self._close_subscription(state)
                released = self.execution_session_repository.release_supervisor_claim(state.session_id, owner_id=owner_id)
                if released:
                    self.execution_session_repository.append_event(
                        state.session_id,
                        event_type="SUPERVISOR_RELEASED",
                        level="INFO",
                        payload={"owner_id": owner_id, "mode": mode},
                    )
                else:
                    self.execution_session_repository.append_event(
                        state.session_id,
                        event_type="SUPERVISOR_RELEASE_SKIPPED",
                        level="WARNING",
                        payload={"owner_id": owner_id, "mode": mode, "reason": "ownership_not_held"},
                    )
        summary.stopped_reason = "no_open_sessions" if not claimed else ("errors_detected" if summary.errors else "pass_completed")
        return summary

    def run_loop(
        self,
        *,
        requested_by: str | None = None,
        owner_id: str | None = None,
        account_id: str | None = None,
        session_id: str | None = None,
        max_loops: int | None = None,
        stop_when_idle: bool = False,
    ) -> OperatorSupervisorRunSummary:
        """持续运行 supervisor，适合作为独立守护进程入口。"""
        owner = (owner_id or new_id("supervisor")).strip()
        aggregate = OperatorSupervisorRunSummary(
            owner_id=owner,
            requested_by=(requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by,
            mode=self._resolve_supervisor_mode(),
            iterations=0,
        )
        loops = 0
        while max_loops is None or loops < max_loops:
            loops += 1
            run_summary = self.run_once(
                requested_by=aggregate.requested_by,
                owner_id=owner,
                account_id=account_id,
                session_id=session_id,
            )
            aggregate.iterations += 1
            aggregate.claimed_session_ids.extend(run_summary.claimed_session_ids)
            aggregate.processed_session_ids.extend(run_summary.processed_session_ids)
            aggregate.completed_session_ids.extend(run_summary.completed_session_ids)
            aggregate.fallback_polled_session_ids.extend(run_summary.fallback_polled_session_ids)
            aggregate.errors.extend(run_summary.errors)
            aggregate.stopped_reason = run_summary.stopped_reason
            if run_summary.errors:
                aggregate.stopped_reason = "errors_detected"
                break
            if stop_when_idle and not run_summary.claimed_session_ids:
                aggregate.stopped_reason = "idle"
                break
            if max_loops is not None and loops >= max_loops:
                aggregate.stopped_reason = "max_loops_reached"
                break
            time.sleep(self.config.operator.supervisor_scan_interval_seconds)
        return aggregate

    def _build_claimed_state(self, session_id: str, *, requested_by: str, mode: str) -> _ClaimedSessionState:
        session = self.execution_session_repository.get(session_id)
        if session is None:
            raise ValueError(f"未找到交易会话: {session_id}")
        orders = self.orchestrator.list_session_orders(session_id)
        if not orders:
            raise ValueError(f"交易会话 {session_id} 不存在可同步订单")
        broker_order_ids = [item.broker_order_id or item.order_id for item in orders if item.broker_order_id or item.order_id]
        now_mono = time.monotonic()
        state = _ClaimedSessionState(
            session_id=session_id,
            account_id=session.account_id,
            broker_order_ids=broker_order_ids,
            requested_by=requested_by,
            mode=mode,
            last_cursor=session.broker_event_cursor,
            started_monotonic=now_mono,
            last_activity_monotonic=now_mono,
            last_heartbeat_monotonic=now_mono,
        )
        if mode in {"auto", "subscribe"} and self.broker.supports_execution_report_subscription():
            state.subscription = self._start_subscription(state)
        return state

    def _start_subscription(self, state: _ClaimedSessionState) -> ExecutionReportSubscription | None:
        def _handle(reports: list[ExecutionReport], cursor: str | None) -> None:
            derived_cursor = cursor or self.orchestrator.derive_broker_event_cursor(state.last_cursor, reports)
            state.last_cursor = derived_cursor or state.last_cursor
            state.push_reports(reports, state.last_cursor)

        subscription = self.broker.subscribe_execution_reports(
            _handle,
            account_id=state.account_id,
            broker_order_ids=state.broker_order_ids,
            cursor=state.last_cursor,
        )
        if subscription is None:
            return None
        self.execution_session_repository.append_event(
            state.session_id,
            event_type="SUPERVISOR_SUBSCRIPTION_STARTED",
            level="INFO",
            payload={"account_id": state.account_id, "broker_event_cursor": state.last_cursor},
        )
        return subscription

    def _drive_claimed_sessions(
        self,
        *,
        session_states: dict[str, _ClaimedSessionState],
        requested_by: str,
        owner_id: str,
        summary: OperatorSupervisorRunSummary,
    ) -> None:
        active = dict(session_states)
        idle_timeout = self.config.operator.supervisor_idle_timeout_seconds
        soft_timeout = max(idle_timeout * 5.0, idle_timeout + 0.5)
        while active:
            did_work = False
            for session_id in list(active):
                state = active[session_id]
                self._renew_claim_or_raise(state, owner_id=owner_id, requested_by=requested_by)
                batch = state.pop_reports()
                if batch is not None:
                    reports, cursor = batch
                    external_fills = self._query_trades_scoped(state.account_id)
                    state.last_result = self.orchestrator.advance_session_from_reports(
                        session_id,
                        reports=reports,
                        external_fills=external_fills,
                        requested_by=requested_by,
                        source="subscription",
                        broker_event_cursor=cursor,
                        supervisor_mode="subscribe",
                    )
                    if state.subscription is not None and hasattr(state.subscription, "update_cursor"):
                        state.subscription.update_cursor(state.last_result.summary.broker_event_cursor)
                    state.last_cursor = state.last_result.summary.broker_event_cursor or cursor or state.last_cursor
                    state.last_activity_monotonic = time.monotonic()
                    did_work = True
                    if state.last_result.summary.status in _TERMINAL_SESSION_STATUSES:
                        self._finalize_processed_state(state, summary=summary)
                        active.pop(session_id, None)
                    continue
                if state.subscription is None:
                    state.last_result = self.orchestrator.sync_session_events(
                        session_id,
                        requested_by=requested_by,
                        supervisor_mode="poll",
                    )
                    state.fallback_used = True if state.mode in {"auto", "subscribe"} else state.fallback_used
                    did_work = True
                    self._finalize_processed_state(state, summary=summary)
                    active.pop(session_id, None)
                    continue
                if time.monotonic() - state.last_activity_monotonic >= idle_timeout:
                    self.execution_session_repository.append_event(
                        session_id,
                        event_type="SUPERVISOR_SUBSCRIPTION_IDLE_TIMEOUT",
                        level="WARNING",
                        payload={"idle_timeout_seconds": idle_timeout, "broker_event_cursor": state.last_cursor},
                    )
                    self._close_subscription(state)
                    state.fallback_used = True
                    did_work = True
                    continue
                if time.monotonic() - state.started_monotonic >= soft_timeout:
                    self.execution_session_repository.append_event(
                        session_id,
                        event_type="SUPERVISOR_SUBSCRIPTION_SOFT_TIMEOUT",
                        level="WARNING",
                        payload={"max_wait_seconds": soft_timeout, "broker_event_cursor": state.last_cursor},
                    )
                    self._close_subscription(state)
                    state.fallback_used = True
                    did_work = True
            if active and not did_work:
                time.sleep(min(self.config.operator.supervisor_scan_interval_seconds, 0.1))

    def _query_trades_scoped(self, account_id: str | None):
        query_scoped = getattr(self.broker, "query_trades_scoped", None)
        if callable(query_scoped):
            return list(query_scoped(account_id=account_id))
        fills = list(self.broker.query_trades())
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]

    def _renew_claim_or_raise(self, state: _ClaimedSessionState, *, owner_id: str, requested_by: str) -> None:
        now_mono = time.monotonic()
        if now_mono - state.last_heartbeat_monotonic < self.config.operator.supervisor_heartbeat_interval_seconds:
            return
        renewed = self.execution_session_repository.renew_supervisor_claim(
            state.session_id,
            owner_id=owner_id,
            lease_expires_at=self._lease_expires_at(),
            now=now_iso(),
            supervisor_mode=state.mode if state.subscription is not None else "poll",
        )
        if not renewed:
            self._close_subscription(state)
            self.execution_session_repository.append_event(
                state.session_id,
                event_type="SUPERVISOR_OWNERSHIP_LOST",
                level="ERROR",
                payload={"owner_id": owner_id, "mode": state.mode},
            )
            self.audit_repository.write(
                run_id=None,
                trace_id=new_id("trace"),
                module="operator_supervisor",
                action="session_ownership_lost",
                entity_type="trade_session",
                entity_id=state.session_id,
                payload={"owner_id": owner_id, "mode": state.mode},
                operator=requested_by,
                level="ERROR",
            )
            raise RuntimeError(f"supervisor ownership lost: session_id={state.session_id}, owner_id={owner_id}")
        state.last_heartbeat_monotonic = now_mono
        self.execution_session_repository.append_event(
            state.session_id,
            event_type="SUPERVISOR_RENEWED",
            level="INFO",
            payload={"owner_id": owner_id, "mode": state.mode},
        )

    def _finalize_processed_state(self, state: _ClaimedSessionState, *, summary: OperatorSupervisorRunSummary) -> None:
        if state.finalized:
            return
        state.finalized = True
        summary.processed_session_ids.append(state.session_id)
        if state.fallback_used:
            summary.fallback_polled_session_ids.append(state.session_id)
        if state.last_result is not None and state.last_result.summary.status in _TERMINAL_SESSION_STATUSES:
            summary.completed_session_ids.append(state.session_id)
        if state.last_result is not None and state.last_result.summary.status in _TERMINAL_SESSION_STATUSES:
            self.execution_session_repository.append_event(
                state.session_id,
                event_type="SUPERVISOR_SESSION_TERMINAL",
                level="INFO",
                payload={
                    "status": state.last_result.summary.status.value,
                    "broker_event_cursor": state.last_result.summary.broker_event_cursor,
                    "supervisor_mode": state.last_result.summary.supervisor_mode,
                },
            )

    @staticmethod
    def _find_active_session_id(session_states: dict[str, _ClaimedSessionState]) -> str | None:
        for session_id, state in session_states.items():
            if not state.finalized:
                return session_id
        return next(iter(session_states), None)

    @staticmethod
    def _close_subscription(state: _ClaimedSessionState) -> None:
        if state.subscription is None:
            return
        state.subscription.close()
        state.subscription = None

    def _lease_expires_at(self) -> str:
        expires = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=self.config.operator.supervisor_lease_seconds)
        return expires.isoformat()

    def _resolve_supervisor_mode(self) -> str:
        configured = self.config.broker.event_source_mode
        if configured == "auto":
            return "subscribe" if self.broker.supports_execution_report_subscription() else "poll"
        return configured
