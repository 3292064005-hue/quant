"""operator submit 前置准备服务。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import OrderRequest, TradeSessionResult, TradeSessionStatus, TradeSessionSummary
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService
from a_share_quant.services.operator_session_query_service import OperatorSessionQueryService
from a_share_quant.services.operator_submission_service import OperatorSubmissionService
from a_share_quant.services.operator_trade_validation_service import OperatorTradeValidationService


@dataclass(slots=True)
class PreparedOperatorSubmission:
    """提交主链进入 broker 前的正式准备结果。"""

    session_id: str
    command_type: str
    command_source: str
    requested_by: str
    idempotency_key: str | None
    captured_at: str
    trade_date: date
    account_id: str | None
    risk_summary: dict[str, Any]
    orders: list[OrderRequest]
    accepted_orders: list[OrderRequest]
    rejected_orders: list[OrderRequest]
    initial_summary: TradeSessionSummary
    initial_events: list


class OperatorSubmissionPreparationService:
    """处理 operator submit 的前置校验、幂等与初始事件准备。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        order_repository: OrderRepository,
        execution_session_repository: ExecutionSessionRepository,
        validation_service: OperatorTradeValidationService,
        event_service: OperatorSessionEventService,
        query_service: OperatorSessionQueryService,
        submission_service: OperatorSubmissionService,
    ) -> None:
        self.config = config
        self.broker = broker
        self.order_repository = order_repository
        self.execution_session_repository = execution_session_repository
        self.validation_service = validation_service
        self.event_service = event_service
        self.query_service = query_service
        self.submission_service = submission_service

    def resolve_idempotent_session(self, idempotency_key: str | None) -> TradeSessionResult | None:
        if not idempotency_key:
            return None
        existing = self.execution_session_repository.get_by_idempotency_key(idempotency_key)
        if existing is None:
            return None
        return TradeSessionResult(
            summary=existing,
            orders=self.query_service.list_session_orders(existing.session_id),
            fills=self.query_service.list_session_fills(existing.session_id),
            events=self.execution_session_repository.list_events(existing.session_id),
            replayed=True,
        )

    def prepare_submission(
        self,
        orders: list[OrderRequest],
        *,
        command_source: str,
        command_type: str,
        requested_by: str | None,
        idempotency_key: str | None,
        approved: bool,
        account_id: str | None,
        plugin_manager=None,
        plugin_context=None,
        now_iso_value: str,
    ) -> PreparedOperatorSubmission:
        """对 operator submit 主链做进入 broker 前的全部正式准备。"""
        if self.config.app.runtime_mode not in {"paper_trade", "live_trade"}:
            raise ValueError(
                f"TradeOrchestratorService 仅支持 paper/live lane；收到 app.runtime_mode={self.config.app.runtime_mode}"
            )
        if not orders:
            raise ValueError("operator trade 提交不能为空")
        if len(orders) > self.config.operator.max_batch_orders:
            raise ValueError(
                f"单次 operator trade 数量超过上限: {len(orders)} > {self.config.operator.max_batch_orders}"
            )
        if self.config.operator.require_approval and not approved:
            raise ValueError("当前配置要求 operator 显式批准；请传入 approved=True")
        if self.config.risk.kill_switch:
            raise ValueError("risk.kill_switch 已开启，禁止提交 operator trade")
        if not self.broker.heartbeat():
            raise RuntimeError("broker heartbeat 失败，禁止提交 operator trade")

        effective_requested_by = (requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by
        resolved_account_id = self.resolve_account_id(account_id)
        self.validation_service.bind_orders_account_id(orders, resolved_account_id)
        self.normalize_operator_order_ids(orders)
        trade_date = self.validation_service.resolve_trade_date(orders)
        risk_summary, accepted_orders, rejected_orders = self.validation_service.pre_trade_validate(orders)
        self._emit_risk_decisions(orders, rejected_orders, plugin_manager=plugin_manager, plugin_context=plugin_context)

        session_id = new_id("session")
        summary = self.build_session_summary(
            session_id=session_id,
            command_type=command_type,
            command_source=command_source,
            requested_by=effective_requested_by,
            requested_trade_date=trade_date.isoformat(),
            idempotency_key=idempotency_key,
            risk_summary=risk_summary,
            order_count=len(orders),
            status=TradeSessionStatus.RUNNING,
            account_id=resolved_account_id,
            created_at=now_iso_value,
            updated_at=now_iso_value,
            runtime_mode=self.config.app.runtime_mode,
            broker_provider=self.config.broker.provider,
        )
        events = [
            self.event_service.new_session_event(
                session_id,
                event_type="SESSION_CREATED",
                level="INFO",
                payload={
                    "order_count": len(orders),
                    "command_type": command_type,
                    "command_source": command_source,
                    "account_id": resolved_account_id,
                },
                created_at=now_iso_value,
            )
        ]
        events.extend(self.event_service.build_order_intent_events(session_id, orders, rejected_orders))
        return PreparedOperatorSubmission(
            session_id=session_id,
            command_type=command_type,
            command_source=command_source,
            requested_by=effective_requested_by,
            idempotency_key=idempotency_key,
            captured_at=now_iso_value,
            trade_date=trade_date,
            account_id=resolved_account_id,
            risk_summary=risk_summary,
            orders=orders,
            accepted_orders=accepted_orders,
            rejected_orders=rejected_orders,
            initial_summary=summary,
            initial_events=events,
        )

    def resolve_account_id(self, account_id: str | None) -> str | None:
        candidate = (account_id or self.config.broker.account_id or "").strip() or None
        allowed = set(self.config.broker.allowed_account_ids or [])
        if self.config.broker.account_id:
            allowed.add(self.config.broker.account_id)
        if candidate is not None and allowed and candidate not in allowed:
            raise ValueError(f"account_id 不在允许列表内: {candidate}; allowed={sorted(allowed)}")
        return candidate

    def normalize_operator_order_ids(self, orders: list[OrderRequest]) -> None:
        assigned_ids: set[str] = set()
        for order in orders:
            candidate = str(order.order_id).strip() if order.order_id else ""
            needs_reissue = not candidate or candidate in assigned_ids or self.order_repository.get_order_by_id(candidate) is not None
            if needs_reissue:
                candidate = self._generate_unique_operator_order_id(assigned_ids)
                order.order_id = candidate
            assigned_ids.add(candidate)

    def _generate_unique_operator_order_id(self, assigned_ids: set[str]) -> str:
        while True:
            candidate = new_id("operator_order")
            if candidate in assigned_ids:
                continue
            if self.order_repository.get_order_by_id(candidate) is not None:
                continue
            return candidate

    @staticmethod
    def build_session_summary(
        *,
        session_id: str,
        command_type: str,
        command_source: str,
        requested_by: str,
        requested_trade_date: str | None,
        idempotency_key: str | None,
        risk_summary: dict[str, Any],
        order_count: int,
        status: TradeSessionStatus,
        account_id: str | None,
        created_at: str,
        updated_at: str,
        runtime_mode: str | None = None,
        broker_provider: str | None = None,
    ) -> TradeSessionSummary:
        return TradeSessionSummary(
            session_id=session_id,
            runtime_mode=runtime_mode or "unknown",
            broker_provider=broker_provider or "unknown",
            command_type=command_type,
            command_source=command_source,
            requested_by=requested_by,
            status=status,
            idempotency_key=idempotency_key,
            requested_trade_date=requested_trade_date,
            risk_summary=dict(risk_summary),
            order_count=order_count,
            submitted_count=0,
            rejected_count=0,
            account_id=account_id,
            broker_event_cursor=None,
            last_synced_at=None,
            supervisor_owner=None,
            supervisor_lease_expires_at=None,
            supervisor_mode=None,
            last_supervised_at=None,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _emit_risk_decisions(self, orders: list[OrderRequest], rejected_orders: list[OrderRequest], *, plugin_manager=None, plugin_context=None) -> None:
        if plugin_manager is None:
            return
        for order in orders:
            plugin_manager.emit_risk_decision(
                plugin_context,
                order.order_id,
                {
                    "passed": order not in rejected_orders,
                    "account_id": order.account_id,
                    "status": order.status.value,
                    "runtime_lane": "operator_trade",
                },
            )
