"""operator trade 审计支持服务。"""
from __future__ import annotations

import logging
from typing import Any

from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import TradeCommandEvent
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.services.operator_session_event_service import OperatorSessionEventService

logger = logging.getLogger(__name__)


class OperatorTradeAuditService:
    """封装 trade session 审计写入与摘要构造。"""

    def __init__(
        self,
        *,
        audit_repository: AuditRepository,
        execution_session_repository: ExecutionSessionRepository,
        event_service: OperatorSessionEventService,
    ) -> None:
        self.audit_repository = audit_repository
        self.execution_session_repository = execution_session_repository
        self.event_service = event_service

    def write_best_effort(
        self,
        *,
        action: str,
        entity_id: str,
        payload: dict[str, Any],
        operator: str,
        level: str,
        session_id: str | None = None,
        lifecycle_events: list[TradeCommandEvent] | None = None,
    ) -> None:
        """最佳努力写入 trade session 审计。"""
        effective_payload = self.event_service.lifecycle_service.build_audit_payload(
            action=action,
            base_payload=payload,
            lifecycle_events=lifecycle_events,
            runtime_lane="operator_trade",
        )
        try:
            self.audit_repository.write(
                run_id=None,
                trace_id=new_id("trace"),
                module="trade_orchestrator",
                action=action,
                entity_type="trade_session",
                entity_id=entity_id,
                payload=effective_payload,
                operator=operator,
                level=level,
            )
        except Exception as exc:
            logger.warning(
                "trade session audit write failed: action=%s entity_id=%s session_id=%s error=%s",
                action,
                entity_id,
                session_id,
                exc,
            )
            if session_id is None:
                return
            try:
                self.execution_session_repository.append_event(
                    session_id,
                    event_type="AUDIT_WRITE_FAILED",
                    level="ERROR",
                    payload={
                        "action": action,
                        "entity_id": entity_id,
                        "error": str(exc),
                        "degradation_kind": "audit_write_failed",
                    },
                )
            except Exception:
                logger.exception(
                    "trade session audit fallback event append failed: action=%s entity_id=%s session_id=%s",
                    action,
                    entity_id,
                    session_id,
                )
