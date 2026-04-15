"""operator submit recovery 支持服务。"""
from __future__ import annotations

import logging
from datetime import date

from a_share_quant.domain.models import Fill, OrderRequest, TradeCommandEvent, TradeSessionResult, TradeSessionStatus
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_submission_preparation_service import OperatorSubmissionPreparationService
from a_share_quant.services.operator_submission_service import OperatorSubmissionService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService

logger = logging.getLogger(__name__)


class OperatorRecoveryService:
    """封装 submit 主链失败后的 recovery seed 与 reconciliation 重试。"""

    def __init__(
        self,
        *,
        execution_session_repository: ExecutionSessionRepository,
        order_repository: OrderRepository,
        submission_service: OperatorSubmissionService,
        reconciliation_service: TradeReconciliationService,
        runtime_mode: str,
        broker_provider: str,
    ) -> None:
        self.execution_session_repository = execution_session_repository
        self.order_repository = order_repository
        self.submission_service = submission_service
        self.reconciliation_service = reconciliation_service
        self.runtime_mode = runtime_mode
        self.broker_provider = broker_provider

    def persist_recovery_seed(
        self,
        *,
        session_id: str,
        command_type: str,
        command_source: str,
        requested_by: str,
        requested_trade_date: str | None,
        idempotency_key: str | None,
        risk_summary: dict,
        orders: list[OrderRequest],
        fills: list[Fill],
        events: list[TradeCommandEvent],
        status: TradeSessionStatus,
        error_message: str | None,
        account_id: str | None,
        created_at: str,
    ) -> None:
        if self.execution_session_repository.get(session_id) is not None:
            return
        seed_summary = OperatorSubmissionPreparationService.build_session_summary(
            session_id=session_id,
            command_type=command_type,
            command_source=command_source,
            requested_by=requested_by,
            requested_trade_date=requested_trade_date,
            idempotency_key=idempotency_key,
            risk_summary=risk_summary,
            order_count=len(orders),
            status=TradeSessionStatus.RUNNING,
            account_id=account_id,
            created_at=created_at,
            updated_at=created_at,
            runtime_mode=self.runtime_mode,
            broker_provider=self.broker_provider,
        )
        with self.order_repository.store.transaction():
            self.execution_session_repository.insert_session(seed_summary)
            self.execution_session_repository.append_events(events)
            self.order_repository.save_orders(None, orders, execution_session_id=session_id)
            if fills:
                self.order_repository.save_fills(None, fills, execution_session_id=session_id)
            self.execution_session_repository.update_session(
                session_id,
                status=status,
                submitted_count=self.submission_service.count_submitted_orders(orders),
                rejected_count=self.submission_service.count_rejected_orders(orders),
                risk_summary=risk_summary,
                error_message=error_message,
            )

    def attempt_recovery(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        submitted_orders: list[OrderRequest],
        fills: list[Fill],
        requested_by: str,
        terminal_message: str,
    ) -> TradeSessionResult | None:
        if not submitted_orders and not fills:
            return None
        try:
            return self.reconciliation_service.reconcile_session(
                session_id,
                expected_orders=list(submitted_orders or orders),
                requested_by=requested_by,
                failure_reason=terminal_message,
            )
        except Exception as exc:
            logger.exception(
                "trade session recovery attempt failed: session_id=%s requested_by=%s error=%s",
                session_id,
                requested_by,
                exc,
            )
            try:
                self.execution_session_repository.append_event(
                    session_id,
                    event_type="RECOVERY_RETRY_FAILED",
                    level="ERROR",
                    payload={
                        "error": str(exc),
                        "failure_reason": terminal_message,
                        "degradation_kind": "recovery_retry_failed",
                    },
                )
            except Exception:
                logger.exception("trade session recovery failure event append also failed: session_id=%s", session_id)
            return None
