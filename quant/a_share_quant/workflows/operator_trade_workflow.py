"""paper/live operator 交易工作流。"""
from __future__ import annotations

from a_share_quant.domain.models import OrderRequest, TradeSessionResult
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService


class OperatorTradeWorkflow:
    """对外暴露正式 operator submit/sync workflow。"""

    def __init__(self, orchestrator: TradeOrchestratorService, context, *, plugin_manager=None) -> None:
        self.orchestrator = orchestrator
        self.context = context
        self.plugin_manager = plugin_manager

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    def submit_orders(
        self,
        orders: list[OrderRequest],
        *,
        command_source: str,
        requested_by: str | None = None,
        idempotency_key: str | None = None,
        approved: bool = False,
        account_id: str | None = None,
    ) -> TradeSessionResult:
        payload = {
            "order_count": len(orders),
            "command_source": command_source,
            "requested_by": requested_by,
            "idempotency_key": idempotency_key,
            "account_id": account_id,
        }
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.operator_trade", payload)
        result = None
        error = None
        try:
            result = self.orchestrator.submit_orders(
                orders,
                command_source=command_source,
                requested_by=requested_by,
                idempotency_key=idempotency_key,
                approved=approved,
                account_id=account_id,
            )
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(self.context, "workflow.operator_trade", payload, result=result, error=error)

    def sync_session_events(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        payload = {"session_id": session_id, "requested_by": requested_by}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.operator_trade.sync", payload)
        result = None
        error = None
        try:
            result = self.orchestrator.sync_session_events(session_id, requested_by=requested_by)
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(self.context, "workflow.operator_trade.sync", payload, result=result, error=error)
