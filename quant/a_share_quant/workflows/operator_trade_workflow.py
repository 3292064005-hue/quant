"""paper/live operator 交易工作流。"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from datetime import date

from a_share_quant.domain.models import ExecutionIntentSubmissionResult, OrderRequest, TradeSessionResult
from a_share_quant.services.operator_supervisor_service import OperatorSupervisorRunSummary, OperatorSupervisorService
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService

_ResultT = TypeVar("_ResultT")


class OperatorTradeWorkflow:
    """对外暴露正式 operator submit / reconcile / sync / supervisor workflow。"""

    def __init__(
        self,
        orchestrator: TradeOrchestratorService,
        context,
        *,
        supervisor_service: OperatorSupervisorService | None = None,
        plugin_manager=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.supervisor_service = supervisor_service
        self.context = context
        self.plugin_manager = plugin_manager

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    def _run_with_hooks(self, workflow_name: str, payload: dict[str, Any], action: Callable[[], _ResultT]) -> _ResultT:
        """统一包裹 workflow lifecycle hooks。"""
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, workflow_name, payload)
        result: _ResultT | None = None
        error = None
        try:
            result = action()
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(self.context, workflow_name, payload, result=result, error=error)

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
        """通过统一 workflow 边界提交 operator 订单批次。"""
        payload = {
            "order_count": len(orders),
            "command_source": command_source,
            "requested_by": requested_by,
            "idempotency_key": idempotency_key,
            "account_id": account_id,
        }
        return self._run_with_hooks(
            "workflow.operator_trade",
            payload,
            lambda: self.orchestrator.submit_orders(
                orders,
                command_source=command_source,
                requested_by=requested_by,
                idempotency_key=idempotency_key,
                approved=approved,
                account_id=account_id,
            ),
        )

    def submit_research_signal(
        self,
        *,
        research_run_id: str,
        trade_date: date | None = None,
        command_source: str,
        requested_by: str | None = None,
        idempotency_key: str | None = None,
        approved: bool = False,
        account_id: str | None = None,
        strategy_id: str | None = None,
    ) -> ExecutionIntentSubmissionResult:
        """通过统一 workflow 边界提交 research signal_snapshot 晋级执行。"""
        payload = {
            "research_run_id": research_run_id,
            "trade_date": trade_date.isoformat() if trade_date is not None else None,
            "command_source": command_source,
            "requested_by": requested_by,
            "idempotency_key": idempotency_key,
            "account_id": account_id,
            "strategy_id": strategy_id,
        }
        return self._run_with_hooks(
            "workflow.operator_trade.signal",
            payload,
            lambda: self.orchestrator.submit_research_signal(
                research_run_id=research_run_id,
                trade_date=trade_date,
                command_source=command_source,
                requested_by=requested_by,
                idempotency_key=idempotency_key,
                approved=approved,
                account_id=account_id,
                strategy_id=strategy_id,
            ),
        )

    def reconcile_session(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        """显式恢复指定交易会话。"""
        payload = {"session_id": session_id, "requested_by": requested_by}
        return self._run_with_hooks(
            "workflow.operator_trade.reconcile",
            payload,
            lambda: self.orchestrator.reconcile_session(session_id, requested_by=requested_by),
        )

    def reconcile_latest_recovery_required(self, *, requested_by: str | None = None) -> TradeSessionResult:
        """恢复最近一个待回补会话。"""
        payload = {"requested_by": requested_by, "scope": "latest_recovery_required"}
        return self._run_with_hooks(
            "workflow.operator_trade.reconcile_latest",
            payload,
            lambda: self.orchestrator.reconcile_latest_recovery_required(requested_by=requested_by),
        )

    def sync_session_events(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        """同步指定交易会话的 broker 事件。"""
        payload = {"session_id": session_id, "requested_by": requested_by}
        return self._run_with_hooks(
            "workflow.operator_trade.sync",
            payload,
            lambda: self.orchestrator.sync_session_events(session_id, requested_by=requested_by),
        )

    def sync_latest_open_session(self, *, requested_by: str | None = None) -> TradeSessionResult:
        """同步最近一个 open session。"""
        payload = {"requested_by": requested_by, "scope": "latest_open_session"}
        return self._run_with_hooks(
            "workflow.operator_trade.sync_latest",
            payload,
            lambda: self.orchestrator.sync_latest_open_session(requested_by=requested_by),
        )

    def run_supervisor(
        self,
        *,
        requested_by: str | None = None,
        owner_id: str | None = None,
        account_id: str | None = None,
        session_id: str | None = None,
        max_loops: int | None = None,
        stop_when_idle: bool = False,
    ) -> OperatorSupervisorRunSummary:
        """通过 workflow 统一封装 operator supervisor 入口。"""
        if self.supervisor_service is None:
            raise RuntimeError("当前上下文未装配 OperatorSupervisorService")
        payload = {
            "requested_by": requested_by,
            "owner_id": owner_id,
            "account_id": account_id,
            "session_id": session_id,
            "max_loops": max_loops,
            "stop_when_idle": stop_when_idle,
        }
        return self._run_with_hooks(
            "workflow.operator_trade.supervisor",
            payload,
            lambda: self.supervisor_service.run_loop(
                requested_by=requested_by,
                owner_id=owner_id,
                account_id=account_id,
                session_id=session_id,
                max_loops=max_loops,
                stop_when_idle=stop_when_idle,
            ),
        )
