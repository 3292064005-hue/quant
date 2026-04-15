from __future__ import annotations

from dataclasses import dataclass

from a_share_quant.domain.models import TradeSessionResult
from a_share_quant.workflows.operator_trade_workflow import OperatorTradeWorkflow


@dataclass
class _Summary:
    session_id: str = "session_1"
    status: object = "COMPLETED"


class _FakePluginManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def emit_before_workflow_run(self, _context, workflow_name: str, _payload):
        self.calls.append(("before", workflow_name))

    def emit_after_workflow_run(self, _context, workflow_name: str, _payload, result=None, error=None):
        self.calls.append(("after", workflow_name))


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def reconcile_session(self, session_id: str, *, requested_by=None):
        self.calls.append(("reconcile_session", (session_id,), {"requested_by": requested_by}))
        return TradeSessionResult(summary=_Summary())

    def sync_latest_open_session(self, *, requested_by=None):
        self.calls.append(("sync_latest_open_session", tuple(), {"requested_by": requested_by}))
        return TradeSessionResult(summary=_Summary())


class _FakeSupervisorService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run_loop(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True, **kwargs}


def test_operator_trade_workflow_routes_reconcile_sync_latest_and_supervisor() -> None:
    plugin_manager = _FakePluginManager()
    orchestrator = _FakeOrchestrator()
    supervisor = _FakeSupervisorService()
    workflow = OperatorTradeWorkflow(orchestrator, context=object(), supervisor_service=supervisor, plugin_manager=plugin_manager)

    workflow.reconcile_session("session_target", requested_by="tester")
    workflow.sync_latest_open_session(requested_by="tester")
    summary = workflow.run_supervisor(requested_by="tester", owner_id="owner-1", max_loops=1, stop_when_idle=True)

    assert orchestrator.calls == [
        ("reconcile_session", ("session_target",), {"requested_by": "tester"}),
        ("sync_latest_open_session", tuple(), {"requested_by": "tester"}),
    ]
    assert supervisor.calls == [{"requested_by": "tester", "owner_id": "owner-1", "account_id": None, "session_id": None, "max_loops": 1, "stop_when_idle": True}]
    assert summary["ok"] is True
    assert plugin_manager.calls == [
        ("before", "workflow.operator_trade.reconcile"),
        ("after", "workflow.operator_trade.reconcile"),
        ("before", "workflow.operator_trade.sync_latest"),
        ("after", "workflow.operator_trade.sync_latest"),
        ("before", "workflow.operator_trade.supervisor"),
        ("after", "workflow.operator_trade.supervisor"),
    ]
