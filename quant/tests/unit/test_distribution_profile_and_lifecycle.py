from __future__ import annotations

from pathlib import Path

import json
import pytest

from a_share_quant.config.models import AppConfig
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import BacktestResult, OrderRequest, OrderSide, RunArtifacts, TradeSessionStatus
from a_share_quant.execution.order_lifecycle_service import OrderLifecycleEventService
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.run_query_service import RunQueryService
from a_share_quant.storage.sqlite_store import SQLiteStore


def _build_store(tmp_path: Path) -> SQLiteStore:
    store = SQLiteStore(str(tmp_path / "repo.db"))
    store.init_schema(load_schema_sql())
    return store


def _build_order(*, with_run_id: bool) -> OrderRequest:
    from datetime import date

    return OrderRequest(
        order_id="ord-1",
        trade_date=date(2026, 1, 5),
        strategy_id="demo.strategy",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=200,
        reason="rebalance",
        run_id="run-1" if with_run_id else None,
        account_id="acct-A",
    )


def test_distribution_profile_workstation_capabilities() -> None:
    from a_share_quant.app.distribution_profile_contract import get_distribution_profile_spec

    cfg = AppConfig.model_validate({"app": {"distribution_profile": "workstation", "runtime_mode": "research_backtest"}})
    capabilities = cfg.distribution_capabilities()
    assert capabilities == get_distribution_profile_spec("workstation").capabilities
    assert capabilities["supports_ui"] is True
    assert capabilities["supports_research_workflow"] is True
    assert capabilities["enforces_strict_market_contract"] is False


def test_distribution_profile_production_requires_strict_contracts() -> None:
    with pytest.raises(ValueError):
        AppConfig.model_validate(
            {
                "app": {"distribution_profile": "production", "runtime_mode": "paper_trade"},
                "data": {"calendar_policy": "demo", "allow_degraded_data": True, "fail_on_degraded_data": False},
                "broker": {"provider": "qmt", "strict_contract_mapping": False},
                "operator": {"require_approval": False},
            }
        )


def test_lifecycle_service_emits_same_contract_shape_for_backtest_and_operator() -> None:
    service = OrderLifecycleEventService()
    backtest_order = _build_order(with_run_id=True)
    operator_order = _build_order(with_run_id=False)

    backtest_event = service.build_lifecycle_event(
        event_type="ORDER_SUBMITTED",
        level="INFO",
        order=backtest_order,
        payload=service.build_order_payload(backtest_order, runtime_lane="research_backtest"),
        runtime_lane="research_backtest",
        broker_provider="MockBroker",
    )
    operator_event = service.build_lifecycle_event(
        event_type="ORDER_SUBMITTED",
        level="INFO",
        order=operator_order,
        payload=service.build_order_payload(operator_order, session_id="session-1", runtime_lane="operator_trade"),
        runtime_lane="operator_trade",
        broker_provider="qmt",
        session_id="session-1",
    )

    backtest_payload = service.lifecycle_event_to_payload(backtest_event)
    operator_payload = service.lifecycle_event_to_payload(operator_event)

    for payload, expected_lane in [(backtest_payload, "research_backtest"), (operator_payload, "operator_trade")]:
        lifecycle = payload["lifecycle"]
        assert set(lifecycle.keys()) >= {"order_intent"}
        assert lifecycle["order_intent"]["order_id"] == "ord-1"
        assert lifecycle["order_intent"]["runtime_lane"] == expected_lane
        assert payload["requested_quantity"] == 200
        assert payload["ts_code"] == "600000.SH"


def test_execution_session_repository_backfills_runtime_stream_from_legacy_events(tmp_path: Path) -> None:
    store = _build_store(tmp_path)
    repo = ExecutionSessionRepository(store)
    session = repo.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2026-01-05",
        idempotency_key=None,
        status=TradeSessionStatus.RUNNING,
        account_id="acct-A",
    )
    store.execute(
        "INSERT INTO trade_command_events (event_id, session_id, event_type, level, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("legacy-1", session.session_id, "ORDER_ACCEPTED", "INFO", '{"order_id":"ord-1"}', "2026-01-05T09:30:00"),
    )

    events = repo.list_events(session.session_id)
    assert events[0].event_id == "legacy-1"
    runtime_events = repo.runtime_event_repository.list_stream_events(
        source_domain="operator",
        stream_scope="trade_session",
        stream_id=session.session_id,
        newest_first=False,
    )
    assert runtime_events[0]["event_id"] == "legacy-1"


def test_run_query_service_operator_observability_summary(tmp_path: Path) -> None:
    store = _build_store(tmp_path)
    execution_repo = ExecutionSessionRepository(store)
    session = execution_repo.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2026-01-05",
        idempotency_key=None,
        status=TradeSessionStatus.RUNNING,
        account_id="acct-A",
    )
    execution_repo.append_event(session.session_id, event_type="SUPERVISOR_ERROR", level="ERROR", payload={"error": "timeout"})
    execution_repo.append_event(session.session_id, event_type="AUDIT_WRITE_FAILED", level="ERROR", payload={"error": "disk full"})
    execution_repo.append_event(session.session_id, event_type="RECOVERY_RETRY_FAILED", level="ERROR", payload={"error": "network"})
    execution_repo.append_event(session.session_id, event_type="SESSION_SYNC_COMPLETED", level="INFO", payload={})

    query_service = RunQueryService(
        backtest_run_repository=BacktestRunRepository(store),
        order_repository=OrderRepository(store),
        audit_repository=AuditRepository(store),
        data_import_repository=DataImportRepository(store),
        research_run_repository=ResearchRunRepository(store),
        execution_session_repository=execution_repo,
        runtime_event_repository=execution_repo.runtime_event_repository,
    )

    latest_session = query_service.build_latest_snapshot()["latest_operator_session"]
    observability = latest_session["observability"]
    assert observability["degraded_event_count"] >= 3
    assert observability["audit_write_failure_count"] == 1
    assert observability["recovery_retry_failure_count"] == 1
    assert observability["supervisor_event_count"] >= 1
    assert observability["reconcile_event_count"] >= 2


def test_lifecycle_service_replays_cross_lane_invariant() -> None:
    service = OrderLifecycleEventService()
    backtest_order = _build_order(with_run_id=True)
    operator_order = _build_order(with_run_id=False)
    backtest_submitted = service.build_lifecycle_event(event_type="ORDER_SUBMITTED", level="INFO", order=backtest_order, payload={**service.build_order_payload(backtest_order, runtime_lane="research_backtest"), "status": "SUBMITTED"}, runtime_lane="research_backtest")
    backtest_filled = service.build_lifecycle_event(event_type="ORDER_FILLED", level="INFO", order=backtest_order, payload={**service.build_order_payload(backtest_order, runtime_lane="research_backtest"), "status": "FILLED", "filled_quantity": 200, "remaining_quantity": 0}, runtime_lane="research_backtest", previous_state=backtest_submitted.state_after)
    operator_submitted = service.build_lifecycle_event(event_type="ORDER_SUBMITTED", level="INFO", order=operator_order, payload={**service.build_order_payload(operator_order, session_id="session-1", runtime_lane="operator_trade"), "status": "SUBMITTED"}, runtime_lane="operator_trade", session_id="session-1")
    operator_filled = service.build_lifecycle_event(event_type="ORDER_FILLED", level="INFO", order=operator_order, payload={**service.build_order_payload(operator_order, session_id="session-1", runtime_lane="operator_trade"), "status": "FILLED", "filled_quantity": 200, "remaining_quantity": 0}, runtime_lane="operator_trade", session_id="session-1", previous_state=operator_submitted.state_after)
    invariant = service.assert_cross_lane_invariant([backtest_submitted.payload | {"event_type": backtest_submitted.event_type, "occurred_at": backtest_submitted.created_at}], [operator_submitted.payload | {"event_type": operator_submitted.event_type, "occurred_at": operator_submitted.created_at}])
    assert invariant.status == "SUBMITTED"
    final_invariant = service.assert_cross_lane_invariant([backtest_submitted.payload | {"event_type": backtest_submitted.event_type, "occurred_at": backtest_submitted.created_at}, backtest_filled.payload | {"event_type": backtest_filled.event_type, "occurred_at": backtest_filled.created_at}], [operator_submitted.payload | {"event_type": operator_submitted.event_type, "occurred_at": operator_submitted.created_at}, operator_filled.payload | {"event_type": operator_filled.event_type, "occurred_at": operator_filled.created_at}])
    assert final_invariant.status == "FILLED"


def test_lifecycle_summary_is_embedded_into_report_payload(tmp_path: Path) -> None:
    service = OrderLifecycleEventService()
    order = _build_order(with_run_id=True)
    submitted = service.build_lifecycle_event(event_type="ORDER_SUBMITTED", level="INFO", order=order, payload={**service.build_order_payload(order, runtime_lane="research_backtest"), "status": "SUBMITTED"}, runtime_lane="research_backtest")
    filled = service.build_lifecycle_event(event_type="ORDER_FILLED", level="INFO", order=order, payload={**service.build_order_payload(order, runtime_lane="research_backtest"), "status": "FILLED", "filled_quantity": 200, "remaining_quantity": 0}, runtime_lane="research_backtest", previous_state=submitted.state_after)
    report_service = ReportService(str(tmp_path), "{strategy_id}_{run_id}.json")
    result = BacktestResult(strategy_id="demo.strategy", run_id="run-1", artifacts=RunArtifacts(), run_events=[submitted.payload | {"event_type": submitted.event_type, "occurred_at": submitted.created_at}, filled.payload | {"event_type": filled.event_type, "occurred_at": filled.created_at}])
    report_paths = report_service.write_backtest_report(result)
    payload = json.loads(report_paths[0].read_text(encoding="utf-8"))
    assert payload["lifecycle_summary"]["event_count"] == 2
    assert payload["lifecycle_summary"]["terminal_statuses"]["FILLED"] == 1
    assert payload["run_event_summary"]["lifecycle_summary"]["terminal_statuses"]["FILLED"] == 1


def test_lifecycle_audit_payload_carries_terminal_status_summary() -> None:
    service = OrderLifecycleEventService()
    order = _build_order(with_run_id=False)
    submitted = service.build_lifecycle_event(event_type="ORDER_SUBMITTED", level="INFO", order=order, payload={**service.build_order_payload(order, runtime_lane="operator_trade"), "status": "SUBMITTED"}, runtime_lane="operator_trade", session_id="session-1")
    rejected = service.build_lifecycle_event(event_type="ORDER_REJECTED", level="ERROR", order=order, payload={**service.build_order_payload(order, runtime_lane="operator_trade", session_id="session-1"), "status": "EXECUTION_REJECTED", "reason": "broker rejected"}, runtime_lane="operator_trade", session_id="session-1", previous_state=submitted.state_after)
    payload = service.build_audit_payload(action="session_failed", base_payload={"status": "FAILED"}, lifecycle_events=[service.lifecycle_event_to_trade_command_event("session-1", submitted), service.lifecycle_event_to_trade_command_event("session-1", rejected)], runtime_lane="operator_trade")
    assert payload["lifecycle_summary"]["event_count"] == 2
    assert payload["lifecycle_summary"]["terminal_statuses"]["EXECUTION_REJECTED"] == 1
