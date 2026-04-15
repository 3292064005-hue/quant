"""读模型 source-of-truth 约束。"""
from __future__ import annotations

from typing import Any


SNAPSHOT_SOURCE_OF_TRUTH = {
    "latest_import_run": "data_import_runs",
    "latest_import_quality_events": "data_import_quality_events",
    "latest_backtest_run": "backtest_runs.run_manifest_json",
    "latest_execution_summary": "orders + fills",
    "latest_risk_alerts": "audit_logs + data_import_quality_events",
    "latest_report_replay_summary": "backtest_runs.run_manifest_json + research_runs + research_run_edges",
    "recent_research_runs": "research_runs + research_run_edges",
    "latest_operator_session": "trade_sessions + trade_command_events + runtime_events",
    "recent_runtime_events": "runtime_events",
}

OPERATOR_SOURCE_OF_TRUTH = {
    "account_views": "broker query + operator_account_snapshots + operator_position_snapshots",
    "latest_runs": "RunQueryService.build_latest_snapshot",
    "observability": "runtime_events",
}


def build_snapshot_source_of_truth(*, operator_mode: bool = False) -> dict[str, Any]:
    payload = {
        "schema_version": 1,
        "snapshot": dict(SNAPSHOT_SOURCE_OF_TRUTH),
    }
    if operator_mode:
        payload["operator"] = dict(OPERATOR_SOURCE_OF_TRUTH)
    return payload
