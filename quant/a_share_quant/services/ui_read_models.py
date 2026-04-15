"""桌面 UI 只读投影构建。"""
from __future__ import annotations

from typing import Any


UI_SCHEMA_VERSION = 2
PLUGIN_EVENT_SCHEMA_VERSION = 1


def build_runtime_check_projection(runtime_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in runtime_results:
        capability = item.get("capability") or {}
        rows.append(
            {
                "check": item.get("name"),
                "status": "PASS" if item.get("ok") else "FAIL",
                "message": item.get("message"),
                "details": item.get("details") or {},
                "capability": capability,
                "config_ok": bool(capability.get("config_ok")),
                "boundary_ok": bool(capability.get("boundary_ok")),
                "client_contract_ok": bool(capability.get("client_contract_ok")),
                "operable_ok": bool(capability.get("operable_ok")),
            }
        )
    return rows


def build_component_projection(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        descriptor = entry.get("descriptor") or {}
        metadata = entry.get("metadata") or {}
        rows.append(
            {
                "name": entry.get("name"),
                "component_type": descriptor.get("component_type") or metadata.get("component_type"),
                "contract_kind": descriptor.get("contract_kind") or metadata.get("contract_kind"),
                "input_contract": descriptor.get("input_contract"),
                "output_contract": descriptor.get("output_contract"),
                "callable_path": descriptor.get("callable_path"),
                "tags": descriptor.get("tags") or [],
                "metadata": metadata,
            }
        )
    return rows


def build_recent_research_run_projection(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in runs:
        request = row.get("request") or {}
        result = row.get("result") or {}
        feature_spec = result.get("feature_spec") or {}
        experiment = result.get("experiment") or {}
        selected_symbols = result.get("selected_symbols") or []
        top_symbols = result.get("top_symbols") or (result.get("feature") or {}).get("top_symbols") or []
        signal_preview = [item.get("ts_code") for item in selected_symbols if isinstance(item, dict) and item.get("ts_code")]
        lookback = feature_spec.get("params", {}).get("lookback") if isinstance(feature_spec.get("params"), dict) else None
        if lookback is None:
            lookback = request.get("lookback") or experiment.get("lookback")
        top_n = result.get("top_n") or request.get("top_n") or experiment.get("top_n")
        rows.append(
            {
                "research_run_id": row.get("research_run_id"),
                "artifact_type": row.get("artifact_type"),
                "dataset_version_id": row.get("dataset_version_id"),
                "dataset_digest": row.get("dataset_digest"),
                "created_at": row.get("created_at"),
                "research_session_id": row.get("research_session_id"),
                "step_name": row.get("step_name"),
                "is_primary_run": bool(row.get("is_primary_run", True)),
                "feature_name": feature_spec.get("name") or experiment.get("feature_name") or request.get("feature_name"),
                "lookback": lookback,
                "top_n": top_n,
                "value_count": result.get("value_count") or (result.get("feature") or {}).get("value_count"),
                "selected_count": len(selected_symbols) if isinstance(selected_symbols, list) else (result.get("signal") or {}).get("selected_count"),
                "top_symbols": top_symbols,
                "signal_snapshot": ", ".join(signal_preview[:5]) if signal_preview else None,
                "ui_role": "primary" if row.get("is_primary_run", True) else "internal_step",
            }
        )
    return rows


def build_plugin_lifecycle_projection(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in events:
        rows.append(
            {
                "schema_version": row.get("schema_version") or PLUGIN_EVENT_SCHEMA_VERSION,
                "event": row.get("event"),
                "event_type": row.get("event_type") or row.get("event"),
                "source": row.get("source") or "plugin_manager",
                "level": row.get("level") or ("ERROR" if str(row.get("event", "")).endswith("_error") else "INFO"),
                "plugin_name": row.get("plugin_name"),
                "workflow_name": row.get("workflow_name"),
                "created_at": row.get("created_at"),
                "payload": dict(row.get("payload") or {}),
            }
        )
    return rows


def build_runtime_event_projection(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.get("event_id"),
                "source_domain": event.get("source_domain"),
                "stream_scope": event.get("stream_scope"),
                "stream_id": event.get("stream_id"),
                "event_type": event.get("event_type"),
                "stage": event.get("stage") or (event.get("payload") or {}).get("stage"),
                "runtime_lane": event.get("runtime_lane") or ((event.get("payload") or {}).get("lifecycle") or {}).get("order_intent", {}).get("runtime_lane"),
                "level": event.get("level") or "INFO",
                "payload": dict(event.get("payload") or {}),
                "created_at": event.get("occurred_at") or event.get("created_at"),
            }
        )
    return rows


def build_operator_session_projection(session: dict[str, Any] | None) -> dict[str, Any] | None:
    if session is None:
        return None
    projected = dict(session)
    projected["events"] = build_runtime_event_projection(session.get("events") or [])
    observability = dict(session.get("observability") or {})
    observability["recent_degraded_events"] = build_runtime_event_projection(observability.get("recent_degraded_events") or [])
    projected["observability"] = observability
    return projected


def build_import_run_projection(latest_import: dict[str, Any] | None) -> dict[str, Any] | None:
    if latest_import is None:
        return None
    return {
        "import_run_id": latest_import.get("import_run_id"),
        "source": latest_import.get("source"),
        "status": latest_import.get("status"),
        "started_at": latest_import.get("started_at"),
        "finished_at": latest_import.get("finished_at"),
        "securities_count": latest_import.get("securities_count"),
        "calendar_count": latest_import.get("calendar_count"),
        "bars_count": latest_import.get("bars_count"),
        "degradation_flags": list(latest_import.get("degradation_flags") or []),
        "warnings": list(latest_import.get("warnings") or []),
    }


def build_quality_event_projection(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in events:
        rows.append(
            {
                "event_type": row.get("event_type"),
                "level": row.get("level"),
                "trade_date": row.get("trade_date"),
                "ts_code": row.get("ts_code"),
                "payload": dict(row.get("payload") or {}),
            }
        )
    return rows


def build_backtest_run_projection(latest_run: dict[str, Any] | None) -> dict[str, Any] | None:
    if latest_run is None:
        return None
    return {
        "run_id": latest_run.get("run_id"),
        "strategy_id": latest_run.get("strategy_id"),
        "status": latest_run.get("status"),
        "status_breakdown": dict(latest_run.get("status_breakdown") or {}),
        "runtime_mode": latest_run.get("runtime_mode"),
        "dataset_version_id": latest_run.get("dataset_version_id"),
        "import_run_id": latest_run.get("import_run_id"),
        "dataset_digest": latest_run.get("dataset_digest"),
    }


def build_execution_summary_projection(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "run_id": summary.get("run_id"),
        "order_count": summary.get("order_count"),
        "fill_count": summary.get("fill_count"),
        "fill_notional": summary.get("fill_notional"),
        "order_status_counts": dict(summary.get("order_status_counts") or {}),
        "recent_orders": list(summary.get("recent_orders") or []),
        "recent_fills": list(summary.get("recent_fills") or []),
    }


def build_risk_summary_projection(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "run_id": summary.get("run_id"),
        "import_run_id": summary.get("import_run_id"),
        "audit_log_count": summary.get("audit_log_count"),
        "audit_module_counts": dict(summary.get("audit_module_counts") or {}),
        "audit_level_counts": dict(summary.get("audit_level_counts") or {}),
        "risk_audit_logs": list(summary.get("risk_audit_logs") or []),
        "import_quality_events": build_quality_event_projection(summary.get("import_quality_events") or []),
    }


def build_report_replay_projection(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "run_id": summary.get("run_id"),
        "report_path": summary.get("report_path"),
        "artifact_status": summary.get("artifact_status"),
        "artifact_errors": list(summary.get("artifact_errors") or []),
        "signal_source_run_id": summary.get("signal_source_run_id"),
        "signal_source_artifact_type": summary.get("signal_source_artifact_type"),
        "promotion_package": dict(summary.get("promotion_package") or {}),
        "run_event_summary": dict(summary.get("run_event_summary") or {}),
        "related_research_run_summaries": build_recent_research_run_projection(summary.get("related_research_run_summaries") or []),
    }


def build_ui_snapshot_projection(
    *,
    runtime_results: list[dict[str, Any]],
    available_provider_details: list[dict[str, Any]],
    available_workflow_details: list[dict[str, Any]],
    recent_research_runs: list[dict[str, Any]],
    latest_import_run: dict[str, Any] | None = None,
    latest_import_quality_events: list[dict[str, Any]] | None = None,
    latest_backtest_run: dict[str, Any] | None = None,
    latest_execution_summary: dict[str, Any] | None = None,
    latest_risk_alerts: dict[str, Any] | None = None,
    latest_report_replay_summary: dict[str, Any] | None = None,
    latest_operator_session: dict[str, Any] | None = None,
    plugin_lifecycle_events: list[dict[str, Any]] | None = None,
    recent_runtime_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "ui_schema_version": UI_SCHEMA_VERSION,
        "ui_runtime_checks": build_runtime_check_projection(runtime_results),
        "ui_available_provider_details": build_component_projection(available_provider_details),
        "ui_available_workflow_details": build_component_projection(available_workflow_details),
        "ui_recent_research_runs": build_recent_research_run_projection(recent_research_runs),
        "ui_latest_import_run": build_import_run_projection(latest_import_run),
        "ui_latest_import_quality_events": build_quality_event_projection(latest_import_quality_events or []),
        "ui_latest_backtest_run": build_backtest_run_projection(latest_backtest_run),
        "ui_latest_execution_summary": build_execution_summary_projection(latest_execution_summary),
        "ui_latest_risk_alerts": build_risk_summary_projection(latest_risk_alerts),
        "ui_latest_report_replay_summary": build_report_replay_projection(latest_report_replay_summary),
        "ui_latest_operator_session": build_operator_session_projection(latest_operator_session),
        "ui_plugin_lifecycle_events": build_plugin_lifecycle_projection(plugin_lifecycle_events or []),
        "ui_recent_runtime_events": build_runtime_event_projection(recent_runtime_events or []),
    }
