"""桌面 UI 只读投影构建。"""
from __future__ import annotations

from typing import Any


UI_SCHEMA_VERSION = 1


def build_runtime_check_projection(runtime_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """将 runtime checks 转为桌面表格稳定契约。"""
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
    """将 provider/workflow/component registry 摘要打平为 UI 可消费结构。"""
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
    """将 research run 记录打平为 recent-runs / UI 列表稳定契约。"""
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


def build_ui_snapshot_projection(
    *,
    runtime_results: list[dict[str, Any]],
    available_provider_details: list[dict[str, Any]],
    available_workflow_details: list[dict[str, Any]],
    recent_research_runs: list[dict[str, Any]],
) -> dict[str, Any]:
    """生成桌面 UI 所需的稳定读模型。"""
    return {
        "ui_schema_version": UI_SCHEMA_VERSION,
        "ui_runtime_checks": build_runtime_check_projection(runtime_results),
        "ui_available_provider_details": build_component_projection(available_provider_details),
        "ui_available_workflow_details": build_component_projection(available_workflow_details),
        "ui_recent_research_runs": build_recent_research_run_projection(recent_research_runs),
    }
