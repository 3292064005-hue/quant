"""风险与质量事件面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group


def build_risk_alert_panel(operations_snapshot: dict[str, Any]) -> object:
    """展示最近回测风险审计、导入质量事件与 operator 命令降级事件。"""
    latest_risk = operations_snapshot.get("ui_latest_risk_alerts") or {}
    latest_operator = operations_snapshot.get("ui_latest_operator_session") or {}
    observability = latest_operator.get("observability") or {}
    return build_page(
        "风险审计与质量事件",
        [
            build_key_value_group(
                "回测风控摘要",
                {
                    "run_id": latest_risk.get("run_id"),
                    "import_run_id": latest_risk.get("import_run_id"),
                    "audit_log_count": latest_risk.get("audit_log_count"),
                    "audit_module_counts": latest_risk.get("audit_module_counts"),
                    "audit_level_counts": latest_risk.get("audit_level_counts"),
                },
            ),
            build_table_group(
                "回测风控日志",
                latest_risk.get("risk_audit_logs", []),
                [("模块", "module"), ("动作", "action"), ("级别", "level"), ("实体", "entity_id"), ("详情", "payload")],
            ),
            build_table_group(
                "导入质量事件",
                latest_risk.get("import_quality_events", []),
                [("事件", "event_type"), ("级别", "level"), ("日期", "trade_date"), ("标的", "ts_code"), ("详情", "payload")],
            ),
            build_key_value_group(
                "Operator 降级摘要",
                {
                    "degraded_event_count": observability.get("degraded_event_count"),
                    "audit_write_failure_count": observability.get("audit_write_failure_count"),
                    "recovery_retry_failure_count": observability.get("recovery_retry_failure_count"),
                    "supervisor_event_count": observability.get("supervisor_event_count"),
                    "reconcile_event_count": observability.get("reconcile_event_count"),
                },
            ),
            build_table_group(
                "Operator 命令事件",
                latest_operator.get("events", []),
                [("事件类型", "event_type"), ("级别", "level"), ("详情", "payload"), ("时间", "created_at")],
            ),
            build_table_group(
                "最近降级事件",
                observability.get("recent_degraded_events", []),
                [("事件类型", "event_type"), ("级别", "level"), ("时间", "created_at"), ("详情", "payload")],
            ),
        ],
    )
