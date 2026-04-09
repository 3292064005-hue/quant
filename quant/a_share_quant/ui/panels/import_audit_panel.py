"""导入审计面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group


def build_import_audit_panel(operations_snapshot: dict[str, Any]) -> object:
    """展示导入审计摘要。"""
    latest_import = operations_snapshot.get("latest_import_run") or {}
    quality_events = operations_snapshot.get("latest_import_quality_events", [])
    return build_page(
        "导入审计",
        [
            build_key_value_group(
                "最近导入",
                {
                    "import_run_id": latest_import.get("import_run_id"),
                    "source": latest_import.get("source"),
                    "status": latest_import.get("status"),
                    "started_at": latest_import.get("started_at"),
                    "finished_at": latest_import.get("finished_at"),
                    "securities_count": latest_import.get("securities_count"),
                    "calendar_count": latest_import.get("calendar_count"),
                    "bars_count": latest_import.get("bars_count"),
                    "degradation_flags": latest_import.get("degradation_flags"),
                    "warnings": latest_import.get("warnings"),
                },
            ),
            build_table_group(
                "质量事件",
                quality_events,
                [
                    ("事件", "event_type"),
                    ("级别", "level"),
                    ("交易日", "trade_date"),
                    ("标的", "ts_code"),
                    ("详情", "payload"),
                ],
            ),
        ],
    )
