"""报告/回放面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group


def build_report_replay_panel(operations_snapshot: dict[str, Any]) -> object:
    """展示报告、回放、谱系与插件摘要。"""
    summary = operations_snapshot.get("latest_report_replay_summary") or {}
    research_rows = operations_snapshot.get("ui_recent_research_runs") or operations_snapshot.get("recent_research_run_summaries", [])
    provider_rows = operations_snapshot.get("ui_available_provider_details") or operations_snapshot.get("available_provider_details", [])
    related_research_rows = summary.get("related_research_run_summaries") or []
    return build_page(
        "报告、回放与谱系摘要",
        [
            build_key_value_group(
                "最近报告/回放",
                {
                    "run_id": summary.get("run_id"),
                    "report_path": summary.get("report_path"),
                    "artifact_status": summary.get("artifact_status"),
                    "artifact_errors": summary.get("artifact_errors"),
                    "signal_source_run_id": summary.get("signal_source_run_id"),
                    "signal_source_artifact_type": summary.get("signal_source_artifact_type"),
                    "promotion_package": summary.get("promotion_package"),
                    "run_event_summary": summary.get("run_event_summary"),
                },
            ),
            build_table_group(
                "最近研究运行",
                research_rows,
                [
                    ("Research Run", "research_run_id"),
                    ("产物", "artifact_type"),
                    ("数据版本", "dataset_version_id"),
                    ("数据摘要", "dataset_digest"),
                ],
            ),
            build_table_group(
                "同源研究参考",
                related_research_rows,
                [
                    ("Research Run", "research_run_id"),
                    ("产物", "artifact_type"),
                    ("数据版本", "dataset_version_id"),
                    ("信号快照", "signal_snapshot"),
                ],
            ),
            build_table_group(
                "插件生命周期事件",
                operations_snapshot.get("plugin_lifecycle_events", []),
                [
                    ("事件", "event"),
                    ("插件", "plugin_name"),
                    ("工作流", "workflow_name"),
                    ("时间", "created_at"),
                ],
            ),
            build_table_group(
                "可用 Provider",
                provider_rows,
                [
                    ("名称", "name"),
                    ("类型", "component_type"),
                    ("合同形态", "contract_kind"),
                    ("输入合同", "input_contract"),
                    ("输出合同", "output_contract"),
                ],
            ),
        ],
    )
