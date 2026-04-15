"""策略/工作流面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group


def build_strategy_lifecycle_panel(operations_snapshot: dict[str, Any]) -> object:
    """展示工作流、策略与最近运行状态。"""
    latest_run = operations_snapshot.get("ui_latest_backtest_run") or {}
    workflow_rows = operations_snapshot.get("ui_available_workflow_details") or []
    recent_research_rows = operations_snapshot.get("ui_recent_research_runs") or []
    return build_page(
        "策略生命周期",
        [
            build_key_value_group(
                "最近回测运行",
                {
                    "run_id": latest_run.get("run_id"),
                    "strategy_id": latest_run.get("strategy_id"),
                    "status": latest_run.get("status"),
                    "status_breakdown": latest_run.get("status_breakdown"),
                    "runtime_mode": latest_run.get("runtime_mode"),
                    "dataset_version_id": latest_run.get("dataset_version_id"),
                    "import_run_id": latest_run.get("import_run_id"),
                    "dataset_digest": latest_run.get("dataset_digest"),
                },
            ),
            build_table_group(
                "可用工作流",
                workflow_rows,
                [
                    ("名称", "name"),
                    ("类型", "component_type"),
                    ("合同形态", "contract_kind"),
                    ("输入合同", "input_contract"),
                    ("输出合同", "output_contract"),
                ],
            ),
            build_table_group(
                "最近研究运行",
                recent_research_rows,
                [
                    ("Research Run", "research_run_id"),
                    ("产物", "artifact_type"),
                    ("特征", "feature_name"),
                    ("数据版本", "dataset_version_id"),
                    ("信号快照", "signal_snapshot"),
                ],
            ),
        ],
    )
