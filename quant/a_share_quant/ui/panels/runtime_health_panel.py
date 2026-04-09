"""运行时健康面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.core.runtime_checks import summarize_runtime_results
from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group
from a_share_quant.services.ui_read_models import build_runtime_check_projection


def build_runtime_health_panel(runtime_results: list[dict[str, Any]]) -> object:
    """构建运行时摘要页。"""
    runtime_summary = summarize_runtime_results(runtime_results) if runtime_results else {}
    runtime_projection = build_runtime_check_projection(runtime_results)
    return build_page(
        "运行时健康",
        [
            build_key_value_group("健康摘要", runtime_summary),
            build_table_group(
                "检查结果",
                runtime_projection,
                [
                    ("检查项", "check"),
                    ("状态", "status"),
                    ("消息", "message"),
                    ("详情", "details"),
                ],
            ),
        ],
    )
