"""report/replay 命令域 CLI。"""
from __future__ import annotations

import argparse
from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap_report_context


def main_generate_report(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="基于数据库中的回测结果重建报告")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--run-id", default=None, help="指定要重建报告的回测 run_id；缺省时使用最近一次可重建运行（COMPLETED / ENGINE_COMPLETED / ARTIFACT_EXPORT_FAILED）")
    args = parser.parse_args(argv)
    with bootstrap_report_context(args.config) as context:
        workflow = context.require_workflow_registry().get("workflow.report")
        path = workflow.rebuild(run_id=args.run_id)
        print({"report": str(Path(path))})
        return 0
