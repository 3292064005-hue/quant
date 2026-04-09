"""报告工作流。"""
from __future__ import annotations

from a_share_quant.services.report_service import ReportService


class ReportWorkflow:
    """负责报告重建。"""

    def __init__(self, report_service: ReportService, context, *, plugin_manager=None) -> None:
        self.report_service = report_service
        self.context = context
        self.plugin_manager = plugin_manager

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    def rebuild(self, *, run_id: str | None = None) -> str:
        """重建指定运行报告。

        Args:
            run_id: 可选回测运行标识；为空时重建最近一次可重建运行。

        Returns:
            生成的报告路径。
        """
        payload = {"run_id": run_id}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.report", payload)
        result = None
        error = None
        try:
            result = str(self.report_service.rebuild_backtest_report(run_id=run_id))
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(
                    self.context,
                    "workflow.report",
                    payload,
                    result=result,
                    error=error,
                )
