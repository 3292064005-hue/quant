"""回放/重放工作流。"""
from __future__ import annotations

from typing import Any

from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.services.report_service import ReportService


class ReplayWorkflow:
    """读取最近一次运行并在需要时重建报告。"""

    def __init__(self, run_repository: BacktestRunRepository, report_service: ReportService, context, *, plugin_manager=None) -> None:
        self.run_repository = run_repository
        self.report_service = report_service
        self.context = context
        self.plugin_manager = plugin_manager

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    def summarize_latest(self) -> dict[str, Any] | None:
        """返回最近一次回测运行摘要。"""
        payload = {"mode": "latest_summary"}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.replay", payload)
        result = None
        error = None
        try:
            latest = self.run_repository.get_latest_run()
            if latest is None:
                return None
            result = {
                "run_id": latest.run_id,
                "strategy_id": latest.strategy_id,
                "status": latest.status.value,
                "started_at": latest.started_at,
                "finished_at": latest.finished_at,
                "report_path": latest.report_path,
                "dataset_version_id": latest.dataset_version_id,
                "dataset_digest": latest.dataset_digest,
            }
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(
                    self.context,
                    "workflow.replay",
                    payload,
                    result=result,
                    error=error,
                )

    def rebuild_latest_report(self) -> str:
        """重建最近一次可重建运行的报告。"""
        payload = {"mode": "rebuild_latest"}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.replay", payload)
        result = None
        error = None
        try:
            result = str(self.report_service.rebuild_backtest_report(run_id=None))
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(
                    self.context,
                    "workflow.replay",
                    payload,
                    result=result,
                    error=error,
                )
