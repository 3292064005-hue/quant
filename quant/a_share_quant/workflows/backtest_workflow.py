"""回测工作流。"""
from __future__ import annotations

from a_share_quant.services.backtest_service import BacktestService


class BacktestWorkflow:
    """编排默认回测流程。"""

    def __init__(self, backtest_service: BacktestService, context, *, plugin_manager=None) -> None:
        self.backtest_service = backtest_service
        self.context = context
        self.plugin_manager = plugin_manager

    def bind_plugin_manager(self, plugin_manager) -> None:
        """在 bootstrap 后为 workflow 回填 plugin manager。"""
        self.plugin_manager = plugin_manager

    def run_default(self, strategy, *, entrypoint: str) -> object:
        """运行默认策略回测。

        Args:
            strategy: 已构建策略实例。
            entrypoint: 调用入口标识，用于审计和运行谱系。

        Returns:
            回测结果对象。
        """
        payload = {"strategy_id": getattr(strategy, "strategy_id", ""), "entrypoint": entrypoint}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.backtest", payload)
        result = None
        error = None
        try:
            result = self.backtest_service.run(strategy, entrypoint=entrypoint)
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(
                    self.context,
                    "workflow.backtest",
                    payload,
                    result=result,
                    error=error,
                )
