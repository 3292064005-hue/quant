"""回测服务。"""
from __future__ import annotations

from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.services.report_service import ReportService


class BacktestService:
    """编排完整回测流程。"""

    def __init__(self, config: AppConfig, engine: BacktestEngine, report_service: ReportService, run_repository: BacktestRunRepository) -> None:
        self.config = config
        self.engine = engine
        self.report_service = report_service
        self.run_repository = run_repository

    def run(self, strategy, bars_by_symbol, securities) -> BacktestResult:
        """执行完整回测并写出报告。

        Args:
            strategy: 已构建的策略对象。
            bars_by_symbol: 按证券分组的行情。
            securities: 证券主数据映射。

        Returns:
            `BacktestResult`。

        Raises:
            Exception: 回测或报告写出失败时向上抛出；失败状态会同步写入 `backtest_runs`。
        """
        result = self.engine.run(
            strategy,
            bars_by_symbol,
            securities,
            config_snapshot=self.config.model_dump(mode="json"),
            benchmark_symbol=self.config.backtest.benchmark_symbol,
        )
        try:
            report_path = self.report_service.write_backtest_report(result)
        except Exception as exc:
            self.run_repository.finish_run(result.run_id, BacktestRunStatus.FAILED, error_message=str(exc))
            raise
        result.report_path = str(report_path)
        self.run_repository.finish_run(result.run_id, BacktestRunStatus.COMPLETED, report_path=str(report_path))
        return result
