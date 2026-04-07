"""回测服务。"""
from __future__ import annotations

import logging

from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage, RunArtifacts, TradingCalendarEntry
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.services.data_service import DataService
from a_share_quant.services.report_service import ReportService

logger = logging.getLogger(__name__)


class BacktestService:
    """编排完整回测流程。"""

    def __init__(
        self,
        config: AppConfig,
        engine: BacktestEngine,
        report_service: ReportService,
        run_repository: BacktestRunRepository,
        data_service: DataService | None = None,
    ) -> None:
        self.config = config
        self.engine = engine
        self.report_service = report_service
        self.run_repository = run_repository
        self.data_service = data_service

    def run(
        self,
        strategy,
        bars_by_symbol=None,
        securities=None,
        trade_calendar: list[TradingCalendarEntry] | None = None,
        *,
        entrypoint: str = "backtest_service.run",
    ) -> BacktestResult:
        """执行完整回测并写出报告。

        Args:
            strategy: 已实例化策略。
            bars_by_symbol: 可选预加载行情。
            securities: 可选证券池。
            trade_calendar: 可选交易日历。
            entrypoint: 触发本次运行的入口标识，写入 run manifest。

        Raises:
            ValueError: 当前运行模式不是 ``research_backtest``，或缺少数据服务/行情输入。
            Exception: 回测执行或报表写出失败时向上抛出。
        """
        if self.config.app.runtime_mode != "research_backtest":
            raise ValueError(
                f"当前版本的 BacktestService 仅支持 research_backtest；收到 app.runtime_mode={self.config.app.runtime_mode}"
            )
        artifacts = RunArtifacts(
            entrypoint=entrypoint,
            strategy_version=self.config.strategy.version,
            runtime_mode=self.config.app.runtime_mode,
        )
        data_lineage = DataLineage()
        if bars_by_symbol is None or securities is None:
            if self.data_service is None:
                raise ValueError("BacktestService.run 未提供行情入参，且未注入 DataService")
            if self.config.backtest.data_access_mode == "stream":
                day_batches, loaded_securities, trade_dates = self.data_service.stream_market_data()
                lineage_bundle = self.data_service.load_market_data_bundle(
                    start_date=trade_dates[0] if trade_dates else None,
                    end_date=trade_dates[-1] if trade_dates else None,
                )
                data_lineage = lineage_bundle.data_lineage
                result = self.engine.run_streaming(
                    strategy,
                    day_batches=day_batches,
                    trade_dates=trade_dates,
                    securities=loaded_securities,
                    config_snapshot=self.config.model_dump(mode="json"),
                    benchmark_symbol=self.config.backtest.benchmark_symbol,
                    data_lineage=data_lineage,
                    artifacts=artifacts,
                )
            else:
                loaded_bundle = self.data_service.load_market_data_bundle()
                data_lineage = loaded_bundle.data_lineage
                result = self.engine.run(
                    strategy,
                    loaded_bundle.bars_by_symbol,
                    loaded_bundle.securities,
                    config_snapshot=self.config.model_dump(mode="json"),
                    benchmark_symbol=self.config.backtest.benchmark_symbol,
                    trade_calendar=loaded_bundle.trade_calendar,
                    data_lineage=data_lineage,
                    artifacts=artifacts,
                )
        else:
            data_lineage = DataLineage(
                data_source="inline_bars",
                data_start_date=trade_calendar[0].cal_date.isoformat() if trade_calendar else None,
                data_end_date=trade_calendar[-1].cal_date.isoformat() if trade_calendar else None,
                dataset_digest=None,
            )
            result = self.engine.run(
                strategy,
                bars_by_symbol,
                securities,
                config_snapshot=self.config.model_dump(mode="json"),
                benchmark_symbol=self.config.backtest.benchmark_symbol,
                trade_calendar=trade_calendar,
                data_lineage=data_lineage,
                artifacts=artifacts,
            )
        try:
            report_paths = self.report_service.write_backtest_report(result)
        except Exception as exc:
            logger.exception("回测报告写出失败 run_id=%s error=%s", result.run_id, exc)
            self.run_repository.finish_run(result.run_id, BacktestRunStatus.FAILED, error_message=str(exc))
            raise
        result.artifacts.report_paths = [str(path) for path in report_paths]
        result.report_path = str(report_paths[0])
        self.run_repository.finish_run(
            result.run_id,
            BacktestRunStatus.COMPLETED,
            report_path=str(report_paths[0]),
            report_artifacts=result.artifacts.report_paths,
        )
        logger.info("回测服务完成 run_id=%s report=%s", result.run_id, report_paths[0])
        return result
