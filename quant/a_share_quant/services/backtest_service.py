"""回测服务。"""
from __future__ import annotations

import logging

from a_share_quant.config.models import AppConfig
from a_share_quant.core.utils import now_iso
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

        Boundary Behavior:
            - 引擎业务完成与报告产物完成分为两个阶段持久化；
            - 报告写出失败不会再伪装成“引擎业务失败”，而会落为 ``ARTIFACT_EXPORT_FAILED``；
            - 若策略实例携带 ``_component_manifest``，会写入 run manifest 用于后续扩展与重建审计。
        """
        if self.config.app.runtime_mode != "research_backtest":
            raise ValueError(
                f"当前版本的 BacktestService 仅支持 research_backtest；收到 app.runtime_mode={self.config.app.runtime_mode}"
            )
        bound_signal_source_run_id = (getattr(strategy, "_bound_research_signal_run_id", None) or "").strip() or None
        artifacts = RunArtifacts(
            schema_version=5,
            entrypoint=entrypoint,
            strategy_version=self.config.strategy.version,
            runtime_mode=self.config.app.runtime_mode,
            benchmark_initial_value=self.config.backtest.initial_cash,
            artifact_status="PENDING",
            component_manifest=dict(getattr(strategy, "_component_manifest", {}) or {}),
            promotion_package=dict(getattr(strategy, "_promotion_package", {}) or {}),
            signal_source_run_id=bound_signal_source_run_id,
            signal_source_artifact_type="signal_snapshot" if bound_signal_source_run_id else None,
        )
        data_lineage = DataLineage()
        if bars_by_symbol is None or securities is None:
            if self.data_service is None:
                raise ValueError("BacktestService.run 未提供行情入参，且未注入 DataService")
            if self.config.backtest.data_access_mode == "stream":
                streaming_bundle = self.data_service.prepare_stream_market_data()
                data_lineage = streaming_bundle.data_lineage
                result = self.engine.run_streaming(
                    strategy,
                    day_batches=streaming_bundle.day_batches,
                    trade_dates=streaming_bundle.trade_dates,
                    securities=streaming_bundle.securities,
                    config_snapshot=self.config.model_dump(mode="json"),
                    benchmark_symbol=self.config.backtest.benchmark_symbol,
                    data_lineage=data_lineage,
                    artifacts=artifacts,
                )
                lineage_tracker = getattr(streaming_bundle, "lineage_tracker", None)
                if lineage_tracker is not None:
                    data_lineage = lineage_tracker.finalize()
                    data_lineage.degradation_flags = sorted(
                        set(data_lineage.degradation_flags) | set(streaming_bundle.data_lineage.degradation_flags)
                    )
                    data_lineage.warnings = [*data_lineage.warnings, *streaming_bundle.data_lineage.warnings]
                    result.data_lineage = data_lineage
                    self.run_repository.update_lineage(result.run_id, data_lineage)
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
        if self.data_service is not None and result.data_lineage.import_run_id and self.data_service.data_import_repository is not None:
            result.data_quality_events = self.data_service.data_import_repository.list_quality_events(result.data_lineage.import_run_id)

        result.artifacts.component_manifest = dict(getattr(strategy, "_component_manifest", {}) or result.artifacts.component_manifest)
        result.artifacts.promotion_package = dict(getattr(strategy, "_promotion_package", {}) or result.artifacts.promotion_package)
        result.artifacts.signal_source_run_id = bound_signal_source_run_id or result.artifacts.signal_source_run_id
        result.artifacts.signal_source_artifact_type = (
            "signal_snapshot" if result.artifacts.signal_source_run_id else result.artifacts.signal_source_artifact_type
        )
        result.artifacts.engine_completed_at = now_iso()
        self._persist_intermediate_engine_completion(result)

        result.artifacts.artifact_status = "GENERATED"
        result.artifacts.artifact_errors = []
        result.artifacts.artifact_completed_at = now_iso()
        try:
            report_paths = self.report_service.write_backtest_report(result)
        except Exception as exc:
            logger.exception("回测报告写出失败 run_id=%s error=%s", result.run_id, exc)
            result.artifacts.artifact_status = "FAILED"
            result.artifacts.artifact_errors = [str(exc)]
            result.artifacts.artifact_completed_at = now_iso()
            self._persist_terminal_status(
                result,
                status=BacktestRunStatus.ARTIFACT_EXPORT_FAILED,
                error_message=str(exc),
                overwrite_error_message=True,
            )
            raise

        if not result.artifacts.report_paths:
            result.artifacts.report_paths = [str(path) for path in report_paths]
        result.report_path = str(report_paths[0])
        self._persist_terminal_status(
            result,
            status=BacktestRunStatus.COMPLETED,
            report_path=str(report_paths[0]),
            report_artifacts=result.artifacts.report_paths,
            overwrite_error_message=True,
        )
        logger.info("回测服务完成 run_id=%s report=%s", result.run_id, report_paths[0])
        return result

    def _persist_intermediate_engine_completion(self, result: BacktestResult) -> None:
        """持久化引擎业务完成态。"""
        self.run_repository.finish_run(
            result.run_id,
            BacktestRunStatus.ENGINE_COMPLETED,
            run_manifest=result.artifacts,
            run_events=result.run_events,
            set_finished_at=False,
        )

    def _persist_terminal_status(
        self,
        result: BacktestResult,
        *,
        status: BacktestRunStatus,
        error_message: str | None = None,
        report_path: str | None = None,
        report_artifacts: list[str] | None = None,
        overwrite_error_message: bool = False,
    ) -> None:
        """持久化终态及产物信息。"""
        self.run_repository.finish_run(
            result.run_id,
            status,
            error_message=error_message,
            report_path=report_path,
            report_artifacts=report_artifacts,
            run_manifest=result.artifacts,
            run_events=result.run_events,
            overwrite_error_message=overwrite_error_message,
        )
