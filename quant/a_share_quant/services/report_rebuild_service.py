"""report rebuild use-case。"""
from __future__ import annotations

import json

from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage


class ReportRebuildService:
    def __init__(self, owner) -> None:
        self.owner = owner

    def rebuild_backtest_report(self, run_id: str | None = None):
        if self.owner.run_repository is None or self.owner.account_repository is None or self.owner.order_repository is None:
            raise RuntimeError("ReportService 未注入重建报表所需的 repository")
        rebuildable_statuses = [
            BacktestRunStatus.COMPLETED,
            BacktestRunStatus.ENGINE_COMPLETED,
            BacktestRunStatus.ARTIFACT_EXPORT_FAILED,
        ]
        run = (
            self.owner.run_repository.get_run(run_id)
            if run_id is not None
            else self.owner.run_repository.get_latest_run_by_statuses(rebuildable_statuses)
        )
        if run is None:
            if run_id is None:
                raise ValueError("数据库中不存在可重建的回测运行")
            raise ValueError(f"找不到指定 run_id 的回测运行: {run_id}")
        if not run.status.rebuildable:
            raise ValueError(f"run_id={run.run_id} 当前状态={run.status.value}，不可重建报告")
        trade_dates, equity_curve = self.owner.account_repository.load_equity_curve(run.run_id)
        config_snapshot = json.loads(run.config_snapshot_json)
        benchmark_symbol = config_snapshot.get("backtest", {}).get("benchmark_symbol")
        manifest = self.owner._load_run_manifest(run)
        benchmark_initial_value = manifest.benchmark_initial_value
        if benchmark_initial_value is None:
            benchmark_initial_value = self.owner._coerce_float(config_snapshot.get("backtest", {}).get("initial_cash"))
        benchmark_curve = self.owner._rebuild_benchmark_curve(trade_dates, benchmark_symbol, benchmark_initial_value)
        metrics_payload = self.owner._build_metrics_payload(equity_curve, benchmark_curve)
        quality_events = self.owner._load_quality_events(run.import_run_id)
        run_events = self.owner._load_run_events(run, manifest)
        result = BacktestResult(
            strategy_id=run.strategy_id,
            run_id=run.run_id,
            benchmark_symbol=benchmark_symbol,
            trade_dates=trade_dates,
            equity_curve=equity_curve,
            benchmark_curve=benchmark_curve,
            order_count=self.owner.order_repository.count_orders(run.run_id),
            fill_count=self.owner.order_repository.count_fills(run.run_id),
            metrics=metrics_payload,
            data_lineage=DataLineage(
                dataset_version_id=run.dataset_version_id,
                import_run_id=run.import_run_id,
                import_run_ids=json.loads(run.import_run_ids_json or "[]"),
                data_source=run.data_source or "database_snapshot",
                data_start_date=run.data_start_date,
                data_end_date=run.data_end_date,
                dataset_digest=run.dataset_digest,
                degradation_flags=json.loads(run.degradation_flags_json or "[]"),
                warnings=json.loads(run.warnings_json or "[]"),
            ),
            artifacts=manifest,
            run_events=run_events,
            data_quality_events=quality_events,
        )
        result.artifacts.artifact_status = "GENERATED"
        result.artifacts.artifact_errors = []
        result.artifacts.artifact_completed_at = now_iso()
        try:
            report_paths = self.owner.write_service.write_backtest_report(result)
        except Exception as exc:
            result.artifacts.artifact_status = "FAILED"
            result.artifacts.artifact_errors = [str(exc)]
            result.artifacts.artifact_completed_at = now_iso()
            self.owner.run_repository.finish_run(
                run.run_id,
                BacktestRunStatus.ARTIFACT_EXPORT_FAILED,
                error_message=str(exc),
                report_path=None,
                report_artifacts=result.artifacts.report_paths,
                run_manifest=result.artifacts,
                run_events=result.run_events,
                overwrite_error_message=True,
            )
            raise
        result.report_path = str(report_paths[0])
        self.owner.run_repository.finish_run(
            run.run_id,
            BacktestRunStatus.COMPLETED,
            error_message=None,
            report_path=str(report_paths[0]),
            report_artifacts=result.artifacts.report_paths,
            run_manifest=result.artifacts,
            run_events=result.run_events,
            overwrite_error_message=True,
        )
        return report_paths[0]
