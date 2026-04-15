"""report write use-case。"""
from __future__ import annotations

from pathlib import Path

from a_share_quant.domain.models import BacktestResult
from a_share_quant.execution.order_lifecycle_service import OrderLifecycleEventService


class ReportWriteService:
    def __init__(self, owner) -> None:
        self.owner = owner
        self.lifecycle_service = OrderLifecycleEventService()

    def write_backtest_report(self, result: BacktestResult) -> list[Path]:
        if not result.strategy_id or not result.run_id:
            raise ValueError("write_backtest_report 需要有效的 strategy_id 与 run_id")
        report_name = self.owner.report_name_template.format(strategy_id=result.strategy_id, run_id=result.run_id)
        primary_path = self.owner.reports_dir / report_name
        latest_path = self.owner.reports_dir / f"{result.strategy_id}_backtest.json"
        resolved_report_paths = [primary_path, latest_path]
        result.artifacts.report_paths = [self.owner._to_manifest_path(path) for path in resolved_report_paths]
        result.artifacts.report_artifacts = [
            {"role": "primary", "path": result.artifacts.report_paths[0], "kind": "report", "format": "json", "primary": True},
            {"role": "latest", "path": result.artifacts.report_paths[1], "kind": "report", "format": "json", "primary": False},
        ]
        lifecycle_summary = self.lifecycle_service.summarize_lifecycle_events(result.run_events, runtime_lane="research_backtest")
        run_event_summary = self.owner._resolve_run_event_summary(result.run_events, result.artifacts)
        if lifecycle_summary.get("event_count", 0) > 0:
            run_event_summary = {**run_event_summary, "lifecycle_summary": lifecycle_summary}
        result.artifacts.run_event_summary = run_event_summary
        if result.run_events:
            event_log_path = self.owner.reports_dir / f"{result.strategy_id}_{result.run_id}_events.json"
            self.owner.writer.write_json(
                event_log_path,
                {
                    "run_id": result.run_id,
                    "strategy_id": result.strategy_id,
                    "event_count": len(result.run_events),
                    "lifecycle_summary": lifecycle_summary,
                    "events": result.run_events,
                },
            )
            result.artifacts.event_log_path = self.owner._to_manifest_path(event_log_path)
            result.artifacts.report_artifacts.append({"role": "event_log", "path": result.artifacts.event_log_path, "kind": "event_log", "format": "json", "primary": False})
        payload = {
            "strategy_id": result.strategy_id,
            "run_id": result.run_id,
            "benchmark_symbol": result.benchmark_symbol,
            "trade_dates": [item.isoformat() for item in result.trade_dates],
            "equity_curve": result.equity_curve,
            "benchmark_curve": result.benchmark_curve,
            "order_count": result.order_count,
            "fill_count": result.fill_count,
            "metrics": result.metrics,
            "data_lineage": {
                "dataset_version_id": result.data_lineage.dataset_version_id,
                "import_run_id": result.data_lineage.import_run_id,
                "import_run_ids": result.data_lineage.import_run_ids,
                "data_source": result.data_lineage.data_source,
                "data_start_date": result.data_lineage.data_start_date,
                "data_end_date": result.data_lineage.data_end_date,
                "dataset_digest": result.data_lineage.dataset_digest,
                "degradation_flags": result.data_lineage.degradation_flags,
                "warnings": result.data_lineage.warnings,
            },
            "data_quality_summary": self.owner._build_data_quality_summary(result.data_quality_events),
            "run_event_summary": run_event_summary,
            "lifecycle_summary": lifecycle_summary,
            "artifacts": self.owner._serialize_artifacts(result.artifacts),
        }
        self.owner.writer.write_json(primary_path, payload)
        self.owner.writer.write_json(latest_path, payload)
        return resolved_report_paths
