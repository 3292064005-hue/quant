from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.report_service import ReportService


class _EngineStub:
    def run(self, strategy, bars_by_symbol, securities, config_snapshot, benchmark_symbol, trade_calendar=None, **kwargs):
        return BacktestResult(strategy_id="demo_strategy", run_id="run_demo")


class _ReportServiceStub:
    def __init__(self, report_path: Path) -> None:
        self.report_path = report_path

    def write_backtest_report(self, result: BacktestResult):
        payload = {
            "artifacts": {
                "artifact_status": result.artifacts.artifact_status,
                "artifact_completed_at": result.artifacts.artifact_completed_at,
                "signal_source_run_id": result.artifacts.signal_source_run_id,
            }
        }
        self.report_path.write_text(json.dumps(payload), encoding="utf-8")
        latest = self.report_path.parent / "latest.json"
        latest.write_text(json.dumps(payload), encoding="utf-8")
        return [self.report_path, latest]


class _FailingReportServiceStub:
    def write_backtest_report(self, result: BacktestResult):
        raise RuntimeError("report export failed")


class _RunRepositoryStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, BacktestRunStatus, str | None, str | None, list[str] | None, object | None]] = []
        self.lineage_updates: list[tuple[str, DataLineage]] = []

    def update_lineage(self, run_id: str, lineage: DataLineage) -> None:
        self.lineage_updates.append((run_id, lineage))

    def finish_run(
        self,
        run_id: str,
        status: BacktestRunStatus,
        error_message: str | None = None,
        report_path: str | None = None,
        report_artifacts: list[str] | None = None,
        run_manifest=None,
        run_events=None,
        *,
        set_finished_at: bool = True,
        overwrite_error_message: bool = False,
    ) -> None:
        self.calls.append((run_id, status, error_message, report_path, report_artifacts, run_manifest))


class _StrategyStub:
    strategy_id = "demo_strategy"
    _component_manifest = {"signal_component": "builtin.direct_targets"}
    _bound_research_signal_run_id = "research_demo"


def test_backtest_service_marks_success_with_intermediate_engine_completion(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    run_repository = _RunRepositoryStub()
    service = BacktestService(
        AppConfig(),
        cast(BacktestEngine, _EngineStub()),
        cast(ReportService, _ReportServiceStub(report_path)),
        cast(BacktestRunRepository, run_repository),
    )
    result = service.run(_StrategyStub(), bars_by_symbol={}, securities={})
    assert result.report_path == str(report_path)
    assert [(item[0], item[1], item[2], item[3], item[4]) for item in run_repository.calls] == [
        ("run_demo", BacktestRunStatus.ENGINE_COMPLETED, None, None, None),
        ("run_demo", BacktestRunStatus.COMPLETED, None, str(report_path), [str(report_path), str(report_path.parent / "latest.json")]),
    ]
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_payload["artifacts"]["artifact_status"] == "GENERATED"
    assert report_payload["artifacts"]["signal_source_run_id"] == "research_demo"
    assert result.artifacts.artifact_status == "GENERATED"
    assert result.artifacts.engine_completed_at is not None
    assert result.artifacts.artifact_completed_at is not None


class _EngineStreamingStub(_EngineStub):
    def run_streaming(self, strategy, day_batches, trade_dates, securities, config_snapshot, benchmark_symbol, **kwargs):
        return BacktestResult(strategy_id="demo_strategy", run_id="run_stream")


class _StreamingDataServiceStub:
    class _Tracker:
        def finalize(self) -> DataLineage:
            return DataLineage(dataset_version_id="dataset_demo", import_run_ids=["import_demo"])

    class _Bundle:
        def __init__(self) -> None:
            self.day_batches: Any = iter([])
            self.securities: dict[str, Any] = {}
            self.trade_dates: list[Any] = []
            self.data_lineage = DataLineage()
            self.lineage_tracker = _StreamingDataServiceStub._Tracker()

    def prepare_stream_market_data(self):
        return self._Bundle()

    def load_market_data_bundle(self):  # pragma: no cover - 一旦被调用说明 stream 仍回退全量加载
        raise AssertionError("stream 模式不应回退到 load_market_data_bundle")


def test_backtest_service_stream_mode_does_not_fallback_to_preload(tmp_path: Path) -> None:
    report_path = tmp_path / "stream_report.json"
    run_repository = _RunRepositoryStub()
    config = AppConfig()
    config.backtest.data_access_mode = "stream"
    service = BacktestService(
        config,
        cast(BacktestEngine, _EngineStreamingStub()),
        cast(ReportService, _ReportServiceStub(report_path)),
        cast(BacktestRunRepository, run_repository),
        data_service=cast(DataService, _StreamingDataServiceStub()),
    )
    result = service.run(_StrategyStub())
    assert result.report_path == str(report_path)
    assert run_repository.calls[-1][0] == "run_stream"
    assert run_repository.calls[0][1] == BacktestRunStatus.ENGINE_COMPLETED
    assert run_repository.calls[-1][1] == BacktestRunStatus.COMPLETED
    assert run_repository.lineage_updates[-1][0] == "run_stream"
    assert run_repository.lineage_updates[-1][1].dataset_version_id == "dataset_demo"


def test_backtest_service_marks_artifact_failure_without_downgrading_business_stage() -> None:
    run_repository = _RunRepositoryStub()
    service = BacktestService(
        AppConfig(),
        cast(BacktestEngine, _EngineStub()),
        cast(ReportService, _FailingReportServiceStub()),
        cast(BacktestRunRepository, run_repository),
    )
    with pytest.raises(RuntimeError, match="report export failed"):
        service.run(_StrategyStub(), bars_by_symbol={}, securities={})
    assert [(item[0], item[1], item[2], item[3], item[4]) for item in run_repository.calls] == [
        ("run_demo", BacktestRunStatus.ENGINE_COMPLETED, None, None, None),
        ("run_demo", BacktestRunStatus.ARTIFACT_EXPORT_FAILED, "report export failed", None, None),
    ]
