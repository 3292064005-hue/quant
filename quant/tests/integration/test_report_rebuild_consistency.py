from __future__ import annotations

import json
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap, bootstrap_report_context
from a_share_quant.domain.models import BacktestRunStatus


def test_report_rebuild_preserves_benchmark_curve_when_initial_cash_changes(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("backtest", {})["initial_cash"] = 2_000_000.0
    payload.setdefault("backtest", {})["benchmark_symbol"] = "600000.SH"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.rebuild_consistency")
        assert result.report_path is not None
        original_report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
        assert original_report["artifacts"]["artifact_status"] == "GENERATED"

    with bootstrap_report_context(str(app_path)) as report_context:
        rebuilt_path = report_context.require_report_service().rebuild_backtest_report(run_id=result.run_id)
        rebuilt_report = json.loads(Path(rebuilt_path).read_text(encoding="utf-8"))

    assert rebuilt_report["artifacts"]["artifact_status"] == "GENERATED"
    assert rebuilt_report["benchmark_curve"] == original_report["benchmark_curve"]
    assert rebuilt_report["metrics"] == original_report["metrics"]
    assert rebuilt_report["artifacts"]["benchmark_initial_value"] == 2_000_000.0


def test_stream_mode_rebuild_uses_same_manifest_contract(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("backtest", {})["data_access_mode"] = "stream"
    payload.setdefault("backtest", {})["initial_cash"] = 1_500_000.0
    payload.setdefault("backtest", {})["benchmark_symbol"] = "600000.SH"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.stream_manifest")
        assert result.report_path is not None
        original_report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
        assert result.artifacts.schema_version >= 3
        assert result.artifacts.benchmark_initial_value == 1_500_000.0

    with bootstrap_report_context(str(app_path)) as report_context:
        rebuilt_path = report_context.require_report_service().rebuild_backtest_report(run_id=result.run_id)
        rebuilt_report = json.loads(Path(rebuilt_path).read_text(encoding="utf-8"))

    assert rebuilt_report["artifacts"]["artifact_status"] == "GENERATED"
    assert rebuilt_report["benchmark_curve"] == original_report["benchmark_curve"]
    assert rebuilt_report["artifacts"]["schema_version"] >= 3
    assert rebuilt_report["artifacts"]["benchmark_initial_value"] == 1_500_000.0


def test_rebuild_report_falls_back_to_manifest_event_summary_when_event_log_missing(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        report_service = context.require_report_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.event_summary_fallback")
        assert result.artifacts.event_log_path is not None
        event_log_path = Path(context.config.data.reports_dir) / result.artifacts.event_log_path
        assert event_log_path.exists()
        original_summary = dict(result.artifacts.run_event_summary)
        original_events = list(result.run_events)
        event_log_path.unlink()
        rebuilt_path = report_service.rebuild_backtest_report(result.run_id)
        rebuilt_report = json.loads(rebuilt_path.read_text(encoding="utf-8"))
        assert rebuilt_report["run_event_summary"] == original_summary
        assert rebuilt_report["run_event_summary"]["event_count"] > 0
        rebuilt_run = context.backtest_run_repository.get_run(result.run_id)
        assert rebuilt_run is not None
        assert json.loads(rebuilt_run.run_events_json) == original_events


def test_report_artifact_paths_are_portable_relative_paths(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.relative_artifacts")
        assert all(not Path(item).is_absolute() for item in result.artifacts.report_paths)
        assert result.artifacts.event_log_path is not None
        assert not Path(result.artifacts.event_log_path).is_absolute()
        for relpath in result.artifacts.report_paths:
            assert (Path(context.config.data.reports_dir) / relpath).exists()
        assert (Path(context.config.data.reports_dir) / result.artifacts.event_log_path).exists()


def test_report_rebuild_recovers_artifact_export_failed_run(temp_config_dir: Path, monkeypatch) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        report_service = context.require_report_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        original_writer = report_service.write_backtest_report

        def _fail_once(result):
            monkeypatch.setattr(report_service, "write_backtest_report", original_writer)
            raise RuntimeError("report export failed in integration")

        monkeypatch.setattr(report_service, "write_backtest_report", _fail_once)
        try:
            backtest_service.run(strategy, entrypoint="tests.integration.artifact_recovery")
        except RuntimeError as exc:
            assert "report export failed in integration" in str(exc)
        else:  # pragma: no cover - 保护分支，说明失败注入未生效
            raise AssertionError("expected injected report failure")

        failed_run = context.backtest_run_repository.get_latest_run_by_statuses([BacktestRunStatus.ARTIFACT_EXPORT_FAILED])
        assert failed_run is not None
        assert failed_run.error_message == "report export failed in integration"

        rebuilt_path = report_service.rebuild_backtest_report(failed_run.run_id)
        rebuilt_run = context.backtest_run_repository.get_run(failed_run.run_id)
        assert rebuilt_run is not None
        assert rebuilt_run.status == BacktestRunStatus.COMPLETED
        assert rebuilt_run.error_message is None
        assert Path(rebuilt_path).exists()
