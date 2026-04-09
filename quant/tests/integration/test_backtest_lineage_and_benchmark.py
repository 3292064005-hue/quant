from __future__ import annotations

import json
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap


def test_backtest_persists_data_lineage_and_benchmark_curve(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("backtest", {})["benchmark_symbol"] = "600000.SH"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.lineage")

        assert result.data_lineage.dataset_version_id is not None
        assert result.data_lineage.import_run_id is not None
        assert result.data_lineage.import_run_ids == [result.data_lineage.import_run_id]
        assert result.data_lineage.data_source == "csv"
        assert result.data_lineage.dataset_digest
        assert len(result.benchmark_curve) == len(result.trade_dates)
        assert "benchmark_total_return" in result.metrics
        assert "information_ratio" in result.metrics

        run = context.backtest_run_repository.get_run(result.run_id)
        assert run is not None
        assert run.dataset_version_id == result.data_lineage.dataset_version_id
        assert run.import_run_id == result.data_lineage.import_run_id
        assert run.dataset_digest == result.data_lineage.dataset_digest
        assert run.entrypoint == "tests.integration.lineage"
        assert run.runtime_mode == "research_backtest"
        artifacts = json.loads(run.report_artifacts_json)
        assert len(artifacts) == 2
        run_manifest = json.loads(run.run_manifest_json)
        assert run_manifest["schema_version"] >= 3
        assert run_manifest["benchmark_initial_value"] == payload["backtest"].get("initial_cash", 1000000.0)
        assert json.loads(run.run_events_json) == result.run_events

        assert result.report_path is not None
        report_payload = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
        assert report_payload["data_lineage"]["dataset_version_id"] == result.data_lineage.dataset_version_id
        assert report_payload["data_lineage"]["dataset_digest"] == result.data_lineage.dataset_digest
        assert len(report_payload["benchmark_curve"]) == len(result.trade_dates)
        assert report_payload["artifacts"]["entrypoint"] == "tests.integration.lineage"
        assert report_payload["artifacts"]["benchmark_initial_value"] == payload["backtest"].get("initial_cash", 1000000.0)
        assert len(report_payload["artifacts"]["report_paths"]) == 2
        assert sorted(report_payload["artifacts"]["report_paths"]) == sorted(result.artifacts.report_paths)
        assert all(not Path(item).is_absolute() for item in report_payload["artifacts"]["report_paths"])
