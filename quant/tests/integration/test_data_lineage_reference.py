from __future__ import annotations

from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap


def test_backtest_lineage_ignores_latest_failed_import_run(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"

    with bootstrap(str(app_path)) as context:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        completed_import_run_id = context.data_service.last_import_run_id
        failed_import_run_id = context.data_import_repository.create_run(
            source="csv",
            request_context={"csv_path": "non-existent.csv"},
        )
        context.data_import_repository.finish_run(
            failed_import_run_id,
            status="FAILED",
            error_message="synthetic failure",
        )
        context.store.execute(
            "UPDATE data_import_runs SET started_at = '2000-01-01T00:00:00+00:00' WHERE import_run_id = ?",
            (completed_import_run_id,),
        )
        context.store.execute(
            "UPDATE data_import_runs SET started_at = '2099-01-01T00:00:00+00:00' WHERE import_run_id = ?",
            (failed_import_run_id,),
        )

    with bootstrap(str(app_path)) as context:
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, entrypoint="tests.integration.failed_import_lineage")
        assert result.data_lineage.import_run_id == completed_import_run_id
        assert result.data_lineage.import_run_id != failed_import_run_id
