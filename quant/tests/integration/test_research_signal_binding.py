from __future__ import annotations

import json
from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.cli import _load_ui_operations_snapshot, main_research
from a_share_quant.services.run_query_service import RunQueryService


def test_backtest_can_consume_persisted_research_signal_snapshot(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"

    exit_code = main_research(
        [
            "--config",
            str(app_path),
            "--artifact",
            "signal",
            "--csv",
            str(project_root / "sample_data" / "daily_bars.csv"),
            "--lookback",
            "3",
            "--top-n",
            "2",
        ]
    )
    assert exit_code == 0

    with bootstrap(str(app_path)) as context:
        latest_signal_run = context.research_run_repository.get_latest(artifact_type="signal_snapshot")
        assert latest_signal_run is not None
        bound_research_run_id = latest_signal_run["research_run_id"]
        strategy = context.require_strategy_service().build_default(research_signal_run_id=bound_research_run_id)
        assert strategy._component_manifest["signal_component"] == "research.signal_snapshot"
        result = context.require_backtest_service().run(strategy, entrypoint="tests.integration.research_signal_binding")
        assert result.run_id
        assert result.order_count > 0
        assert result.fill_count > 0
        run = context.backtest_run_repository.get_run(result.run_id)
        assert run is not None
        manifest = json.loads(run.run_manifest_json)
        assert manifest["component_manifest"]["signal_component"] == "research.signal_snapshot"
        assert manifest["component_manifest"]["portfolio_construction_component"] == "builtin.bypassed_portfolio"
        assert manifest["signal_source_run_id"] == bound_research_run_id
        snapshot = RunQueryService(
            backtest_run_repository=context.backtest_run_repository,
            order_repository=context.order_repository,
            audit_repository=context.audit_repository,
            data_import_repository=context.data_import_repository,
            research_run_repository=context.research_run_repository,
        ).build_latest_snapshot()
        replay_summary = snapshot["latest_report_replay_summary"]
        assert replay_summary["signal_source_run_id"] == bound_research_run_id
        assert replay_summary["lineage_graph"]["signal_source_run_id"] == bound_research_run_id
        assert replay_summary["lineage_graph"]["causal_research_runs"][0]["research_run_id"] == bound_research_run_id
        assert replay_summary["lineage_graph"]["causal_research_runs"][0]["binding_mode"] == "signal_source"
        assert replay_summary["lineage_graph"]["research_runs"][0]["research_run_id"] == bound_research_run_id


def test_ui_snapshot_exposes_execution_risk_and_replay_queries(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        backtest_service.run(strategy, entrypoint="tests.integration.ui_snapshot")

    runtime_results = [{"name": "data_provider", "ok": True, "message": "ok", "details": {}, "capability": {}}]
    snapshot = _load_ui_operations_snapshot(str(app_path), runtime_results=runtime_results)
    assert snapshot["latest_execution_summary"]["order_count"] >= 0
    assert "recent_orders" in snapshot["latest_execution_summary"]
    assert "risk_audit_logs" in snapshot["latest_risk_alerts"]
    assert snapshot["latest_report_replay_summary"] is not None
    assert "lineage_graph" in snapshot["latest_report_replay_summary"]
    assert snapshot["ui_schema_version"] == 1
    assert snapshot["ui_runtime_checks"][0]["check"] == "data_provider"


def test_ui_snapshot_risk_alerts_follow_backtest_import_run(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint="tests.integration.ui_snapshot.risk_binding")
        first_import_run_id = result.data_lineage.import_run_id
        assert first_import_run_id
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        snapshot = RunQueryService(
            backtest_run_repository=context.backtest_run_repository,
            order_repository=context.order_repository,
            audit_repository=context.audit_repository,
            data_import_repository=context.data_import_repository,
            research_run_repository=context.research_run_repository,
        ).build_latest_snapshot()
        assert snapshot["latest_backtest_run"]["import_run_id"] == first_import_run_id
        assert snapshot["latest_risk_alerts"]["import_run_id"] == first_import_run_id
