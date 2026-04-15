from __future__ import annotations

from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.engines.execution_registry import build_builtin_execution_registry
from a_share_quant.workflows import research_workflow as workflow_module


def _write_config(temp_dir: Path) -> Path:
    payload = yaml.safe_load(Path("configs/app.yaml").read_text())
    runtime_dir = temp_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = temp_dir / "app.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_execution_registry_exposes_builtin_entries() -> None:
    registry = build_builtin_execution_registry()
    assert registry.get_entry("fill", "volume_share").descriptor.model_type == "fill"
    assert registry.get_entry("slippage", "bps").descriptor.config_fields == ("backtest.slippage_bps",)
    assert len(registry.list_entries()) == 4


def test_research_workflow_exposes_feature_signal_and_experiment_summary(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        data_service = context.require_data_service()
        data_service.import_csv("sample_data/daily_bars.csv")
        workflow = context.require_workflow_registry().get("workflow.research")
        feature = workflow.run_feature_snapshot(feature_name="momentum", lookback=3)
        signal = workflow.run_signal_snapshot(feature_name="momentum", lookback=3, top_n=2)
        experiment = workflow.summarize_experiment(feature_name="momentum", lookback=3, top_n=2)
        recent = workflow.list_recent_runs()

    assert feature["feature_spec"]["name"] == "momentum"
    assert feature["value_count"] > 0
    assert len(signal["selected_symbols"]) <= 2
    assert experiment["experiment"]["feature_name"] == "momentum"
    assert experiment["signal"]["selected_count"] == len(signal["selected_symbols"])
    assert feature["research_run_id"] is None
    assert signal["research_run_id"] is None
    assert feature["recorded"] is False
    assert signal["recorded"] is False
    assert experiment["research_run_id"].startswith("research_")
    assert experiment["research_session_id"].startswith("research_session_")
    assert any(item["research_run_id"] == experiment["research_run_id"] for item in recent)

    with bootstrap_data_context(str(config_path)) as context:
        children = context.research_run_repository.list_children(experiment["research_run_id"])
    assert [item["artifact_type"] for item in children] == ["dataset_summary", "feature_snapshot", "signal_snapshot"]
    assert all(item["research_session_id"] == experiment["research_session_id"] for item in children)
    assert all(item["is_primary_run"] is False for item in children)


def test_strategy_manifest_and_blueprint_include_universe_component(temp_config_dir: Path) -> None:
    from a_share_quant.app.bootstrap import bootstrap_storage_context
    from a_share_quant.config.loader import ConfigLoader
    from a_share_quant.services.strategy_service import StrategyService

    app_path = temp_config_dir / "app.yaml"
    config = ConfigLoader.load(str(app_path))
    with bootstrap_storage_context(str(app_path)) as context:
        service = StrategyService(config, context.strategy_repository)
        strategy = service.build_default()
        assert strategy._component_manifest["universe_component"] == "builtin.all_active_a_share"
        assert strategy._strategy_blueprint["factor"] == "builtin.momentum"
        saved = service.get_saved_strategy_definition(config.strategy.strategy_id)
        assert saved is not None
        assert saved["strategy_blueprint"]["universe"] == "builtin.all_active_a_share"


def test_research_experiment_batch_uses_single_primary_run(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    specs = [
        {"task_name": "lb3", "feature_name": "momentum", "lookback": 3, "top_n": 2},
        {"task_name": "lb5", "feature_name": "momentum", "lookback": 5, "top_n": 3},
    ]
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv")
        workflow = context.require_workflow_registry().get("workflow.research")
        payload = workflow.summarize_experiment_batch([workflow_module.ResearchTaskSpec.from_payload(i, item) for i, item in enumerate(specs)])
        recent = context.research_run_repository.list_recent(limit=10)
        batch_row = context.research_run_repository.get(payload["research_run_id"])
    assert payload["aggregate"]["task_count"] == 2
    assert [item["artifact_type"] for item in recent] == ["experiment_batch_summary"]
    assert batch_row is not None
    assert batch_row["root_research_run_id"] == batch_row["research_run_id"]
    with bootstrap_data_context(str(config_path)) as context:
        for task in payload["tasks"]:
            experiment_row = context.research_run_repository.get(task["research_run_id"])
            assert experiment_row is not None
            assert experiment_row["is_primary_run"] is False
            assert experiment_row["parent_research_run_id"] == payload["research_run_id"]
            assert experiment_row["root_research_run_id"] == payload["research_run_id"]
            assert task["signal_snapshot_run_id"]




def test_research_workflow_dataset_snapshot_uses_persistent_cache(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv")
        workflow = context.require_workflow_registry().get("workflow.research")
        first = workflow.load_snapshot_summary()
        second = workflow.load_snapshot_summary()
        cache_rows = context.store.query(
            "SELECT artifact_type, hit_count FROM research_cache_entries WHERE artifact_type = ?",
            ("dataset_summary",),
        )
    assert first["cache_meta"]["cache_hit"] is False
    assert second["cache_meta"]["cache_hit"] is True
    assert first["research_run_id"] is None
    assert second["research_run_id"] is None
    assert len(cache_rows) == 1
    assert cache_rows[0]["artifact_type"] == "dataset_summary"
    assert cache_rows[0]["hit_count"] >= 1

def test_research_workflow_feature_snapshot_uses_persistent_cache(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv")
        workflow = context.require_workflow_registry().get("workflow.research")
        first = workflow.run_feature_snapshot(feature_name="momentum", lookback=3)
        second = workflow.run_feature_snapshot(feature_name="momentum", lookback=3)
        cache_rows = context.store.query(
            "SELECT artifact_type, hit_count FROM research_cache_entries WHERE artifact_type = ?",
            ("feature_snapshot",),
        )
    assert first["cache_meta"]["cache_hit"] is False
    assert second["cache_meta"]["cache_hit"] is True
    assert first["research_run_id"] is None
    assert second["research_run_id"] is None
    assert len(cache_rows) == 1
    assert cache_rows[0]["artifact_type"] == "feature_snapshot"
    assert cache_rows[0]["hit_count"] >= 1


def test_research_workflow_query_snapshots_require_explicit_record_flag(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv")
        workflow = context.require_workflow_registry().get("workflow.research")
        signal = workflow.run_signal_snapshot(feature_name="momentum", lookback=3, top_n=2, record=True)
        recent = workflow.list_recent_runs(limit=5)
    assert signal["research_run_id"].startswith("research_")
    assert signal["recorded"] is True
    assert any(item["research_run_id"] == signal["research_run_id"] for item in recent)
