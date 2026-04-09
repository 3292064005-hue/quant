from __future__ import annotations

import json
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context, bootstrap_storage_context
from a_share_quant.cli import main_research
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.services.strategy_service import StrategyService


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


def test_strategy_service_binds_component_runtime_in_storage_context(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    config = ConfigLoader.load(str(app_path))
    with bootstrap_storage_context(str(app_path)) as context:
        service = StrategyService(config, context.strategy_repository)
        strategy = service.build_default()
        runtime = getattr(strategy, "_execution_runtime", None)
        assert runtime is not None
        assert runtime.manifest.factor_component == "builtin.momentum"
        assert runtime.required_history_bars(strategy) == config.strategy.lookback + 1


def test_main_research_default_demo_parameters_produce_non_empty_signal(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)
    exit_code = main_research([
        "--config",
        str(config_path),
        "--artifact",
        "experiment",
        "--csv",
        "sample_data/daily_bars.csv",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["feature"]["value_count"] > 0
    assert payload["signal"]["selected_count"] > 0


def test_plugin_descriptor_exposes_capability_and_hook_contracts(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        descriptors = {item.name: item for item in context.require_plugin_manager().descriptors()}
    dataset_descriptor = descriptors["builtin.dataset"]
    assert "dataset_snapshot" in dataset_descriptor.capability_tags
    assert "before_workflow_run" in dataset_descriptor.hook_contracts
