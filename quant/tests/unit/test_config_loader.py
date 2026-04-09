from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from a_share_quant.config.loader import ConfigLoader, ConfigLoaderError


def test_config_loader_merges_companion_files(temp_config_dir: Path) -> None:
    payload = (temp_config_dir / "risk.yaml").read_text(encoding="utf-8")
    assert "block_st" in payload
    config = ConfigLoader.load(temp_config_dir / "app.yaml")
    assert config.risk.rules.block_st is True
    assert config.backtest.metrics.annual_trading_days == 252


def test_app_yaml_overrides_companion_files(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("backtest", {})["data_access_mode"] = "stream"
    payload.setdefault("data", {})["provider"] = "tushare"
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    broker_path = temp_config_dir / "broker" / "qmt.yaml"
    broker_path.write_text(
        yaml.safe_dump(
            {
                "provider": "qmt",
                "endpoint": "",
                "account_id": "",
                "operation_timeout_seconds": 15.0,
                "strict_contract_mapping": True,
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    config = ConfigLoader.load(app_path)
    assert config.backtest.data_access_mode == "stream"
    assert config.data.provider == "tushare"
    assert config.broker.provider == "qmt"
    assert config.broker.endpoint == "tcp://127.0.0.1:1234"
    assert config.broker.account_id == "demo"


def test_config_loader_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigLoaderError):
        ConfigLoader.load(tmp_path / "missing.yaml")


def test_config_loader_rejects_invalid_runtime_enums(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("backtest", {})["data_access_mode"] = "invalid_mode"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError):
        ConfigLoader.load(app_path)


def test_config_loader_resolves_runtime_paths_relative_to_config_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True)
    app_path = config_dir / "app.yaml"
    app_path.write_text(
        yaml.safe_dump(
            {
                "app": {"logs_dir": "../runtime/logs", "path_resolution_mode": "config_dir"},
                "data": {"storage_dir": "../runtime/data", "reports_dir": "../runtime/reports"},
                "database": {"path": "../runtime/a_share_quant.db"},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    config = ConfigLoader.load(app_path)
    assert config.app.logs_dir == str((tmp_path / "runtime" / "logs").resolve())
    assert config.database.path == str((tmp_path / "runtime" / "a_share_quant.db").resolve())


def test_config_loader_supports_cwd_path_resolution_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "configs"
    cwd_dir = tmp_path / "workspace"
    config_dir.mkdir(parents=True)
    cwd_dir.mkdir(parents=True)
    app_path = config_dir / "app.yaml"
    app_path.write_text(
        yaml.safe_dump(
            {
                "app": {"logs_dir": "runtime/logs", "path_resolution_mode": "cwd"},
                "data": {"storage_dir": "runtime/data", "reports_dir": "runtime/reports"},
                "database": {"path": "runtime/runtime.db"},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(cwd_dir)
    config = ConfigLoader.load(app_path)
    assert config.database.path == str((cwd_dir / "runtime" / "runtime.db").resolve())


def test_config_loader_supports_extends_chain(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True)
    base = config_dir / "base.yaml"
    child = config_dir / "child.yaml"
    base.write_text(
        yaml.safe_dump(
            {
                "app": {"logs_dir": "../runtime/logs", "path_resolution_mode": "config_dir", "runtime_mode": "research_backtest"},
                "data": {"storage_dir": "../runtime/data", "reports_dir": "../runtime/reports"},
                "database": {"path": "../runtime/base.db"},
                "broker": {"provider": "mock"},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    child.write_text(
        yaml.safe_dump(
            {
                "extends": "base.yaml",
                "app": {"runtime_mode": "paper_trade"},
                "database": {"path": "../runtime/operator.db"},
                "broker": {"provider": "qmt", "endpoint": "tcp://127.0.0.1:12345", "account_id": "demo"},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    config = ConfigLoader.load(child)
    assert config.app.runtime_mode == "paper_trade"
    assert config.database.path == str((tmp_path / "runtime" / "operator.db").resolve())
    assert config.broker.provider == "qmt"
    assert config.broker.endpoint == "tcp://127.0.0.1:12345"


def test_config_loader_rejects_cyclic_extends(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True)
    first = config_dir / "first.yaml"
    second = config_dir / "second.yaml"
    first.write_text(yaml.safe_dump({"extends": "second.yaml"}, allow_unicode=True, sort_keys=False), encoding="utf-8")
    second.write_text(yaml.safe_dump({"extends": "first.yaml"}, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ConfigLoaderError):
        ConfigLoader.load(first)


def test_config_loader_falls_back_to_packaged_configs_when_repo_configs_absent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = ConfigLoader.load("configs/operator_paper_trade_demo.yaml")
    assert config.app.runtime_mode == "paper_trade"
    assert config.broker.provider == "qmt"
    assert config.broker.client_factory == "a_share_quant.demo.operator_demo_broker:create_client"
