from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.cli import main_operator_snapshot


def _write_config(temp_dir: Path, *, runtime_mode: str = "research_backtest", provider: str = "mock", client_factory: str | None = None) -> Path:
    payload = yaml.safe_load(Path("configs/app.yaml").read_text(encoding="utf-8"))
    runtime_dir = temp_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    payload.setdefault("app", {})["runtime_mode"] = runtime_mode
    payload.setdefault("broker", {})["provider"] = provider
    payload.setdefault("broker", {})["endpoint"] = "tcp://127.0.0.1:12345"
    payload.setdefault("broker", {})["account_id"] = "demo-account"
    payload.setdefault("broker", {})["client_factory"] = client_factory
    config_path = temp_dir / "app.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_operator_snapshot_rejects_research_backtest_with_clean_message(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="research_backtest", provider="mock")

    with pytest.raises(SystemExit, match="paper_trade/live_trade"):
        main_operator_snapshot(["--config", str(config_path)])


def test_operator_snapshot_requires_factory_with_clean_message(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="qmt", client_factory=None)

    with pytest.raises(SystemExit, match="broker client factory"):
        main_operator_snapshot(["--config", str(config_path)])


def test_operator_sync_script_prefers_local_source_tree_from_external_cwd(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [sys.executable, str(repo_root / "scripts/operator_sync_session.py"), "--help"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "session-id" in completed.stdout




def test_operator_submit_order_runtime_error_is_wrapped_as_clean_system_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _write_config(
        tmp_path,
        runtime_mode="paper_trade",
        provider="qmt",
        client_factory="a_share_quant.demo.operator_demo_broker:create_client",
    )

    from a_share_quant import cli as cli_module

    def _boom(*args, **kwargs):
        raise RuntimeError("broker heartbeat 失败，禁止提交 operator trade")

    monkeypatch.setattr(cli_module, "bootstrap_trade_operator_context", _boom)

    with pytest.raises(SystemExit, match="operator_submit_order 失败：broker heartbeat 失败"):
        cli_module.main_operator_submit_order(
            [
                "--config",
                str(config_path),
                "--symbol",
                "600000.SH",
                "--side",
                "BUY",
                "--price",
                "10.0",
                "--quantity",
                "100",
            ]
        )

def test_csv_import_persists_calendar_degradation_summary(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        data_service = context.require_data_service()
        data_service.import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)
        latest = context.data_import_repository.get_latest_run()
        assert latest is not None
        flags = json.loads(latest.degradation_flags_json)
        warnings = json.loads(latest.warnings_json)
        events = context.data_import_repository.list_quality_events(latest.import_run_id)

    assert "calendar_inferred_from_bars" in flags
    assert any("bars" in item for item in warnings)
    assert any(item["event_type"] == "calendar_missing" for item in events)
