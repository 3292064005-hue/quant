from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import yaml

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.app.bootstrap import bootstrap, bootstrap_data_context
from a_share_quant.app.context import AppContext
from a_share_quant.cli import _load_ui_operations_snapshot, main_research
from a_share_quant.plugins import AppPlugin, PluginDescriptor
from a_share_quant.plugins.plugin_manager import PluginLifecycleHookError, PluginManager
from a_share_quant.storage.sqlite_store import SQLiteStore


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


class _TrackingPlugin(AppPlugin):
    descriptor = PluginDescriptor(name="test.tracking", plugin_type="test")

    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def configure(self, context) -> None:
        return None

    def after_workflow_run(self, context, workflow_name: str, payload: dict[str, object], result, error: Exception | None) -> None:
        self._sink.append(f"after:{workflow_name}")

    def shutdown(self, context) -> None:
        self._sink.append("plugin_shutdown")


class _FailingAfterPlugin(AppPlugin):
    descriptor = PluginDescriptor(name="test.fail_after", plugin_type="test")

    def configure(self, context) -> None:
        return None

    def after_workflow_run(self, context, workflow_name: str, payload: dict[str, object], result, error: Exception | None) -> None:
        raise RuntimeError("after hook failed")


class _FailingShutdownPlugin(AppPlugin):
    descriptor = PluginDescriptor(name="test.fail_shutdown", plugin_type="test")

    def configure(self, context) -> None:
        return None

    def shutdown(self, context) -> None:
        raise RuntimeError("shutdown failed")


class _DummyBroker:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def close(self) -> None:
        self._sink.append("broker_close")


class _DummyStore:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def close(self) -> None:
        self._sink.append("store_close")


def test_workflow_execution_records_plugin_lifecycle_events(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv")
        strategy = context.require_strategy_service().build_default()
        workflow = context.require_workflow_registry().get("workflow.backtest")
        result = workflow.run_default(strategy, entrypoint="tests.unit.plugin_lifecycle")
        events = context.require_plugin_manager().lifecycle_events()

    assert result.run_id
    assert any(item["event"] == "before_workflow_run" and item.get("workflow_name") == "workflow.backtest" for item in events)
    assert any(item["event"] == "after_workflow_run" and item.get("workflow_name") == "workflow.backtest" for item in events)


def test_main_research_emits_json_payload_and_persists_runs(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)
    exit_code = main_research([
        "--config",
        str(config_path),
        "--artifact",
        "experiment",
        "--csv",
        "sample_data/daily_bars.csv",
        "--lookback",
        "3",
        "--top-n",
        "2",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["experiment"]["feature_name"] == "momentum"
    assert payload["research_run_id"].startswith("research_")

    with bootstrap_data_context(str(config_path)) as context:
        workflow = context.require_workflow_registry().get("workflow.research")
        recent = workflow.list_recent_runs(limit=5)
    assert any(item["research_run_id"] == payload["research_run_id"] for item in recent)


def test_ui_snapshot_exposes_component_and_plugin_details(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    snapshot = _load_ui_operations_snapshot(str(config_path), runtime_results=[{"name": "ui", "ok": True, "message": "ok", "details": {}, "capability": {}}])

    assert "available_provider_details" in snapshot
    assert "available_workflow_details" in snapshot
    assert "installed_plugin_details" in snapshot
    assert "registered_components" in snapshot
    assert snapshot["ui_schema_version"] == 1
    assert any(item["name"] == "provider.dataset" for item in snapshot["available_provider_details"])
    assert any(item["name"] == "workflow.research" for item in snapshot["available_workflow_details"])
    assert any(item["name"] == "provider.dataset" for item in snapshot["ui_available_provider_details"])
    assert any(item["name"] == "workflow.research" for item in snapshot["ui_available_workflow_details"])
    assert any(item["name"] == "builtin.dataset" for item in snapshot["installed_plugin_details"])


def test_after_hook_failures_are_recorded_and_later_plugins_still_run() -> None:
    sink: list[str] = []
    manager = PluginManager()
    manager.extend([_FailingAfterPlugin(), _TrackingPlugin(sink)])

    try:
        manager.emit_after_workflow_run(object(), "workflow.research", {"artifact_type": "dataset_summary"}, result={"ok": True}, error=None)
    except PluginLifecycleHookError as exc:
        assert exc.hook_name == "after_workflow_run"
        assert exc.plugin_name == "test.fail_after"
    else:  # pragma: no cover - 防止误判
        raise AssertionError("应抛出 PluginLifecycleHookError")

    events = manager.lifecycle_events()
    assert any(item["event"] == "after_workflow_run_error" and item["plugin_name"] == "test.fail_after" for item in events)
    assert any(item["event"] == "after_workflow_run" and item["plugin_name"] == "test.tracking" for item in events)
    assert sink == ["after:workflow.research"]


def test_shutdown_failures_are_recorded_and_following_plugins_still_close() -> None:
    sink: list[str] = []
    manager = PluginManager()
    manager.extend([_TrackingPlugin(sink), _FailingShutdownPlugin()])

    try:
        manager.shutdown(object())
    except PluginLifecycleHookError as exc:
        assert exc.hook_name == "shutdown"
        assert exc.plugin_name == "test.fail_shutdown"
    else:  # pragma: no cover - 防止误判
        raise AssertionError("应抛出 PluginLifecycleHookError")

    events = manager.lifecycle_events()
    assert any(item["event"] == "shutdown_error" and item["plugin_name"] == "test.fail_shutdown" for item in events)
    assert any(item["event"] == "shutdown" and item["plugin_name"] == "test.tracking" for item in events)
    assert sink == ["plugin_shutdown"]


def test_app_context_close_uses_plugin_then_broker_then_store_order() -> None:
    sink: list[str] = []
    manager = PluginManager()
    manager.register(_TrackingPlugin(sink))
    context = AppContext(
        config=None,  # type: ignore[arg-type]
        market_repository=None,  # type: ignore[arg-type]
        order_repository=None,  # type: ignore[arg-type]
        account_repository=None,  # type: ignore[arg-type]
        audit_repository=None,  # type: ignore[arg-type]
        strategy_repository=None,  # type: ignore[arg-type]
        backtest_run_repository=None,  # type: ignore[arg-type]
        data_import_repository=None,  # type: ignore[arg-type]
        dataset_version_repository=None,  # type: ignore[arg-type]
        research_run_repository=None,  # type: ignore[arg-type]
        store=cast(SQLiteStore, _DummyStore(sink)),
        plugin_manager=manager,
        broker=cast(BrokerBase | LiveBrokerPort, _DummyBroker(sink)),
    )

    context.close()

    assert sink == ["plugin_shutdown", "broker_close", "store_close"]
