from __future__ import annotations

from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap, bootstrap_operator_context, bootstrap_trade_operator_context
from a_share_quant.app.plugin_loader import builtin_plugin_names, resolve_plugins
from a_share_quant.config.loader import ConfigLoader


class _DummyQmtClient:
    def get_account(self, last_prices):
        return {"account_id": "acct", "cash": 100000.0, "frozen_cash": 0.0, "total_assets": 100000.0}

    def get_positions(self, last_prices):
        return []

    def submit_order(self, order, fill_price, trade_date):
        return {"order_id": "ord_1"}

    def cancel_order(self, order_id):
        return {"order_id": order_id, "cancelled": True}

    def query_orders(self):
        return []

    def query_trades(self):
        return []

    def heartbeat(self):
        return {"ok": True}



def _write_config(tmp_path: Path, *, runtime_mode: str = "research_backtest", provider: str = "mock") -> Path:
    payload = yaml.safe_load(Path("configs/app.yaml").read_text(encoding="utf-8"))
    payload.setdefault("app", {})["runtime_mode"] = runtime_mode
    payload.setdefault("broker", {})["provider"] = provider
    if provider != "mock":
        payload.setdefault("broker", {})["endpoint"] = "tcp://127.0.0.1:10001"
        payload.setdefault("broker", {})["account_id"] = "acct-demo"
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path



def test_plugin_loader_supports_disable_and_external_plugin(tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "custom_plugins.py"
    module_path.write_text(
        "from a_share_quant.plugins.base import AppPlugin, PluginDescriptor\n"
        "class DemoPlugin(AppPlugin):\n"
        "    descriptor = PluginDescriptor(name='external.demo', plugin_type='demo')\n"
        "    def configure(self, context):\n"
        "        pass\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    config_path = _write_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload.setdefault("plugins", {})["disabled"] = ["builtin.scheduler"]
    payload.setdefault("plugins", {})["external"] = ["custom_plugins:DemoPlugin"]
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    config = ConfigLoader.load(str(config_path))
    plugins = resolve_plugins(config)
    plugin_names = {plugin.descriptor.name for plugin in plugins}

    assert "builtin.scheduler" not in plugin_names
    assert "external.demo" in plugin_names
    assert set(builtin_plugin_names()) - {"builtin.scheduler"} <= plugin_names



def test_bootstrap_operator_context_supports_live_trade_broker_lane(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="live_trade", provider="qmt")

    with bootstrap_operator_context(str(config_path), broker_clients={"qmt": _DummyQmtClient()}) as context:
        context.require_broker()
        context.require_report_service()
        context.require_data_service()
        workflow_names = {entry.name for entry in context.require_workflow_registry().list_entries()}
        assert "workflow.report" in workflow_names
        assert "workflow.research" in workflow_names
        assert "workflow.backtest" not in workflow_names


def test_bootstrap_operator_context_rejects_research_backtest_lane(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="research_backtest", provider="mock")

    from a_share_quant.app.assembly_steps import AssemblyValidationError

    try:
        bootstrap_operator_context(str(config_path))
    except AssemblyValidationError as exc:
        assert "仅支持 paper/live" in str(exc)
    else:  # pragma: no cover - 防御性保护
        raise AssertionError("bootstrap_operator_context 应拒绝 research_backtest lane")


def test_runtime_operator_lane_keeps_report_and_research_workflows_but_no_backtest(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="ptrade")

    with bootstrap_operator_context(str(config_path), broker_clients={"ptrade": _DummyQmtClient()}) as context:
        workflow_names = {entry.name for entry in context.require_workflow_registry().list_entries()}
        assert workflow_names == {"workflow.report", "workflow.replay", "workflow.research"}


def test_bootstrap_backtest_context_binds_plugin_manager_to_strategy_service_and_runtime(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="research_backtest", provider="mock")

    with bootstrap(str(config_path)) as context:
        plugin_manager = context.require_plugin_manager()
        strategy_service = context.require_strategy_service()
        assert strategy_service.plugin_manager is plugin_manager
        strategy = strategy_service.build_default()
        assert getattr(strategy, "_execution_runtime").plugin_manager is plugin_manager


def test_bootstrap_trade_operator_context_binds_plugin_manager_to_trade_orchestrator(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="ptrade")

    with bootstrap_trade_operator_context(str(config_path), broker_clients={"ptrade": _DummyQmtClient()}) as context:
        plugin_manager = context.require_plugin_manager()
        orchestrator = context.require_trade_orchestrator_service()
        workflow = context.require_workflow_registry().get("workflow.operator_trade")
        assert orchestrator.plugin_manager is plugin_manager
        assert orchestrator.progress_service.plugin_manager is plugin_manager
        assert workflow.plugin_manager is plugin_manager


def test_runtime_assembly_exposes_explicit_install_plan_and_layered_context(tmp_path: Path) -> None:
    from a_share_quant.app.context import OperatorRuntimeContext, PersistenceContext, RegistryContext, ResearchRuntimeContext
    from a_share_quant.app.runtime_assembly import AssemblyRequest, resolve_runtime_assembly

    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="ptrade")
    config = ConfigLoader.load(str(config_path))
    assembly = resolve_runtime_assembly(config.app.runtime_mode)
    plan = assembly.describe_plan(
        AssemblyRequest(
            include_data_service=True,
            include_report_service=True,
            include_broker=True,
            require_operator_lane=True,
            include_trade_orchestrator=True,
        )
    )
    assert plan[:3] == ["install_registries", "create_plugin_manager", "install_data_stack"]
    assert "install_operator_trade_stack" in plan
    assert plan[-2:] == ["bind_plugin_manager_to_runtime", "configure_plugin_manager"]

    with bootstrap_trade_operator_context(str(config_path), broker_clients={"ptrade": _DummyQmtClient()}) as context:
        assert isinstance(context.require_persistence_context(), PersistenceContext)
        assert isinstance(context.require_registry_context(), RegistryContext)
        assert isinstance(context.require_research_runtime_context(), ResearchRuntimeContext)
        assert isinstance(context.require_operator_runtime_context(), OperatorRuntimeContext)
        assert context.require_operator_runtime_context().trade_orchestrator_service is context.require_trade_orchestrator_service()
        assert context.require_research_runtime_context().report_service is context.require_report_service()
