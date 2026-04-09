from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.cli import _load_ui_operations_snapshot
from a_share_quant.domain.models import Bar, OrderRequest, OrderSide, OrderStatus
from a_share_quant.engines.execution_engine import ExecutionEngine
from a_share_quant.engines.execution_models import BpsFeeModel, BpsSlippageModel, VolumeShareFillModel


def _write_config(temp_dir: Path) -> Path:
    payload = yaml.safe_load(Path("configs/app.yaml").read_text())
    runtime_dir = temp_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = temp_dir / "app.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
    return config_path


def test_execution_engine_supports_partial_fill() -> None:
    broker = MockBroker(initial_cash=100000.0, fee_bps=3.0, tax_bps=10.0)
    broker.connect()
    engine = ExecutionEngine(
        broker,
        slippage_model=BpsSlippageModel(0.0),
        fill_model=VolumeShareFillModel(max_volume_share=1.0, lot_size=100, allow_partial_fill=True),
        fee_model=BpsFeeModel(3.0),
    )
    order = OrderRequest(
        order_id="o_partial",
        trade_date=date(2026, 1, 5),
        strategy_id="demo",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.0,
        quantity=200,
        reason="partial",
    )
    bar = Bar(
        ts_code="600000.SH",
        trade_date=date(2026, 1, 5),
        open=10.0,
        high=10.2,
        low=9.9,
        close=10.0,
        volume=100.0,
        amount=1000.0,
    )

    outcome = engine.execute([order], {bar.ts_code: bar}, trade_date=bar.trade_date)

    assert len(outcome.fills) == 1
    assert outcome.fills[0].fill_quantity == 100
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == 100
    assert outcome.tickets[order.order_id].remaining_quantity == 100
    assert any(report.status == OrderStatus.PARTIALLY_FILLED for report in outcome.reports)


def test_bootstrap_data_context_registers_providers_workflows_and_plugins(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    with bootstrap_data_context(str(config_path)) as context:
        provider_names = {entry.name for entry in context.require_provider_registry().list_entries()}
        workflow_names = {entry.name for entry in context.require_workflow_registry().list_entries()}
        plugin_names = set(context.require_plugin_manager().names())

    assert {"provider.calendar", "provider.instrument", "provider.bar", "provider.feature", "provider.dataset"} <= provider_names
    assert "workflow.research" in workflow_names
    assert {"builtin.risk", "builtin.analyser", "builtin.scheduler", "builtin.dataset"} <= plugin_names


def test_ui_operations_snapshot_exposes_provider_workflow_and_plugin_metadata(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    snapshot = _load_ui_operations_snapshot(str(config_path))

    assert "available_providers" in snapshot
    assert "available_workflows" in snapshot
    assert "installed_plugins" in snapshot
    assert "provider.dataset" in snapshot["available_providers"]
    assert "workflow.research" in snapshot["available_workflows"]
    assert "builtin.dataset" in snapshot["installed_plugins"]
