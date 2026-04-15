from __future__ import annotations

import json
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_trade_operator_context
from a_share_quant.cli_operator import main_operator_submit_signal


class _DummyLiveClient:
    def get_account(self, last_prices):
        return {
            "account_id": "acct-demo",
            "cash": 1_000_000.0,
            "available_cash": 1_000_000.0,
            "frozen_cash": 0.0,
            "market_value": 0.0,
            "total_assets": 1_000_000.0,
        }

    def get_positions(self, last_prices):
        return []

    def submit_order(self, order, fill_price, trade_date):
        return {
            "order_id": order.order_id,
            "broker_order_id": f"broker_{order.order_id}",
            "fill_id": f"fill_{order.order_id}",
            "ts_code": order.ts_code,
            "side": order.side.value,
            "fill_price": float(fill_price),
            "fill_quantity": int(order.quantity),
            "account_id": order.account_id,
            "trade_date": trade_date.isoformat(),
            "fee": 0.0,
            "tax": 0.0,
        }

    def cancel_order(self, order_id):
        return {"order_id": order_id, "cancelled": True}

    def query_orders(self):
        return []

    def query_trades(self):
        return []

    def heartbeat(self):
        return {"ok": True}


def create_client():
    return _DummyLiveClient()


def _write_operator_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(Path("configs/app.yaml").read_text(encoding="utf-8"))
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["runtime_mode"] = "paper_trade"
    payload.setdefault("broker", {})["provider"] = "ptrade"
    payload.setdefault("broker", {})["account_id"] = "acct-demo"
    payload.setdefault("broker", {})["endpoint"] = "tcp://127.0.0.1:10001"
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = tmp_path / "operator.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_main_operator_submit_signal_emits_execution_intent_payload(tmp_path: Path, capsys) -> None:
    config_path = _write_operator_config(tmp_path)

    with bootstrap_trade_operator_context(str(config_path), broker_clients={"ptrade": _DummyLiveClient()}) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)
        research_workflow = context.require_workflow_registry().get("workflow.research")
        signal_payload = research_workflow.run_signal_snapshot(
            feature_name="momentum",
            lookback=5,
            top_n=2,
            record=True,
        )

    exit_code = main_operator_submit_signal([
        "--config",
        str(config_path),
        "--research-run-id",
        signal_payload["research_run_id"],
        "--approved",
        "--account-id",
        "acct-demo",
        "--broker-client-factory",
        "tests.integration.test_operator_submit_signal_entrypoint:create_client",
    ])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["intent"]["intent_type"] == "research.signal_snapshot"
    assert payload["session"]["command_type"] == "submit_execution_intent"
    assert payload["session"]["submitted_count"] == len(payload["planned_orders"])


def test_main_operator_submit_signal_requires_explicit_research_run_id(tmp_path: Path) -> None:
    config_path = _write_operator_config(tmp_path)

    try:
        main_operator_submit_signal([
            "--config",
            str(config_path),
            "--approved",
            "--account-id",
            "acct-demo",
            "--broker-client-factory",
            "tests.integration.test_operator_submit_signal_entrypoint:create_client",
        ])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - 防御性保护
        raise AssertionError("operator_submit_signal 应要求显式 research_run_id")
