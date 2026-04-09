from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context


def _write_operator_config(temp_dir: Path) -> Path:
    payload = yaml.safe_load(Path("configs/operator_paper_trade.yaml").read_text(encoding="utf-8"))
    payload.pop("extends", None)
    payload.setdefault("app", {})["name"] = "AShareQuantWorkstation"
    payload.setdefault("app", {})["environment"] = "local"
    payload.setdefault("app", {})["timezone"] = "Asia/Shanghai"
    payload.setdefault("app", {})["path_resolution_mode"] = "config_dir"
    payload.setdefault("data", {}).setdefault("default_exchange", "SSE")
    payload.setdefault("data", {}).setdefault("default_csv_encoding", "utf-8")
    payload.setdefault("data", {}).setdefault("provider", "csv")
    payload.setdefault("data", {}).setdefault("calendar_policy", "demo")
    payload.setdefault("database", {})
    payload.setdefault("risk", {}).setdefault("max_position_weight", 0.6)
    payload.setdefault("risk", {}).setdefault("max_order_value", 600000.0)
    payload.setdefault("risk", {}).setdefault("blocked_symbols", [])
    payload.setdefault("risk", {}).setdefault("kill_switch", False)
    payload.setdefault("strategy", {}).setdefault("strategy_id", "momentum_top_n")
    payload.setdefault("strategy", {}).setdefault("version", "1.0.0")
    payload.setdefault("strategy", {}).setdefault("params", {})
    payload.setdefault("backtest", {}).setdefault("initial_cash", 1000000.0)
    payload.setdefault("backtest", {}).setdefault("fee_bps", 3.0)
    payload.setdefault("backtest", {}).setdefault("tax_bps", 10.0)
    payload.setdefault("backtest", {}).setdefault("slippage_bps", 5.0)
    payload.setdefault("backtest", {}).setdefault("benchmark_symbol", "600000.SH")
    payload.setdefault("plugins", {}).setdefault("enabled_builtin", [])
    payload.setdefault("plugins", {}).setdefault("disabled", [])
    payload.setdefault("plugins", {}).setdefault("external", [])
    payload.setdefault("operator", {}).setdefault("require_approval", False)
    payload.setdefault("operator", {}).setdefault("max_batch_orders", 20)
    payload.setdefault("operator", {}).setdefault("default_requested_by", "operator")
    payload.setdefault("operator", {}).setdefault("fail_fast", False)
    runtime_dir = temp_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = temp_dir / "operator_paper_trade.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_operator_submit_order_script_prefers_local_source_tree(tmp_path: Path) -> None:
    config_path = _write_operator_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)

    client_module = tmp_path / "demo_submit_broker_factory.py"
    client_module.write_text(
        """
from datetime import date

from a_share_quant.domain.models import AccountSnapshot, Fill, PositionSnapshot


class DemoBroker:
    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self):
        return True

    def get_account(self, last_prices=None):
        return AccountSnapshot(cash=100000.0, available_cash=100000.0, market_value=0.0, total_assets=100000.0, pnl=0.0)

    def get_positions(self, last_prices=None):
        return []

    def submit_order(self, order, fill_price, trade_date):
        order.broker_order_id = "broker_order_demo_1"
        return Fill(
            fill_id="fill_demo_1",
            order_id=order.order_id,
            trade_date=trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date)),
            ts_code=order.ts_code,
            side=order.side,
            fill_price=float(fill_price),
            fill_quantity=int(order.quantity),
            fee=0.0,
            tax=0.0,
        )

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return []

    def query_trades(self):
        return []


def create_client():
    return DemoBroker()
""".strip(),
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/operator_submit_order.py",
            "--config",
            str(config_path),
            "--broker-client-factory",
            "demo_submit_broker_factory:create_client",
            "--symbol",
            "600000.SH",
            "--side",
            "BUY",
            "--price",
            "10.50",
            "--quantity",
            "100",
            "--trade-date",
            "2026-01-05",
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(completed.stdout)
    assert payload["session"]["runtime_mode"] == "paper_trade"
    assert payload["session"]["status"] == "COMPLETED"
    assert payload["orders"][0]["broker_order_id"] == "broker_order_demo_1"
    assert payload["fills"][0]["fill_id"] == "fill_demo_1"


def test_operator_submit_order_script_accepts_broker_payload_with_external_ids(tmp_path: Path) -> None:
    config_path = _write_operator_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)

    client_module = tmp_path / "demo_submit_broker_external_factory.py"
    client_module.write_text(
        """
class DemoBroker:
    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self):
        return True

    def get_account(self, last_prices=None):
        return {"cash": 100000.0, "available_cash": 100000.0, "market_value": 0.0, "total_assets": 100000.0, "pnl": 0.0, "cum_pnl": 0.0, "daily_pnl": 0.0, "drawdown": 0.0}

    def get_positions(self, last_prices=None):
        return []

    def submit_order(self, order, fill_price, trade_date):
        return {
            "fill_id": "fill_demo_ext_1",
            "broker_order_id": "broker_order_demo_ext_1",
            "trade_date": str(trade_date),
            "symbol": order.ts_code,
            "side": order.side.value,
            "fill_price": float(fill_price),
            "fill_quantity": int(order.quantity),
            "fee": 0.0,
            "tax": 0.0,
        }

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return [{
            "broker_order_id": "broker_order_demo_ext_1",
            "trade_date": "2024-01-02",
            "symbol": "600000.SH",
            "side": "BUY",
            "price": 10.5,
            "quantity": 100,
            "reason": "external",
            "status": "FILLED",
        }]

    def query_trades(self):
        return [{
            "fill_id": "fill_demo_ext_1",
            "broker_order_id": "broker_order_demo_ext_1",
            "trade_date": "2024-01-02",
            "symbol": "600000.SH",
            "side": "BUY",
            "fill_price": 10.5,
            "fill_quantity": 100,
            "fee": 0.0,
            "tax": 0.0,
        }]


def create_client():
    return DemoBroker()
""".strip(),
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/operator_submit_order.py",
            "--config",
            str(config_path),
            "--broker-client-factory",
            "demo_submit_broker_external_factory:create_client",
            "--symbol",
            "600000.SH",
            "--side",
            "BUY",
            "--price",
            "10.50",
            "--quantity",
            "100",
            "--trade-date",
            "2026-01-05",
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(completed.stdout)
    assert payload["session"]["status"] == "COMPLETED"
    assert payload["orders"][0]["order_id"].startswith("operator_600000.SH")
    assert payload["orders"][0]["broker_order_id"] == "broker_order_demo_ext_1"
    assert payload["fills"][0]["order_id"] == payload["orders"][0]["order_id"]
    assert payload["fills"][0]["broker_order_id"] == "broker_order_demo_ext_1"
