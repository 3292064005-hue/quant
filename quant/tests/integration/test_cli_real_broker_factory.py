from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from a_share_quant.cli import main_check_runtime, main_daily_run


def test_check_runtime_uses_broker_client_factory_for_real_broker(temp_config_dir: Path, tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "cli_broker_factory.py"
    module_path.write_text(
        """
class DemoClient:
    def get_account(self, last_prices):
        return None
    def get_positions(self, last_prices):
        return []
    def submit_order(self, order, fill_price, trade_date):
        return None
    def cancel_order(self, broker_order_id):
        return None
    def query_orders(self):
        return []
    def query_trades(self):
        return []
    def heartbeat(self):
        return True

def build_client(config=None, provider=None):
    return DemoClient()
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("app", {})["runtime_mode"] = "live_trade"
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    payload["broker"]["client_factory"] = "cli_broker_factory:build_client"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    broker_cfg = temp_config_dir / "broker" / "qmt.yaml"
    broker_cfg.write_text(
        yaml.safe_dump(
            {
                "provider": "qmt",
                "endpoint": "tcp://127.0.0.1:1234",
                "account_id": "demo",
                "operation_timeout_seconds": 15.0,
                "strict_contract_mapping": True,
                "client_factory": "cli_broker_factory:build_client",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    exit_code = main_check_runtime(["--config", str(app_path), "--strict"])
    assert exit_code == 0


def test_daily_run_rejects_real_broker_factory_in_research_backtest(temp_config_dir: Path, tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "cli_run_broker_factory.py"
    module_path.write_text(
        """
class DemoClient:
    def __init__(self):
        self.connected = False
        self.closed = False
    def connect(self):
        self.connected = True
    def close(self):
        self.closed = True
    def get_account(self, last_prices):
        return {"cash": 1000000.0, "available_cash": 1000000.0, "market_value": 0.0, "total_assets": 1000000.0, "pnl": 0.0}
    def get_positions(self, last_prices):
        return []
    def submit_order(self, order, fill_price, trade_date):
        return {"fill_id": f"fill_{order.order_id}", "order_id": order.order_id, "trade_date": str(trade_date), "ts_code": order.ts_code, "side": order.side, "fill_price": fill_price, "fill_quantity": order.quantity, "fee": 0.0, "tax": 0.0, "run_id": order.run_id}
    def cancel_order(self, broker_order_id):
        return None
    def query_orders(self):
        return []
    def query_trades(self):
        return []
    def heartbeat(self):
        return True

def build_client(config=None, provider=None):
    return DemoClient()
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    payload["broker"]["client_factory"] = "cli_run_broker_factory:build_client"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    broker_cfg = temp_config_dir / "broker" / "qmt.yaml"
    broker_cfg.write_text(
        yaml.safe_dump(
            {
                "provider": "qmt",
                "endpoint": "tcp://127.0.0.1:1234",
                "account_id": "demo",
                "operation_timeout_seconds": 15.0,
                "strict_contract_mapping": True,
                "client_factory": "cli_run_broker_factory:build_client",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    csv_path = Path(__file__).resolve().parents[2] / "sample_data" / "daily_bars.csv"
    with pytest.raises(SystemExit, match="research_backtest"):
        main_daily_run(["--config", str(app_path), "--csv", str(csv_path)])
