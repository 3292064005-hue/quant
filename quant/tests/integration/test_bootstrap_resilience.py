from pathlib import Path

import pytest
import yaml

from a_share_quant.app.bootstrap import bootstrap, bootstrap_storage_context


class _ConnectFailingClient:
    def __init__(self) -> None:
        self.closed = False

    def connect(self):
        raise RuntimeError("connect failed")

    def close(self):
        self.closed = True

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


def test_bootstrap_rejects_real_broker_in_research_backtest(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    broker_cfg = temp_config_dir / "broker" / "qmt.yaml"
    broker_cfg.write_text(
        yaml.safe_dump({"provider": "qmt", "endpoint": "tcp://127.0.0.1:1234", "account_id": "demo", "operation_timeout_seconds": 15.0, "strict_contract_mapping": True}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    client = _ConnectFailingClient()
    with pytest.raises(ValueError, match="research_backtest"):
        bootstrap(str(app_path), broker_clients={"qmt": client})
    assert client.closed is False


def test_storage_context_does_not_require_real_broker_client(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_path.read_text(encoding="utf-8"))
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    app_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    broker_cfg = temp_config_dir / "broker" / "qmt.yaml"
    broker_cfg.write_text(
        yaml.safe_dump({"provider": "qmt", "endpoint": "tcp://127.0.0.1:1234", "account_id": "demo", "operation_timeout_seconds": 15.0, "strict_contract_mapping": True}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    with bootstrap_storage_context(str(app_path)) as context:
        assert context.broker is None
        rows = context.store.query("SELECT version FROM schema_version WHERE singleton_id = 1")
        assert rows[0]["version"] >= 0


def test_bootstrap_rejects_real_broker_factory_in_research_backtest(temp_config_dir: Path, tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "bootstrap_broker_factory.py"
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
        return {"cash": 100.0, "available_cash": 100.0, "market_value": 0.0, "total_assets": 100.0, "pnl": 0.0}
    def get_positions(self, last_prices):
        return []
    def submit_order(self, order, fill_price, trade_date):
        return {"fill_id": "f1", "order_id": order.order_id, "trade_date": str(trade_date), "ts_code": order.ts_code, "side": order.side, "fill_price": fill_price, "fill_quantity": order.quantity, "fee": 0.0, "tax": 0.0}
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
    payload["broker"]["client_factory"] = "bootstrap_broker_factory:build_client"
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
                "client_factory": "bootstrap_broker_factory:build_client",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="research_backtest"):
        bootstrap(str(app_path))
