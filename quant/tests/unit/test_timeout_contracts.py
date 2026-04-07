from __future__ import annotations

import time
from pathlib import Path

import yaml

from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.adapters.data.tushare_adapter import TushareDataAdapter
from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.core.exceptions import ExternalServiceTimeoutError
from a_share_quant.domain.models import AccountSnapshot


class _SlowTushareClient:
    def stock_basic(self, **kwargs):
        time.sleep(0.05)
        return []


def test_tushare_adapter_raises_timeout_for_slow_external_call() -> None:
    adapter = TushareDataAdapter(token="dummy", client=_SlowTushareClient(), timeout_seconds=0.01)
    try:
        adapter.fetch_bundle(start_date="20260101", end_date="20260106")
    except ExternalServiceTimeoutError as exc:
        assert "tushare.stock_basic" in str(exc)
    else:
        raise AssertionError("expected timeout")


class _SlowBrokerClient:
    def get_account(self, last_prices):
        time.sleep(0.05)
        return AccountSnapshot(cash=1.0, available_cash=1.0, market_value=0.0, total_assets=1.0, pnl=0.0)

    def get_positions(self, last_prices):
        return []

    def submit_order(self, order, fill_price, trade_date):
        raise NotImplementedError

    def cancel_order(self, broker_order_id):
        return None

    def query_orders(self):
        return []

    def query_trades(self):
        return []

    def heartbeat(self):
        return True


def test_qmt_adapter_raises_timeout_for_slow_external_call() -> None:
    adapter = QMTAdapter(_SlowBrokerClient(), timeout_seconds=0.01)
    try:
        adapter.get_account({})
    except ExternalServiceTimeoutError as exc:
        assert "broker.get_account" in str(exc)
    else:
        raise AssertionError("expected timeout")


def test_bootstrap_honors_configured_logs_dir(temp_config_dir: Path) -> None:
    app_config_path = temp_config_dir / "app.yaml"
    payload = yaml.safe_load(app_config_path.read_text(encoding="utf-8"))
    custom_logs_dir = temp_config_dir.parent / "isolated_logs"
    payload.setdefault("app", {})["logs_dir"] = str(custom_logs_dir)
    app_config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with bootstrap(str(app_config_path)) as context:
        assert context.config.app.logs_dir == str(custom_logs_dir)
    assert (custom_logs_dir / "app.log").exists()
