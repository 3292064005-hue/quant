from __future__ import annotations

from pathlib import Path

import yaml

from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.core.broker_client_loader import load_broker_client
from a_share_quant.core.runtime_checks import check_broker_runtime, check_data_provider_runtime, check_ui_runtime


class _BrokerClient:
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


class _BrokenBrokerClient:
    def get_account(self, last_prices):
        return None


class _BadSignatureBrokerClient(_BrokerClient):
    def submit_order(self, order):  # type: ignore[override]
        return None


def test_check_ui_runtime_reports_missing_dependency() -> None:
    result = check_ui_runtime()
    assert result.ok is False
    assert "PySide6" in result.message


def test_check_data_provider_runtime_requires_tushare_dependency_or_token() -> None:
    result = check_data_provider_runtime("tushare", token_present=False)
    assert result.ok is False
    assert ("tushare" in result.message) or ("token" in result.message)


def test_check_broker_runtime_requires_injected_client() -> None:
    result = check_broker_runtime("qmt", endpoint="tcp://127.0.0.1:1234", account_id="demo", injected_client=None)
    assert result.ok is False
    assert "注入客户端" in result.message


def test_check_broker_runtime_supports_shallow_validation_for_cli() -> None:
    result = check_broker_runtime(
        "qmt",
        endpoint="tcp://127.0.0.1:1234",
        account_id="demo",
        injected_client=None,
        allow_shallow_client_check=True,
    )
    assert result.ok is True
    assert result.details["mode"] == "shallow"


def test_qmt_adapter_rejects_client_with_missing_methods() -> None:
    try:
        QMTAdapter(_BrokenBrokerClient())
    except ValueError as exc:
        assert "缺少必要方法" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_ptrade_adapter_accepts_valid_client_contract() -> None:
    adapter = PTradeAdapter(_BrokerClient())
    assert adapter.heartbeat() is True


def test_check_runtime_script_reports_configured_ui_and_provider_state(temp_config_dir: Path, monkeypatch) -> None:
    app_config_path = temp_config_dir / "app.yaml"
    data_config_path = temp_config_dir / "data.yaml"
    payload = yaml.safe_load(data_config_path.read_text(encoding="utf-8"))
    payload.setdefault("data", {})["provider"] = "akshare"
    data_config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    monkeypatch.chdir(temp_config_dir)
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_runtime.py"
    namespace: dict[str, object] = {"__file__": str(script_path)}
    exec(compile(script_path.read_text(encoding="utf-8"), str(script_path), "exec"), namespace)
    main = namespace["main"]

    import sys

    original_argv = sys.argv[:]
    try:
        sys.argv = [str(script_path), "--config", str(app_config_path), "--check-ui", "--strict"]
        exit_code = main()
    finally:
        sys.argv = original_argv
    assert exit_code == 2


def test_check_broker_runtime_rejects_incompatible_method_signature() -> None:
    result = check_broker_runtime(
        "qmt",
        endpoint="tcp://127.0.0.1:1234",
        account_id="demo",
        injected_client=_BadSignatureBrokerClient(),
    )
    assert result.ok is False
    assert "方法签名" in result.message


def test_check_broker_runtime_validates_sample_payload_mapping() -> None:
    result = check_broker_runtime(
        "ptrade",
        endpoint="tcp://127.0.0.1:1234",
        account_id="demo",
        injected_client=_BrokerClient(),
        sample_payloads={"account": {"cash": "bad-number"}},
    )
    assert result.ok is False
    assert "样本载荷" in result.message


def test_check_broker_runtime_lenient_mode_accepts_best_effort_payloads() -> None:
    result = check_broker_runtime(
        "qmt",
        endpoint="tcp://127.0.0.1:1234",
        account_id="demo",
        injected_client=None,
        allow_shallow_client_check=True,
        strict_contract_mapping=False,
        sample_payloads={
            "account": {"available": 120.0, "assets": 130.0},
            "positions": [{"symbol": "600000.SH", "qty": "bad-int", "available": 50}],
            "fill": {"symbol": "600000.SH", "side": "BUY", "fill_price": 10.2},
        },
    )
    assert result.ok is True
    assert result.details["mapping_mode"] == "lenient"


def test_check_broker_runtime_strict_mode_rejects_same_payloads() -> None:
    result = check_broker_runtime(
        "qmt",
        endpoint="tcp://127.0.0.1:1234",
        account_id="demo",
        injected_client=None,
        allow_shallow_client_check=True,
        strict_contract_mapping=True,
        sample_payloads={
            "account": {"available": 120.0, "assets": 130.0},
            "positions": [{"symbol": "600000.SH", "qty": "bad-int", "available": 50}],
        },
    )
    assert result.ok is False
    assert result.details["mapping_mode"] == "strict"


def test_load_broker_client_supports_configured_factory(temp_config_dir: Path, tmp_path: Path, monkeypatch) -> None:
    module_path = tmp_path / "broker_factory_module.py"
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
    payload.setdefault("broker", {})["provider"] = "qmt"
    payload["broker"]["endpoint"] = "tcp://127.0.0.1:1234"
    payload["broker"]["account_id"] = "demo"
    payload["broker"]["client_factory"] = "broker_factory_module:build_client"
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
                "client_factory": "broker_factory_module:build_client",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    from a_share_quant.config.loader import ConfigLoader

    config = ConfigLoader.load(app_path)
    client = load_broker_client(config)
    assert client is not None
    assert client.heartbeat() is True


def test_check_broker_runtime_rejects_invalid_runtime_mode_provider_combo() -> None:
    result = check_broker_runtime("mock", runtime_mode="live_trade")
    assert result.ok is False
    assert "不允许使用 mock broker" in result.message

