from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.cli import main_daily_run, main_operator_snapshot, main_research


def _write_config(temp_dir: Path, *, runtime_mode: str = "research_backtest", provider: str = "mock") -> Path:
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
    config_path = temp_dir / "app.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path




class _StubLiveBroker:
    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def heartbeat(self) -> bool:
        return True

    def get_account(self, last_prices=None):
        from a_share_quant.domain.models import AccountSnapshot

        return AccountSnapshot(cash=90000.0, available_cash=90000.0, market_value=10000.0, total_assets=100000.0, pnl=0.0)

    def get_positions(self, last_prices=None):
        from a_share_quant.domain.models import PositionSnapshot

        return [PositionSnapshot(ts_code="600000.SH", quantity=100, available_quantity=100, avg_cost=10.0, market_value=1000.0, unrealized_pnl=0.0)]

    def submit_order(self, order, fill_price, trade_date):  # pragma: no cover - 只读入口不应调用
        raise AssertionError("只读 operator snapshot 不应触发 submit_order")

    def cancel_order(self, broker_order_id):  # pragma: no cover - 只读入口不应调用
        raise AssertionError("只读 operator snapshot 不应触发 cancel_order")

    def query_orders(self):
        return []

    def query_trades(self):
        return []


class _ScopedLiveBroker(_StubLiveBroker):
    def get_account(self, last_prices=None, account_id=None):
        from a_share_quant.domain.models import AccountSnapshot

        if account_id == "acct-b":
            return AccountSnapshot(cash=50000.0, available_cash=50000.0, market_value=5000.0, total_assets=55000.0, pnl=1000.0)
        return AccountSnapshot(cash=90000.0, available_cash=90000.0, market_value=10000.0, total_assets=100000.0, pnl=0.0)

    def get_positions(self, last_prices=None, account_id=None):
        from a_share_quant.domain.models import PositionSnapshot

        if account_id == "acct-b":
            return [PositionSnapshot(ts_code="000001.SZ", quantity=200, available_quantity=200, avg_cost=8.0, market_value=1800.0, unrealized_pnl=200.0)]
        return [PositionSnapshot(ts_code="600000.SH", quantity=100, available_quantity=100, avg_cost=10.0, market_value=1000.0, unrealized_pnl=0.0)]

    def query_orders(self, account_id=None):
        return []

    def query_trades(self, account_id=None):
        return []


def test_python_module_entrypoint_runs(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    completed = subprocess.run(
        [sys.executable, "-m", "a_share_quant", "--config", str(config_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = ast.literal_eval(completed.stdout.strip())
    assert payload["run_id"].startswith("run_")
    assert payload["data_lineage"]["import_run_id"] is not None


def test_daily_run_uses_existing_data_without_creating_new_import_run(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        data_service = context.require_data_service()
        data_service.import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)
        existing_import_run_id = data_service.last_import_run_id

    exit_code = main_daily_run(["--config", str(config_path)])
    assert exit_code == 0

    with bootstrap_data_context(str(config_path)) as context:
        latest_import = context.data_import_repository.get_latest_run()
    assert latest_import is not None
    assert latest_import.import_run_id == existing_import_run_id


def test_provider_bar_stream_daily_returns_trade_date_batches(tmp_path: Path) -> None:
    with bootstrap_data_context(str(_write_config(tmp_path / "cfg"))) as context:
        bundle = context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)
        del bundle
    # 直接读取仓内 DB 以验证 provider 契约
    with bootstrap_data_context(str(_write_config(tmp_path / "cfg2"))) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)
        provider = context.require_provider_registry().get("provider.bar")
        batches = list(provider.stream_daily())
    assert batches
    assert batches[0][1]


def test_operator_snapshot_is_read_only_and_uses_operator_lane(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="qmt")

    def _fake_loader(config, factory_path_override=None, provider=None):
        return _StubLiveBroker()

    monkeypatch.setattr("a_share_quant.cli.load_broker_client", _fake_loader)
    monkeypatch.setattr("a_share_quant.app.assembly_broker.load_broker_client", _fake_loader)
    exit_code = main_operator_snapshot(["--config", str(config_path), "--broker-client-factory", "demo.module:create"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["runtime_mode"] == "paper_trade"
    assert payload["orders"] == []
    assert payload["fills"] == []
    assert payload["account"]["total_assets"] == 100000.0


def test_operator_snapshot_script_prefers_local_source_tree(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="qmt")
    client_module = tmp_path / "demo_broker_factory.py"
    client_module.write_text(
        """
from a_share_quant.domain.models import AccountSnapshot, PositionSnapshot


class DemoBroker:
    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self):
        return True

    def get_account(self, last_prices=None):
        return AccountSnapshot(cash=1000.0, available_cash=1000.0, market_value=0.0, total_assets=1000.0, pnl=0.0)

    def get_positions(self, last_prices=None):
        return [PositionSnapshot(ts_code="600000.SH", quantity=100, available_quantity=100, avg_cost=10.0, market_value=1000.0, unrealized_pnl=0.0)]

    def submit_order(self, order, fill_price, trade_date):
        raise RuntimeError("read-only")

    def cancel_order(self, broker_order_id):
        raise RuntimeError("read-only")

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
            "scripts/operator_snapshot.py",
            "--config",
            str(config_path),
            "--broker-client-factory",
            "demo_broker_factory:create_client",
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(completed.stdout)
    assert payload["runtime_mode"] == "paper_trade"
    assert payload["account"]["total_assets"] == 1000.0


def test_research_batch_spec_runs_and_persists_batch_summary(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)
    batch_spec = tmp_path / "batch.json"
    batch_spec.write_text(
        json.dumps(
            {
                "tasks": [
                    {"task_name": "lb3", "feature_name": "momentum", "lookback": 3, "top_n": 2},
                    {"task_name": "lb5", "feature_name": "momentum", "lookback": 5, "top_n": 3},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    exit_code = main_research(
        [
            "--config",
            str(config_path),
            "--csv",
            "sample_data/daily_bars.csv",
            "--artifact",
            "experiment-batch",
            "--batch-spec",
            str(batch_spec),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["aggregate"]["task_count"] == 2
    assert len(payload["tasks"]) == 2
    assert payload["aggregate"]["signal_snapshot_run_ids"]

    with bootstrap_data_context(str(config_path)) as context:
        recent = context.research_run_repository.list_recent(limit=10)
        assert [item["artifact_type"] for item in recent] == ["experiment_batch_summary"]


def test_operator_snapshot_exposes_account_views_for_allowed_accounts(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = _write_config(tmp_path, runtime_mode="paper_trade", provider="qmt")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload.setdefault("broker", {})["allowed_account_ids"] = ["demo-account", "acct-b"]
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def _fake_loader(config, factory_path_override=None, provider=None):
        return _ScopedLiveBroker()

    monkeypatch.setattr("a_share_quant.cli.load_broker_client", _fake_loader)
    monkeypatch.setattr("a_share_quant.app.assembly_broker.load_broker_client", _fake_loader)
    exit_code = main_operator_snapshot(["--config", str(config_path), "--broker-client-factory", "demo.module:create"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["default_account_id"] == "demo-account"
    assert len(payload["account_views"]) == 2
    views = {item["account_id"]: item for item in payload["account_views"]}
    assert views["demo-account"]["account"]["total_assets"] == 100000.0
    assert views["acct-b"]["account"]["total_assets"] == 55000.0
    assert views["acct-b"]["positions"][0]["ts_code"] == "000001.SZ"
