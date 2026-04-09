from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_data_context, bootstrap_storage_context


def _write_operator_config(temp_dir: Path) -> Path:
    payload = yaml.safe_load(Path("configs/operator_paper_trade.yaml").read_text(encoding="utf-8"))
    payload.pop("extends", None)
    payload.setdefault("app", {})["name"] = "AShareQuantWorkstation"
    payload.setdefault("app", {})["environment"] = "local"
    payload.setdefault("app", {})["timezone"] = "Asia/Shanghai"
    payload.setdefault("app", {})["path_resolution_mode"] = "config_dir"
    payload.setdefault("app", {})["runtime_mode"] = "paper_trade"
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
    payload.setdefault("operator", {})["supervisor_idle_timeout_seconds"] = 0.2
    payload.setdefault("operator", {})["supervisor_scan_interval_seconds"] = 0.05
    payload.setdefault("broker", {})["event_source_mode"] = "subscribe"
    runtime_dir = temp_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload.setdefault("app", {})["logs_dir"] = str(runtime_dir / "logs")
    payload.setdefault("data", {})["storage_dir"] = str(runtime_dir / "data")
    payload.setdefault("data", {})["reports_dir"] = str(runtime_dir / "reports")
    payload.setdefault("database", {})["path"] = str(runtime_dir / "a_share_quant.db")
    config_path = temp_dir / "operator_paper_trade.yaml"
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_operator_supervisor_script_runs_subscription_path_and_completes_session(tmp_path: Path) -> None:
    config_path = _write_operator_config(tmp_path)
    with bootstrap_data_context(str(config_path)) as context:
        context.require_data_service().import_csv("sample_data/daily_bars.csv", encoding=context.config.data.default_csv_encoding)

    client_module = tmp_path / "demo_supervisor_broker_factory.py"
    client_module.write_text(
        """
from datetime import date

from a_share_quant.adapters.broker.base import ExecutionReportSubscription
from a_share_quant.domain.models import AccountSnapshot, ExecutionReport, Fill, LiveOrderSubmission, OrderSide, OrderStatus, OrderTicket


class DemoBroker:
    def __init__(self):
        self._account_id = None
        self._broker_order_ids = []

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
        raise AssertionError("submit_order should not be used")

    def submit_order_lifecycle(self, order, fill_price, trade_date):
        self._account_id = order.account_id
        broker_order_id = "script_push_broker_order_1"
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=OrderStatus.ACCEPTED,
            broker_order_id=broker_order_id,
            filled_quantity=0,
        )
        report = ExecutionReport(
            report_id="script_push_accept_1",
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="accepted for supervisor",
            broker_order_id=broker_order_id,
            account_id=order.account_id,
        )
        return LiveOrderSubmission(ticket=ticket, reports=[report], fills=[])

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return []

    def query_trades(self):
        broker_order_id = self._broker_order_ids[0] if self._broker_order_ids else "script_push_broker_order_1"
        return [
            Fill(
                fill_id="script_push_fill_1",
                order_id=broker_order_id,
                trade_date=date(2026, 1, 5),
                ts_code="600000.SH",
                side=OrderSide.BUY,
                fill_price=10.5,
                fill_quantity=100,
                fee=0.0,
                tax=0.0,
                broker_order_id=broker_order_id,
                account_id=self._account_id,
            )
        ]

    def supports_execution_report_subscription(self):
        return True

    def subscribe_execution_reports(self, handler, *, account_id=None, broker_order_ids=None, cursor=None):
        self._account_id = account_id or self._account_id
        self._broker_order_ids = list(broker_order_ids or ["script_push_broker_order_1"])
        handler(
            [
                ExecutionReport(
                    report_id="script_push_fill_report_1",
                    order_id=self._broker_order_ids[0],
                    trade_date=date(2026, 1, 5),
                    status=OrderStatus.FILLED,
                    requested_quantity=100,
                    filled_quantity=100,
                    remaining_quantity=0,
                    message="filled by supervisor subscription",
                    fill_price=10.5,
                    broker_order_id=self._broker_order_ids[0],
                    account_id=self._account_id,
                    metadata={"cursor": "script_cursor_1", "source": "push_subscription"},
                )
            ],
            "script_cursor_1",
        )
        return ExecutionReportSubscription()


def create_client():
    return DemoBroker()
""".strip(),
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmp_path)
    repo_root = Path(__file__).resolve().parents[2]

    submit = subprocess.run(
        [
            sys.executable,
            "scripts/operator_submit_order.py",
            "--config",
            str(config_path),
            "--broker-client-factory",
            "demo_supervisor_broker_factory:create_client",
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
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    submit_payload = json.loads(submit.stdout)
    assert submit_payload["session"]["status"] == "RECOVERY_REQUIRED"
    session_id = submit_payload["session"]["session_id"]

    supervised = subprocess.run(
        [
            sys.executable,
            "scripts/operator_run_supervisor.py",
            "--config",
            str(config_path),
            "--broker-client-factory",
            "demo_supervisor_broker_factory:create_client",
            "--session-id",
            session_id,
            "--max-loops",
            "1",
            "--stop-when-idle",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    supervisor_payload = json.loads(supervised.stdout)
    assert supervisor_payload["claimed_session_ids"] == [session_id]
    assert supervisor_payload["fallback_polled_session_ids"] == []
    assert supervisor_payload["completed_session_ids"] == [session_id]

    with bootstrap_storage_context(str(config_path)) as context:
        latest = context.execution_session_repository.get(session_id)
        assert latest is not None
        assert latest.status.value == "COMPLETED"
        assert latest.broker_event_cursor == "script_cursor_1"
        fills = context.order_repository.list_fills(execution_session_id=session_id, limit=10)
        assert len(fills) == 1
        assert fills[0]["fill_id"] == "script_push_fill_1"
