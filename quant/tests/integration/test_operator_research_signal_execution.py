from __future__ import annotations

from pathlib import Path

import yaml

from a_share_quant.app.bootstrap import bootstrap_trade_operator_context


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
            "fill_price": fill_price,
            "fill_quantity": order.quantity,
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


def test_operator_workflow_can_submit_research_signal_snapshot(tmp_path: Path) -> None:
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
        operator_workflow = context.require_workflow_registry().get("workflow.operator_trade")
        result = operator_workflow.submit_research_signal(
            research_run_id=signal_payload["research_run_id"],
            command_source="tests.integration.operator_research_signal",
            requested_by="tester",
            approved=True,
            account_id="acct-demo",
        )

        assert result.plan.intent.source_run_id == signal_payload["research_run_id"]
        assert result.plan.intent.account_id == "acct-demo"
        assert result.plan.orders
        assert result.trade_session.summary.account_id == "acct-demo"
        assert result.trade_session.summary.submitted_count == len(result.plan.orders)
        assert result.trade_session.summary.command_type == "submit_execution_intent"
        assert {event.event_type for event in result.trade_session.events} >= {"SESSION_CREATED", "BROKER_SUBMISSION_STARTED"}
