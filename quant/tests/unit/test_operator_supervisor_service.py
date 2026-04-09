from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import AccountSnapshot, Bar, ExecutionReport, Fill, LiveOrderSubmission, OrderRequest, OrderSide, OrderStatus, OrderTicket, PositionSnapshot, Security, TradeSessionStatus
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_supervisor_service import OperatorSupervisorService
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService
from a_share_quant.storage.sqlite_store import SQLiteStore


class _PushSubscriptionHandle:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _PushBroker:
    def __init__(self) -> None:
        self._account_id: str | None = None
        self._broker_order_ids: list[str] = []

    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self) -> bool:
        return True

    def get_account(self, last_prices=None):
        return AccountSnapshot(cash=100000.0, available_cash=100000.0, market_value=0.0, total_assets=100000.0, pnl=0.0)

    def get_positions(self, last_prices=None):
        return []

    def submit_order(self, order, fill_price, trade_date):  # pragma: no cover
        raise AssertionError("submit_order should not be used for push broker")

    def submit_order_lifecycle(self, order, fill_price, trade_date):
        self._account_id = order.account_id
        broker_order_id = "push_broker_order_1"
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=OrderStatus.ACCEPTED,
            broker_order_id=broker_order_id,
            filled_quantity=0,
        )
        report = ExecutionReport(
            report_id="push_accept_report_1",
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="accepted for subscription follow-up",
            broker_order_id=broker_order_id,
            account_id=order.account_id,
        )
        return LiveOrderSubmission(ticket=ticket, reports=[report], fills=[])

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return []

    def query_trades(self):
        broker_order_id = self._broker_order_ids[0] if self._broker_order_ids else "push_broker_order_1"
        return [
            Fill(
                fill_id="push_fill_1",
                order_id=broker_order_id,
                trade_date=date(2024, 1, 2),
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

    def supports_execution_report_subscription(self) -> bool:
        return True

    def subscribe_execution_reports(self, handler, *, account_id=None, broker_order_ids=None, cursor=None):
        self._account_id = account_id or self._account_id
        self._broker_order_ids = list(broker_order_ids or ["push_broker_order_1"])
        handler(
            [
                ExecutionReport(
                    report_id="push_fill_report_1",
                    order_id=self._broker_order_ids[0],
                    trade_date=date(2024, 1, 2),
                    status=OrderStatus.FILLED,
                    requested_quantity=100,
                    filled_quantity=100,
                    remaining_quantity=0,
                    message="filled by subscription",
                    fill_price=10.5,
                    broker_order_id=self._broker_order_ids[0],
                    account_id=self._account_id,
                    metadata={"cursor": "push_cursor_1", "source": "push_subscription"},
                )
            ],
            "push_cursor_1",
        )
        return _PushSubscriptionHandle()


def _build_config(tmp_path: Path):
    payload = {
        "app": {
            "name": "AShareQuantWorkstation",
            "environment": "local",
            "timezone": "Asia/Shanghai",
            "path_resolution_mode": "config_dir",
            "runtime_mode": "paper_trade",
            "logs_dir": str(tmp_path / "runtime" / "logs"),
        },
        "data": {
            "storage_dir": str(tmp_path / "runtime" / "data"),
            "reports_dir": str(tmp_path / "runtime" / "reports"),
            "provider": "csv",
            "default_exchange": "SSE",
            "default_csv_encoding": "utf-8",
            "calendar_policy": "demo",
        },
        "database": {"path": str(tmp_path / "test.db")},
        "risk": {"max_position_weight": 0.6, "max_order_value": 600000.0, "blocked_symbols": [], "kill_switch": False},
        "strategy": {"strategy_id": "momentum_top_n", "version": "1.0.0", "params": {}},
        "backtest": {"initial_cash": 1000000.0, "fee_bps": 3.0, "tax_bps": 10.0, "slippage_bps": 5.0, "benchmark_symbol": "600000.SH"},
        "broker": {"provider": "qmt", "account_id": "demo-account", "allowed_account_ids": ["demo-account"], "event_source_mode": "subscribe"},
        "operator": {"require_approval": False, "max_batch_orders": 20, "default_requested_by": "operator", "fail_fast": False, "supervisor_idle_timeout_seconds": 0.2, "supervisor_scan_interval_seconds": 0.05},
        "plugins": {"enabled_builtin": [], "disabled": [], "external": []},
    }
    cfg = tmp_path / "app.yaml"
    cfg.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return ConfigLoader.load(str(cfg))


def _build_supervisor_stack(tmp_path: Path):
    config = _build_config(tmp_path)
    store = SQLiteStore(config.database.path)
    store.init_schema(load_schema_sql())
    market_repository = MarketRepository(store)
    market_repository.upsert_securities(
        {
            "600000.SH": Security(
                ts_code="600000.SH",
                name="PF Bank",
                exchange="SSE",
                board="MAIN",
                is_st=False,
                status="L",
                list_date=None,
                delist_date=None,
            )
        }
    )
    market_repository.upsert_bars(
        [
            Bar(
                ts_code="600000.SH",
                trade_date=date(2024, 1, 2),
                open=10.0,
                high=10.8,
                low=9.8,
                close=10.5,
                volume=1000.0,
                amount=10500.0,
                pre_close=10.0,
                suspended=False,
                limit_up=False,
                limit_down=False,
            )
        ]
    )
    order_repository = OrderRepository(store)
    execution_session_repository = ExecutionSessionRepository(store)
    broker = _PushBroker()
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    orchestrator = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    supervisor = OperatorSupervisorService(
        config=config,
        broker=broker,
        orchestrator=orchestrator,
        execution_session_repository=execution_session_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
    )
    return orchestrator, supervisor, order_repository, execution_session_repository


def test_operator_supervisor_consumes_subscription_and_completes_session(tmp_path: Path) -> None:
    orchestrator, supervisor, order_repository, execution_session_repository = _build_supervisor_stack(tmp_path)
    order = OrderRequest(
        order_id="order_push_demo_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="push-subscription-test",
    )

    submit_result = orchestrator.submit_orders(
        [order],
        command_source="test",
        requested_by="tester",
        account_id="demo-account",
    )
    assert submit_result.summary.status == TradeSessionStatus.RECOVERY_REQUIRED

    summary = supervisor.run_once(requested_by="tester", session_id=submit_result.summary.session_id, owner_id="supervisor-test")
    assert summary.claimed_session_ids == [submit_result.summary.session_id]
    assert summary.fallback_polled_session_ids == []
    assert summary.completed_session_ids == [submit_result.summary.session_id]

    latest_session = execution_session_repository.get(submit_result.summary.session_id)
    assert latest_session is not None
    assert latest_session.status == TradeSessionStatus.COMPLETED
    assert latest_session.broker_event_cursor == "push_cursor_1"
    assert latest_session.supervisor_mode == "subscribe"
    assert latest_session.supervisor_owner is None

    fills = order_repository.list_fills(execution_session_id=submit_result.summary.session_id, limit=10)
    assert len(fills) == 1
    assert fills[0]["fill_id"] == "push_fill_1"

    event_types = [event.event_type for event in execution_session_repository.list_events(submit_result.summary.session_id, limit=50)]
    assert "SUPERVISOR_SUBSCRIPTION_STARTED" in event_types
    assert "SUPERVISOR_RELEASED" in event_types
    assert "ORDER_FILLED" in event_types


def test_operator_supervisor_skips_partially_completed_sessions(tmp_path: Path) -> None:
    orchestrator, supervisor, _order_repository, execution_session_repository = _build_supervisor_stack(tmp_path)

    partial = execution_session_repository.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2024-01-02",
        idempotency_key="partial-terminal",
        status=TradeSessionStatus.PARTIALLY_COMPLETED,
        account_id="demo-account",
    )

    summary = supervisor.run_once(requested_by="tester")
    assert partial.session_id not in summary.claimed_session_ids
    assert summary.claimed_session_ids == []
    events = execution_session_repository.list_events(partial.session_id)
    assert events == []


class _IdleThenPollBroker(_PushBroker):
    def __init__(self) -> None:
        super().__init__()
        self._synced = False

    def subscribe_execution_reports(self, handler, *, account_id=None, broker_order_ids=None, cursor=None):
        self._account_id = account_id or self._account_id
        self._broker_order_ids = list(broker_order_ids or ["push_broker_order_1"])
        return _PushSubscriptionHandle()

    def poll_execution_reports(self, *, account_id=None, broker_order_ids=None):
        if self._synced:
            return []
        self._synced = True
        broker_order_id = (broker_order_ids or ["push_broker_order_1"])[0]
        self._broker_order_ids = [broker_order_id]
        return [
            ExecutionReport(
                report_id="idle_poll_fill_report_1",
                order_id=broker_order_id,
                trade_date=date(2024, 1, 2),
                status=OrderStatus.FILLED,
                requested_quantity=100,
                filled_quantity=100,
                remaining_quantity=0,
                message="filled after idle fallback",
                fill_price=10.5,
                broker_order_id=broker_order_id,
                account_id=account_id or self._account_id,
                metadata={"cursor": "idle_poll_cursor_1"},
            )
        ]


def _build_supervisor_stack_with_broker(tmp_path: Path, broker):
    config = _build_config(tmp_path)
    config.operator.supervisor_idle_timeout_seconds = 0.15
    config.operator.supervisor_scan_interval_seconds = 0.02
    config.operator.supervisor_lease_seconds = 0.12
    config.operator.supervisor_heartbeat_interval_seconds = 0.04
    store = SQLiteStore(config.database.path)
    store.init_schema(load_schema_sql())
    market_repository = MarketRepository(store)
    market_repository.upsert_securities(
        {
            "600000.SH": Security(
                ts_code="600000.SH",
                name="PF Bank",
                exchange="SSE",
                board="MAIN",
                is_st=False,
                status="L",
                list_date=None,
                delist_date=None,
            )
        }
    )
    market_repository.upsert_bars(
        [
            Bar(
                ts_code="600000.SH",
                trade_date=date(2024, 1, 2),
                open=10.0,
                high=10.8,
                low=9.8,
                close=10.5,
                volume=1000.0,
                amount=10500.0,
                pre_close=10.0,
                suspended=False,
                limit_up=False,
                limit_down=False,
            )
        ]
    )
    order_repository = OrderRepository(store)
    execution_session_repository = ExecutionSessionRepository(store)
    audit_repository = AuditRepository(store)
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=audit_repository,
        execution_session_repository=execution_session_repository,
    )
    orchestrator = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=audit_repository,
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    supervisor = OperatorSupervisorService(
        config=config,
        broker=broker,
        orchestrator=orchestrator,
        execution_session_repository=execution_session_repository,
        order_repository=order_repository,
        audit_repository=audit_repository,
    )
    return orchestrator, supervisor, order_repository, execution_session_repository


def test_operator_supervisor_renews_lease_before_idle_fallback_poll(tmp_path: Path) -> None:
    orchestrator, supervisor, _order_repository, execution_session_repository = _build_supervisor_stack_with_broker(
        tmp_path,
        _IdleThenPollBroker(),
    )
    order = OrderRequest(
        order_id="order_idle_demo_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="idle-fallback-test",
    )
    submit_result = orchestrator.submit_orders(
        [order],
        command_source="test",
        requested_by="tester",
        account_id="demo-account",
    )
    assert submit_result.summary.status == TradeSessionStatus.RECOVERY_REQUIRED

    summary = supervisor.run_once(requested_by="tester", session_id=submit_result.summary.session_id, owner_id="supervisor-renew")
    assert summary.completed_session_ids == [submit_result.summary.session_id]
    assert summary.fallback_polled_session_ids == [submit_result.summary.session_id]

    event_types = [event.event_type for event in execution_session_repository.list_events(submit_result.summary.session_id, limit=100)]
    assert "SUPERVISOR_RENEWED" in event_types
    assert "SUPERVISOR_SUBSCRIPTION_IDLE_TIMEOUT" in event_types
