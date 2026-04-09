from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import AccountSnapshot, Bar, ExecutionReport, Fill, LiveOrderSubmission, OrderRequest, OrderSide, OrderStatus, OrderTicket, PositionSnapshot, Security, TradeSessionStatus
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.trade_orchestrator_service import TradeOrchestratorService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService
from a_share_quant.storage.sqlite_store import SQLiteStore


class _DemoBroker:
    def __init__(self) -> None:
        self.orders: list[OrderRequest] = []
        self.fills: list[Fill] = []

    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self) -> bool:
        return True

    def get_account(self, last_prices=None):
        return AccountSnapshot(cash=100000.0, available_cash=100000.0, market_value=0.0, total_assets=100000.0, pnl=0.0)

    def get_positions(self, last_prices=None):
        return [PositionSnapshot(ts_code="600000.SH", quantity=100, available_quantity=100, avg_cost=10.0, market_value=1000.0, unrealized_pnl=0.0)]

    def submit_order(self, order, fill_price, trade_date):
        order.broker_order_id = "broker_order_demo_1"
        cloned = OrderRequest(
            order_id=order.order_id,
            trade_date=order.trade_date,
            strategy_id=order.strategy_id,
            ts_code=order.ts_code,
            side=order.side,
            price=order.price,
            quantity=order.quantity,
            reason=order.reason,
            status=order.status,
            broker_order_id=order.broker_order_id,
            filled_quantity=order.filled_quantity,
            avg_fill_price=order.avg_fill_price,
            last_error=order.last_error,
        )
        self.orders.append(cloned)
        fill = Fill(
            fill_id="fill_demo_1",
            order_id=order.order_id,
            trade_date=trade_date,
            ts_code=order.ts_code,
            side=order.side,
            fill_price=float(fill_price),
            fill_quantity=int(order.quantity),
            fee=0.0,
            tax=0.0,
        )
        self.fills.append(fill)
        return fill

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return list(self.orders)

    def query_trades(self):
        return list(self.fills)


class _ExternalIdClient:
    def connect(self):
        return None

    def close(self):
        return None

    def heartbeat(self):
        return True

    def get_account(self, last_prices=None):
        return {"cash": 100000.0, "available_cash": 100000.0, "market_value": 0.0, "total_assets": 100000.0, "pnl": 0.0}

    def get_positions(self, last_prices=None):
        return [{"symbol": "600000.SH", "qty": 100, "available": 100, "cost_price": 10.0, "market_value": 1000.0, "profit": 0.0}]

    def submit_order(self, order, fill_price, trade_date):
        return {
            "fill_id": "fill_demo_ext_1",
            "broker_order_id": "broker_order_demo_ext_1",
            "trade_date": trade_date.isoformat(),
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
        return [
            {
                "broker_order_id": "broker_order_demo_ext_1",
                "trade_date": "2024-01-02",
                "symbol": "600000.SH",
                "side": "BUY",
                "price": 10.5,
                "quantity": 100,
                "reason": "external",
                "status": "FILLED",
            }
        ]

    def query_trades(self):
        return [
            {
                "fill_id": "fill_demo_ext_1",
                "broker_order_id": "broker_order_demo_ext_1",
                "trade_date": "2024-01-02",
                "symbol": "600000.SH",
                "side": "BUY",
                "fill_price": 10.5,
                "fill_quantity": 100,
                "fee": 0.0,
                "tax": 0.0,
            }
        ]


class _AcceptedOnlyBroker:
    def __init__(self) -> None:
        self.orders: list[OrderRequest] = []

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

    def submit_order(self, order, fill_price, trade_date):  # pragma: no cover - should use lifecycle path
        raise AssertionError("submit_order should not be called for accepted-only lifecycle broker")

    def submit_order_lifecycle(self, order, fill_price, trade_date):
        order.broker_order_id = "broker_order_pending_1"
        self.orders.append(order)
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=OrderStatus.ACCEPTED,
            broker_order_id="broker_order_pending_1",
            filled_quantity=0,
        )
        report = ExecutionReport(
            report_id="report_pending_1",
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="accepted without immediate fill",
            broker_order_id="broker_order_pending_1",
        )
        return LiveOrderSubmission(ticket=ticket, reports=[report], fills=[])

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        return list(self.orders)

    def query_trades(self):
        return []


class _PollingBroker:
    def __init__(self) -> None:
        self.orders: list[OrderRequest] = []
        self.fills: list[Fill] = []
        self._account_id: str | None = None
        self._synced = False

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

    def submit_order(self, order, fill_price, trade_date):  # pragma: no cover - should use lifecycle path
        raise AssertionError("submit_order should not be called for polling lifecycle broker")

    def submit_order_lifecycle(self, order, fill_price, trade_date):
        self._account_id = order.account_id
        order.broker_order_id = "broker_order_sync_1"
        accepted = OrderRequest(
            order_id=order.order_id,
            trade_date=order.trade_date,
            strategy_id=order.strategy_id,
            ts_code=order.ts_code,
            side=order.side,
            price=order.price,
            quantity=order.quantity,
            reason=order.reason,
            status=OrderStatus.ACCEPTED,
            broker_order_id=order.broker_order_id,
            filled_quantity=0,
            avg_fill_price=None,
            last_error=None,
            account_id=order.account_id,
        )
        self.orders = [accepted]
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=OrderStatus.ACCEPTED,
            broker_order_id=order.broker_order_id,
            filled_quantity=0,
        )
        accepted_report = ExecutionReport(
            report_id="report_sync_accept_1",
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="accepted without immediate fill",
            broker_order_id=order.broker_order_id,
            account_id=order.account_id,
        )
        return LiveOrderSubmission(ticket=ticket, reports=[accepted_report], fills=[])

    def poll_execution_reports(self, *, account_id=None, broker_order_ids=None):
        if self._synced:
            return []
        self._synced = True
        matched_broker_order_id = (broker_order_ids or ["broker_order_sync_1"])[0]
        self.fills = [
            Fill(
                fill_id="fill_sync_1",
                order_id=matched_broker_order_id,
                trade_date=date(2024, 1, 2),
                ts_code="600000.SH",
                side=OrderSide.BUY,
                fill_price=10.5,
                fill_quantity=100,
                fee=0.0,
                tax=0.0,
                broker_order_id=matched_broker_order_id,
                account_id=account_id or self._account_id,
            )
        ]
        return [
            ExecutionReport(
                report_id="report_sync_fill_1",
                order_id="broker_order_sync_1",
                trade_date=date(2024, 1, 2),
                status=OrderStatus.FILLED,
                requested_quantity=100,
                filled_quantity=100,
                remaining_quantity=0,
                message="filled by poll",
                fill_price=10.5,
                broker_order_id=matched_broker_order_id,
                account_id=account_id or self._account_id,
            )
        ]

    def cancel_order(self, broker_order_id):
        return True

    def query_orders(self):
        if self.orders and self._synced:
            self.orders[0].status = OrderStatus.FILLED
            self.orders[0].filled_quantity = 100
            self.orders[0].avg_fill_price = 10.5
        return list(self.orders)

    def query_trades(self):
        return list(self.fills)


def _build_service_with_external_id_broker(tmp_path: Path) -> tuple[TradeOrchestratorService, OrderRepository, ExecutionSessionRepository]:
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
    broker = QMTAdapter(_ExternalIdClient(), strict_contract_mapping=True)
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    service = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    return service, order_repository, execution_session_repository


def _build_service_with_accepted_only_broker(tmp_path: Path) -> tuple[TradeOrchestratorService, OrderRepository, ExecutionSessionRepository]:
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
    broker = _AcceptedOnlyBroker()
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    service = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    return service, order_repository, execution_session_repository


def _build_service_with_polling_broker(tmp_path: Path) -> tuple[TradeOrchestratorService, OrderRepository, ExecutionSessionRepository]:
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
    broker = _PollingBroker()
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    service = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    return service, order_repository, execution_session_repository


def _build_config(tmp_path: Path):
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
    payload.setdefault("database", {})["path"] = str(tmp_path / "test.db")
    payload.setdefault("data", {})["storage_dir"] = str(tmp_path / "runtime" / "data")
    payload.setdefault("data", {})["reports_dir"] = str(tmp_path / "runtime" / "reports")
    payload.setdefault("app", {})["logs_dir"] = str(tmp_path / "runtime" / "logs")
    cfg = tmp_path / "app.yaml"
    cfg.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return ConfigLoader.load(str(cfg))


def _build_service(tmp_path: Path) -> tuple[TradeOrchestratorService, OrderRepository, ExecutionSessionRepository]:
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
    broker = _DemoBroker()
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    service = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )
    return service, order_repository, execution_session_repository


def test_trade_orchestrator_marks_recovery_required_when_persistence_keeps_failing(tmp_path: Path, monkeypatch) -> None:
    service, order_repository, execution_session_repository = _build_service(tmp_path)

    def _boom(*args, **kwargs):
        raise RuntimeError("persist batch failed")

    monkeypatch.setattr(order_repository, "save_execution_batch", _boom)

    order = OrderRequest(
        order_id="order_demo_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="audit-test",
    )

    try:
        service.submit_orders([order], command_source="test", requested_by="tester")
    except RuntimeError as exc:
        assert "persist batch failed" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected RuntimeError")

    session = execution_session_repository.get_latest()
    assert session is not None
    assert session.status == TradeSessionStatus.RECOVERY_REQUIRED
    assert session.error_message == "persist batch failed"
    event_types = [event.event_type for event in execution_session_repository.list_events(session.session_id)]
    assert "SESSION_CREATED" in event_types
    assert "RECOVERY_REQUIRED" in event_types


def test_trade_orchestrator_auto_recovers_when_second_persist_succeeds(tmp_path: Path, monkeypatch) -> None:
    service, order_repository, execution_session_repository = _build_service(tmp_path)
    original = order_repository.save_execution_batch
    state = {"calls": 0}

    def _flaky(run_id, orders, fills, *, execution_session_id=None):
        state["calls"] += 1
        if state["calls"] == 1:
            raise RuntimeError("persist batch failed once")
        return original(run_id, orders, fills, execution_session_id=execution_session_id)

    monkeypatch.setattr(order_repository, "save_execution_batch", _flaky)

    order = OrderRequest(
        order_id="order_demo_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="audit-test",
    )

    result = service.submit_orders([order], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.COMPLETED
    assert result.orders[0].broker_order_id == "broker_order_demo_1"
    event_types = [event.event_type for event in execution_session_repository.list_events(result.summary.session_id)]
    assert "RECOVERY_RECONCILED" in event_types


def test_trade_orchestrator_accepts_broker_payload_with_separate_external_ids(tmp_path: Path) -> None:
    service, order_repository, execution_session_repository = _build_service_with_external_id_broker(tmp_path)

    order = OrderRequest(
        order_id="order_demo_ext_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="audit-test",
    )

    result = service.submit_orders([order], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.COMPLETED
    assert result.orders[0].order_id == "order_demo_ext_1"
    assert result.orders[0].broker_order_id == "broker_order_demo_ext_1"
    assert result.fills[0].order_id == "order_demo_ext_1"
    assert result.fills[0].broker_order_id == "broker_order_demo_ext_1"
    assert order_repository.count_orders(execution_session_id=result.summary.session_id) == 1
    assert order_repository.count_fills(execution_session_id=result.summary.session_id) == 1


def test_reconciliation_rebinds_external_broker_ids_back_to_local_orders(tmp_path: Path) -> None:
    service, order_repository, execution_session_repository = _build_service_with_external_id_broker(tmp_path)

    order = OrderRequest(
        order_id="order_demo_ext_2",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="audit-test",
    )

    session = execution_session_repository.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2024-01-02",
        idempotency_key=None,
        risk_summary={},
        order_count=1,
        status=TradeSessionStatus.RUNNING,
    )
    service._persist_order_intents(session.session_id, [order], [])
    result = service.reconcile_session(session.session_id, requested_by="tester")
    assert result.summary.status == TradeSessionStatus.COMPLETED
    assert result.orders[0].broker_order_id == "broker_order_demo_ext_1"
    assert result.fills[0].order_id == "order_demo_ext_2"
    assert result.fills[0].broker_order_id == "broker_order_demo_ext_1"


def test_trade_orchestrator_persists_pre_trade_reject_as_formal_order(tmp_path: Path) -> None:
    service, order_repository, _ = _build_service(tmp_path)

    accepted_order = OrderRequest(
        order_id="order_demo_accept_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="audit-test",
    )
    rejected_order = OrderRequest(
        order_id="order_demo_reject_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=50,
        reason="odd-lot-test",
    )

    result = service.submit_orders([accepted_order, rejected_order], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.PARTIALLY_COMPLETED
    stored_orders = order_repository.list_orders(execution_session_id=result.summary.session_id, limit=10)
    assert len(stored_orders) == 2
    by_order_id = {row["order_id"]: row for row in stored_orders}
    assert by_order_id["order_demo_accept_1"]["status"] == OrderStatus.FILLED.value
    assert by_order_id["order_demo_reject_1"]["status"] == OrderStatus.PRE_TRADE_REJECTED.value
    assert "最小交易单位" in str(by_order_id["order_demo_reject_1"]["last_error"])


def test_trade_orchestrator_operator_risk_blocks_st_buy_orders(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    store = SQLiteStore(config.database.path)
    store.init_schema(load_schema_sql())
    market_repository = MarketRepository(store)
    market_repository.upsert_securities(
        {
            "600001.SH": Security(
                ts_code="600001.SH",
                name="ST Demo",
                exchange="SSE",
                board="MAIN",
                is_st=True,
                status="L",
                list_date=None,
                delist_date=None,
            )
        }
    )
    market_repository.upsert_bars(
        [
            Bar(
                ts_code="600001.SH",
                trade_date=date(2024, 1, 2),
                open=10.0,
                high=10.2,
                low=9.8,
                close=10.0,
                volume=1000.0,
                amount=10000.0,
                pre_close=10.0,
                suspended=False,
                limit_up=False,
                limit_down=False,
            )
        ]
    )
    order_repository = OrderRepository(store)
    execution_session_repository = ExecutionSessionRepository(store)
    broker = _DemoBroker()
    reconciliation_service = TradeReconciliationService(
        broker=broker,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
    )
    service = TradeOrchestratorService(
        config=config,
        broker=broker,
        market_repository=market_repository,
        order_repository=order_repository,
        audit_repository=AuditRepository(store),
        execution_session_repository=execution_session_repository,
        reconciliation_service=reconciliation_service,
    )

    order = OrderRequest(
        order_id="order_demo_st_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600001.SH",
        side=OrderSide.BUY,
        price=10.0,
        quantity=100,
        reason="st-block-test",
    )

    result = service.submit_orders([order], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.REJECTED
    assert result.orders[0].status == OrderStatus.PRE_TRADE_REJECTED
    assert "ST 证券" in str(result.orders[0].last_error)


def test_trade_orchestrator_marks_pending_acceptance_as_recovery_required(tmp_path: Path) -> None:
    service, order_repository, execution_session_repository = _build_service_with_accepted_only_broker(tmp_path)

    order = OrderRequest(
        order_id="order_demo_pending_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="pending-acceptance-test",
    )

    result = service.submit_orders([order], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.RECOVERY_REQUIRED
    assert result.summary.submitted_count == 1
    assert "未终结 broker 订单" in str(result.summary.error_message)
    assert result.orders[0].status == OrderStatus.ACCEPTED
    stored_orders = order_repository.list_orders(execution_session_id=result.summary.session_id, limit=10)
    assert len(stored_orders) == 1
    assert stored_orders[0]["status"] == OrderStatus.ACCEPTED.value
    event_types = [event.event_type for event in execution_session_repository.list_events(result.summary.session_id)]
    assert "ORDER_ACCEPTED" in event_types
    assert "ORDER_TICKET_RECEIVED" in event_types



def test_trade_orchestrator_reissues_conflicting_operator_order_id(tmp_path: Path) -> None:
    service, order_repository, _ = _build_service_with_external_id_broker(tmp_path)

    existing = OrderRequest(
        order_id="operator_manual_conflict",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="existing-order",
    )
    order_repository.save_orders(None, [existing], execution_session_id="historical_session")

    incoming = OrderRequest(
        order_id="operator_manual_conflict",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="new-order",
    )

    result = service.submit_orders([incoming], command_source="test", requested_by="tester")
    assert result.summary.status == TradeSessionStatus.COMPLETED
    assert result.orders[0].order_id != "operator_manual_conflict"
    assert result.orders[0].order_id.startswith("operator_order_")

    historical = order_repository.get_order_by_id("operator_manual_conflict")
    assert historical is not None
    assert historical["execution_session_id"] == "historical_session"

    new_row = order_repository.get_order_by_id(result.orders[0].order_id)
    assert new_row is not None
    assert new_row["execution_session_id"] == result.summary.session_id


def test_trade_orchestrator_sync_session_events_completes_pending_order_with_account_scope(tmp_path: Path) -> None:
    service, order_repository, execution_session_repository = _build_service_with_polling_broker(tmp_path)

    order = OrderRequest(
        order_id="order_demo_sync_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="sync-account-test",
    )

    submit_result = service.submit_orders(
        [order],
        command_source="test",
        requested_by="tester",
        account_id="demo-account-2",
    )
    assert submit_result.summary.status == TradeSessionStatus.RECOVERY_REQUIRED
    latest_session = execution_session_repository.get(submit_result.summary.session_id)
    assert latest_session is not None
    assert latest_session.account_id == "demo-account-2"
    stored_orders = order_repository.list_orders(execution_session_id=submit_result.summary.session_id, account_id="demo-account-2", limit=10)
    assert len(stored_orders) == 1
    assert stored_orders[0]["account_id"] == "demo-account-2"

    sync_result = service.sync_session_events(submit_result.summary.session_id, requested_by="tester")
    assert sync_result.summary.status == TradeSessionStatus.COMPLETED
    assert sync_result.summary.account_id == "demo-account-2"
    assert sync_result.summary.last_synced_at is not None
    synced_orders = order_repository.list_orders(execution_session_id=submit_result.summary.session_id, account_id="demo-account-2", limit=10)
    assert len(synced_orders) == 1
    assert synced_orders[0]["status"] == OrderStatus.FILLED.value
    synced_fills = order_repository.list_fills(execution_session_id=submit_result.summary.session_id, account_id="demo-account-2", limit=10)
    assert len(synced_fills) == 1
    assert synced_fills[0]["account_id"] == "demo-account-2"
    event_types = [event.event_type for event in execution_session_repository.list_events(submit_result.summary.session_id)]
    assert "SESSION_SYNC_COMPLETED" in event_types
    assert "ORDER_FILLED" in event_types


def test_trade_orchestrator_rejects_account_outside_allowlist(tmp_path: Path) -> None:
    service, _, _ = _build_service(tmp_path)

    order = OrderRequest(
        order_id="order_demo_bad_account_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="bad-account-test",
    )

    try:
        service.submit_orders([order], command_source="test", requested_by="tester", account_id="forbidden-account")
    except ValueError as exc:
        assert "不在允许列表内" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")


def test_trade_orchestrator_sync_latest_open_session_ignores_partially_completed(tmp_path: Path) -> None:
    service, _order_repository, execution_session_repository = _build_service(tmp_path)

    execution_session_repository.create_session(
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

    try:
        service.sync_latest_open_session(requested_by="tester")
    except ValueError as exc:
        assert "当前没有需要同步事件的交易会话" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")
