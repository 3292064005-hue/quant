from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.core.utils import new_id, now_iso
from a_share_quant.domain.models import OrderRequest, OrderSide, OrderStatus, TradeCommandEvent, TradeSessionStatus, TradeSessionSummary
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.operator_account_capture_service import OperatorAccountCaptureService
from a_share_quant.services.operator_session_write_service import OperatorSessionWriteService
from a_share_quant.storage.sqlite_store import SQLiteStore


class _ExplodingRuntimeEventRepository:
    def __init__(self) -> None:
        self.calls = 0

    def append(self, **_kwargs):
        self.calls += 1
        if self.calls >= 2:
            raise RuntimeError("runtime event append failed")


def _build_summary() -> TradeSessionSummary:
    timestamp = now_iso()
    return TradeSessionSummary(
        session_id="session_atomic_1",
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        status=TradeSessionStatus.RUNNING,
        idempotency_key="atomic-key",
        requested_trade_date="2024-01-02",
        risk_summary={"accepted_order_count": 1},
        order_count=1,
        submitted_count=0,
        rejected_count=0,
        account_id="demo-account",
        created_at=timestamp,
        updated_at=timestamp,
    )


def _build_order() -> OrderRequest:
    return OrderRequest(
        order_id="order_atomic_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="atomic-test",
        status=OrderStatus.FILLED,
        broker_order_id="broker_atomic_1",
        filled_quantity=100,
        avg_fill_price=10.5,
        account_id="demo-account",
    )


def _build_event(session_id: str, event_type: str) -> TradeCommandEvent:
    return TradeCommandEvent(
        event_id=new_id("event"),
        session_id=session_id,
        event_type=event_type,
        level="INFO",
        payload={"event_type": event_type},
        created_at=now_iso(),
    )


def test_operator_session_write_service_rolls_back_partial_submit_persistence(tmp_path: Path) -> None:
    store = SQLiteStore(str(tmp_path / "atomic.db"))
    store.init_schema(load_schema_sql())
    execution_session_repository = ExecutionSessionRepository(store)
    order_repository = OrderRepository(store)
    execution_session_repository.runtime_event_repository = _ExplodingRuntimeEventRepository()
    service = OperatorSessionWriteService(
        execution_session_repository=execution_session_repository,
        order_repository=order_repository,
        account_capture_service=OperatorAccountCaptureService(account_repository=None),
    )

    summary = _build_summary()
    order = _build_order()
    events = [_build_event(summary.session_id, "SESSION_CREATED"), _build_event(summary.session_id, "ORDER_FILLED")]

    with pytest.raises(RuntimeError, match="runtime event append failed"):
        service.persist_submit_result(
            initial_summary=summary,
            final_status=TradeSessionStatus.COMPLETED,
            risk_summary={"accepted_order_count": 1},
            error_message=None,
            orders=[order],
            fills=[],
            events=events,
            account_capture_plan=OperatorAccountCaptureService(account_repository=None).disabled_plan(
                session_id=summary.session_id,
                trade_date=date(2024, 1, 2),
                account_id="demo-account",
                source="submit_orders",
                captured_at=now_iso(),
            ),
        )

    assert execution_session_repository.get(summary.session_id) is None
    assert order_repository.list_orders(execution_session_id=summary.session_id, limit=10) == []
    assert execution_session_repository.list_events(summary.session_id) == []
