from __future__ import annotations

from datetime import date
from pathlib import Path

from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import OrderRequest, OrderSide, OrderStatus
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


def test_order_repository_upsert_preserves_created_at_and_updates_status(tmp_path: Path) -> None:
    store = SQLiteStore(str(tmp_path / "test.db"))
    store.init_schema(load_schema_sql())
    repository = OrderRepository(store)
    order = OrderRequest(
        order_id="order_upsert_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="initial",
    )

    repository.save_orders(None, [order], execution_session_id="session_1")
    first = repository.list_orders(execution_session_id="session_1", limit=1)[0]

    order.status = OrderStatus.FILLED
    order.filled_quantity = 100
    order.avg_fill_price = 10.5
    repository.save_orders(None, [order], execution_session_id="session_1")
    second = repository.list_orders(execution_session_id="session_1", limit=1)[0]

    assert first["created_at"] == second["created_at"]
    assert second["updated_at"] >= first["updated_at"]
    assert second["status"] == OrderStatus.FILLED.value
    assert int(second["filled_quantity"]) == 100



def test_order_repository_rejects_conflicting_order_id_reuse_across_sessions(tmp_path: Path) -> None:
    store = SQLiteStore(str(tmp_path / "test.db"))
    store.init_schema(load_schema_sql())
    repository = OrderRepository(store)
    original = OrderRequest(
        order_id="order_conflict_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="initial",
    )
    conflicting = OrderRequest(
        order_id="order_conflict_1",
        trade_date=date(2024, 1, 2),
        strategy_id="operator.manual",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.5,
        quantity=100,
        reason="initial",
    )

    repository.save_orders(None, [original], execution_session_id="session_A")

    try:
        repository.save_orders(None, [conflicting], execution_session_id="session_B")
    except ValueError as exc:
        assert "order_id 冲突" in str(exc)
    else:
        raise AssertionError("expected ValueError for conflicting order_id reuse")

    stored = repository.get_order_by_id("order_conflict_1")
    assert stored is not None
    assert stored["execution_session_id"] == "session_A"
