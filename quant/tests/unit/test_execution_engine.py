from __future__ import annotations

from datetime import date

from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.domain.models import OrderRequest, OrderSide, OrderStatus
from a_share_quant.engines.execution_engine import ExecutionEngine


def test_execution_engine_rejects_order_when_bar_missing() -> None:
    engine = ExecutionEngine(MockBroker(initial_cash=1000.0, fee_bps=3.0, tax_bps=10.0))
    order = OrderRequest(
        order_id="o1",
        trade_date=date(2026, 1, 5),
        strategy_id="demo",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.0,
        quantity=100,
        reason="test",
    )
    outcome = engine.execute([order], bars={}, trade_date=date(2026, 1, 5))
    assert outcome.fills == []
    assert "o1" in outcome.rejected
    assert order.status == OrderStatus.EXECUTION_REJECTED
