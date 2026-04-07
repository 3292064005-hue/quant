from __future__ import annotations

from datetime import date

from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, PositionSnapshot


class _BrokerClientStub:
    def __init__(self) -> None:
        self.connected = False
        self.cancelled: list[str] = []

    def connect(self) -> None:
        self.connected = True

    def get_account(self, last_prices):
        return AccountSnapshot(cash=100.0, available_cash=100.0, market_value=0.0, total_assets=100.0, pnl=0.0)

    def get_positions(self, last_prices):
        return [PositionSnapshot(ts_code="600000.SH", quantity=100, available_quantity=100, avg_cost=10.0, market_value=1000.0, unrealized_pnl=0.0)]

    def submit_order(self, order, fill_price, trade_date):
        return Fill(
            fill_id="f1",
            order_id=order.order_id,
            trade_date=trade_date,
            ts_code=order.ts_code,
            side=order.side,
            fill_price=fill_price,
            fill_quantity=order.quantity,
            fee=0.1,
            tax=0.0,
            run_id=order.run_id,
        )

    def cancel_order(self, broker_order_id):
        self.cancelled.append(broker_order_id)

    def query_orders(self):
        return []

    def query_trades(self):
        return []

    def heartbeat(self):
        return True


def _exercise_adapter(adapter) -> None:
    adapter.connect()
    account = adapter.get_account({})
    positions = adapter.get_positions({})
    order = OrderRequest(
        order_id="o1",
        trade_date=date(2026, 1, 5),
        strategy_id="demo",
        ts_code="600000.SH",
        side=OrderSide.BUY,
        price=10.0,
        quantity=100,
        reason="contract",
    )
    fill = adapter.submit_order(order, fill_price=10.1, trade_date=date(2026, 1, 5))
    adapter.cancel_order("broker-order-1")
    assert account.total_assets == 100.0
    assert positions[0].ts_code == "600000.SH"
    assert fill.fill_price == 10.1
    assert adapter.query_orders() == []
    assert adapter.query_trades() == []
    assert adapter.heartbeat() is True


def test_qmt_adapter_contract_mapping() -> None:
    client = _BrokerClientStub()
    adapter = QMTAdapter(client, timeout_seconds=1.0)
    _exercise_adapter(adapter)
    assert client.connected is True
    assert client.cancelled == ["broker-order-1"]


def test_ptrade_adapter_contract_mapping() -> None:
    client = _BrokerClientStub()
    adapter = PTradeAdapter(client, timeout_seconds=1.0)
    _exercise_adapter(adapter)
    assert client.connected is True
    assert client.cancelled == ["broker-order-1"]
