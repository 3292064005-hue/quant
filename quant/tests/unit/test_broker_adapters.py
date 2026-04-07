from __future__ import annotations

from datetime import date

import pytest

from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.core.exceptions import BrokerContractError
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, PositionSnapshot


class _BrokerClientStub:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.cancelled: list[str] = []

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.closed = True

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


class _RawPayloadClient(_BrokerClientStub):
    def get_account(self, last_prices):
        return {"cash": 100.0, "available_cash": 90.0, "market_value": 10.0, "total_assets": 110.0, "pnl": 5.0}

    def get_positions(self, last_prices):
        return [{"symbol": "600000.SH", "qty": 100, "available": 80, "avg_cost": 10.0, "market_value": 1000.0, "unrealized_pnl": 12.5}]

    def submit_order(self, order, fill_price, trade_date):
        return {"trade_id": "f2", "price": fill_price, "deal_qty": order.quantity, "fee": 0.1, "tax": 0.0}

    def query_orders(self):
        return [{"order_id": "o-hist", "trade_date": "2026-01-05", "strategy_id": "demo", "ts_code": "600000.SH", "side": "BUY", "price": 10.0, "quantity": 100, "reason": "hist"}]

    def query_trades(self):
        return [{"fill_id": "f-hist", "order_id": "o-hist", "trade_date": "2026-01-05", "ts_code": "600000.SH", "side": "BUY", "fill_price": 10.0, "fill_quantity": 100, "fee": 0.1, "tax": 0.0}]


class _BrokenPayloadClient(_BrokerClientStub):
    def get_account(self, last_prices):
        return {"cash": "not-a-number"}



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
    assert account.total_assets >= 100.0
    assert positions[0].ts_code == "600000.SH"
    assert fill.fill_price == 10.1
    assert adapter.heartbeat() is True



def test_qmt_adapter_contract_mapping() -> None:
    client = _BrokerClientStub()
    adapter = QMTAdapter(client, timeout_seconds=1.0)
    _exercise_adapter(adapter)
    adapter.close()
    assert client.connected is True
    assert client.closed is True
    assert client.cancelled == ["broker-order-1"]
    with pytest.raises(RuntimeError):
        adapter.get_account({})



def test_ptrade_adapter_contract_mapping() -> None:
    client = _BrokerClientStub()
    adapter = PTradeAdapter(client, timeout_seconds=1.0)
    _exercise_adapter(adapter)
    adapter.close()
    assert client.connected is True
    assert client.closed is True
    assert client.cancelled == ["broker-order-1"]
    with pytest.raises(RuntimeError):
        adapter.get_account({})



def test_qmt_adapter_maps_raw_payloads_to_domain_objects() -> None:
    adapter = QMTAdapter(_RawPayloadClient(), timeout_seconds=1.0)
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
    orders = adapter.query_orders()
    trades = adapter.query_trades()
    assert account.available_cash == 90.0
    assert positions[0].available_quantity == 80
    assert fill.order_id == "o1"
    assert orders[0].strategy_id == "demo"
    assert trades[0].fill_quantity == 100



def test_ptrade_adapter_rejects_invalid_payload_contract() -> None:
    adapter = PTradeAdapter(_BrokenPayloadClient(), timeout_seconds=1.0)
    adapter.connect()
    with pytest.raises(BrokerContractError):
        adapter.get_account({})


class _LenientPayloadClient(_BrokerClientStub):
    def get_account(self, last_prices):
        return {"available": 120.0, "assets": 130.0}

    def get_positions(self, last_prices):
        return [{"symbol": "600000.SH", "qty": "bad-int", "available": 50}]

    def submit_order(self, order, fill_price, trade_date):
        return {"deal_qty": "bad-int"}

    def query_orders(self):
        return [{"symbol": "600000.SH", "side": "BUY", "price": 10.0, "quantity": 100}]

    def query_trades(self):
        return [{"symbol": "600000.SH", "side": "BUY", "trade_date": "2026-01-05", "fill_price": 10.2}]


def test_qmt_adapter_supports_lenient_contract_mapping_mode() -> None:
    adapter = QMTAdapter(_LenientPayloadClient(), timeout_seconds=1.0, strict_contract_mapping=False)
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
    orders = adapter.query_orders()
    trades = adapter.query_trades()
    assert account.available_cash == 120.0
    assert account.total_assets == 130.0
    assert positions[0].quantity == 0
    assert fill.fill_quantity == 100
    assert orders == []
    assert trades[0].ts_code == "600000.SH"
