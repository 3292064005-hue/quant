from datetime import date

from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.domain.models import OrderRequest, OrderSide


def test_mock_broker_buy_and_sell() -> None:
    broker = MockBroker(initial_cash=100000.0, fee_bps=3.0, tax_bps=10.0)
    buy_order = OrderRequest(order_id="o1", trade_date=date(2026, 1, 1), strategy_id="s", ts_code="600000.SH", side=OrderSide.BUY, price=10.0, quantity=100, reason="buy")
    fill_buy = broker.submit_order(buy_order, 10.0, date(2026, 1, 1))
    assert fill_buy.fill_quantity == 100
    sell_order = OrderRequest(order_id="o2", trade_date=date(2026, 1, 2), strategy_id="s", ts_code="600000.SH", side=OrderSide.SELL, price=11.0, quantity=100, reason="sell")
    fill_sell = broker.submit_order(sell_order, 11.0, date(2026, 1, 2))
    assert fill_sell.fill_quantity == 100
    account = broker.get_account({"600000.SH": 11.0})
    assert account.cash > 100000.0
    assert account.pnl == account.cum_pnl
    assert account.drawdown <= 0.0
