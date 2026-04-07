from datetime import date

from a_share_quant.core.rules.risk_rules import BlockedSecurityRule, KillSwitchRule, MaxOrderValueRule, MaxPositionWeightRule, PriceLimitRule, STBlockRule, TradingAvailabilityRule
from a_share_quant.domain.models import AccountSnapshot, Bar, OrderRequest, OrderSide, PositionSnapshot, Security
from a_share_quant.engines.risk_engine import RiskEngine


def _build_engine() -> RiskEngine:
    return RiskEngine(
        rules=[KillSwitchRule(), BlockedSecurityRule(), STBlockRule(), TradingAvailabilityRule(), PriceLimitRule(), MaxOrderValueRule(100000.0), MaxPositionWeightRule(0.5)],
        blocked_symbols=set(),
        kill_switch=False,
        sequential_cash_reservation=True,
        fee_bps=3.0,
        tax_bps=10.0,
    )


def test_risk_engine_blocks_st_buy() -> None:
    engine = _build_engine()
    order = OrderRequest(order_id="o1", trade_date=date(2026, 1, 1), strategy_id="s", ts_code="600999.SH", side=OrderSide.BUY, price=10.0, quantity=100, reason="test")
    accepted, audit = engine.validate_orders(
        [order],
        securities={"600999.SH": Security("600999.SH", "招商证券", "SSE", "MAIN", is_st=True)},
        bars={"600999.SH": Bar("600999.SH", date(2026, 1, 1), 10, 10, 10, 10, 1, 1)},
        positions={},
        account=AccountSnapshot(100000, 100000, 0, 100000, 0),
        target_weights={"600999.SH": 0.1},
    )
    assert accepted == []
    assert any(not item.passed and item.rule_name == "STBlockRule" for item in audit["o1"])


def test_risk_engine_sequential_cash_reservation_blocks_second_buy() -> None:
    engine = _build_engine()
    orders = [
        OrderRequest(order_id="o1", trade_date=date(2026, 1, 1), strategy_id="s", ts_code="600000.SH", side=OrderSide.BUY, price=500.0, quantity=100, reason="buy1"),
        OrderRequest(order_id="o2", trade_date=date(2026, 1, 1), strategy_id="s", ts_code="000001.SZ", side=OrderSide.BUY, price=500.0, quantity=100, reason="buy2"),
    ]
    securities = {
        "600000.SH": Security("600000.SH", "浦发银行", "SSE", "主板"),
        "000001.SZ": Security("000001.SZ", "平安银行", "SZSE", "主板"),
    }
    bars = {
        code: Bar(code, date(2026, 1, 1), 10, 10, 10, 10, 1, 1)
        for code in securities
    }
    accepted, audit = engine.validate_orders(
        orders,
        securities=securities,
        bars=bars,
        positions={},
        account=AccountSnapshot(70000, 70000, 0, 70000, 0),
        target_weights={"600000.SH": 0.1, "000001.SZ": 0.1},
    )
    assert [order.order_id for order in accepted] == ["o1"]
    assert any(result.rule_name == "AvailableCashRule" and not result.passed for result in audit["o2"])
