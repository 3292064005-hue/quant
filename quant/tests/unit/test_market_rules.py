
from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.domain.models import Security


def test_normalize_order_quantity_floor_to_lot_main_board() -> None:
    security = Security("600000.SH", "浦发银行", "SSE", "主板")
    assert MarketRules.normalize_order_quantity(256, security) == 200
    assert MarketRules.normalize_order_quantity(99, security) == 0


def test_star_board_lot_size_is_200() -> None:
    security = Security("688001.SH", "华兴源创", "SSE", "科创板")
    assert MarketRules.normalize_order_quantity(399, security) == 200
    assert MarketRules.normalize_order_quantity(199, security) == 0


def test_compute_limit_prices_for_st_and_chinext() -> None:
    st_security = Security("600001.SH", "ST示例", "SSE", "主板", is_st=True)
    cy_security = Security("300001.SZ", "创业板示例", "SZSE", "创业板")
    assert MarketRules.compute_limit_prices(10.0, st_security) == (10.5, 9.5)
    assert MarketRules.compute_limit_prices(10.0, cy_security) == (12.0, 8.0)


def test_infer_limit_state() -> None:
    security = Security("600000.SH", "浦发银行", "SSE", "主板")
    assert MarketRules.infer_limit_state(11.0, 10.0, security) == (True, False)
    assert MarketRules.infer_limit_state(9.0, 10.0, security) == (False, True)


def test_normalize_sell_quantity_allows_odd_lot_clearance() -> None:
    security = Security("688001.SH", "华兴源创", "SSE", "科创板")
    assert MarketRules.normalize_sell_quantity(150, security, current_quantity=150) == 150
    assert MarketRules.normalize_sell_quantity(150, security, current_quantity=350) == 0
