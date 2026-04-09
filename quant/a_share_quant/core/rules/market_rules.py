"""市场规则。"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from a_share_quant.core.utils import floor_to_lot
from a_share_quant.domain.models import Bar, OrderSide, Security


class MarketRules:
    """A 股交易基础规则。"""

    MAIN_BOARD_LOT_SIZE: int = 100
    STAR_BOARD_LOT_SIZE: int = 200

    _MAIN_BOARD_ALIASES = {"主板", "MAIN", "MAIN_BOARD", "MAIN BOARD"}
    _CHINEXT_ALIASES = {"创业板", "CHI", "CHINEXT", "CHI NEXT"}
    _STAR_BOARD_ALIASES = {"科创板", "STAR", "STAR MARKET", "STAR_BOARD"}

    @classmethod
    def normalize_board(cls, board: str | None) -> str:
        """把外部 board 别名规范化为统一枚举。"""
        normalized = str(board or "").strip().upper()
        if normalized in {item.upper() for item in cls._STAR_BOARD_ALIASES}:
            return "科创板"
        if normalized in {item.upper() for item in cls._CHINEXT_ALIASES}:
            return "创业板"
        if normalized in {item.upper() for item in cls._MAIN_BOARD_ALIASES}:
            return "主板"
        return str(board or "主板").strip() or "主板"

    @classmethod
    def get_lot_size(cls, security: Security | None) -> int:
        """返回证券最小交易单位。"""
        if security and cls.normalize_board(security.board) == "科创板":
            return cls.STAR_BOARD_LOT_SIZE
        return cls.MAIN_BOARD_LOT_SIZE

    @classmethod
    def normalize_order_quantity(cls, quantity: int | float, security: Security | None = None) -> int:
        """将订单数量按对应板块整数手向下取整。"""
        return floor_to_lot(quantity, cls.get_lot_size(security))

    @classmethod
    def normalize_sell_quantity(
        cls,
        quantity: int | float,
        security: Security | None = None,
        current_quantity: int | None = None,
    ) -> int:
        """按卖出规则规范化数量。"""
        desired = int(quantity)
        if current_quantity is not None and desired >= current_quantity > 0:
            return current_quantity
        return cls.normalize_order_quantity(desired, security)

    @staticmethod
    def can_trade(bar: Bar) -> bool:
        """判断证券是否允许交易。"""
        return not bar.suspended

    @staticmethod
    def violates_price_limit(bar: Bar, side: OrderSide) -> bool:
        """判断买卖方向是否违反涨跌停约束。"""
        if side == OrderSide.BUY:
            return bar.limit_up
        return bar.limit_down

    @classmethod
    def price_limit_ratio(cls, security: Security | None) -> float:
        """返回常规日涨跌幅比例。"""
        if security and security.is_st:
            return 0.05
        if security and cls.normalize_board(security.board) in {"科创板", "创业板"}:
            return 0.20
        return 0.10

    @classmethod
    def compute_limit_prices(
        cls,
        pre_close: float | None,
        security: Security | None,
    ) -> tuple[float | None, float | None]:
        """根据常规涨跌幅规则推算涨停/跌停价。"""
        if pre_close is None or pre_close <= 0:
            return None, None
        ratio = Decimal(str(cls.price_limit_ratio(security)))
        base = Decimal(str(pre_close))
        up = (base * (Decimal("1") + ratio)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        down = (base * (Decimal("1") - ratio)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(up), float(down)

    @classmethod
    def infer_limit_state(cls, close: float, pre_close: float | None, security: Security | None) -> tuple[bool, bool]:
        """用常规涨跌幅规则推断是否收于涨停/跌停。"""
        up_limit, down_limit = cls.compute_limit_prices(pre_close, security)
        if up_limit is None or down_limit is None:
            return False, False
        epsilon = 1e-9
        return close >= up_limit - epsilon, close <= down_limit + epsilon
