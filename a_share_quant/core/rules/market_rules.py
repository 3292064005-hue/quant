"""市场规则。"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from a_share_quant.core.utils import floor_to_lot
from a_share_quant.domain.models import Bar, OrderSide, Security


class MarketRules:
    """A 股交易基础规则。

    当前实现覆盖：
    - 主板默认 100 股一手；科创板默认 200 股一手。
    - 主板默认 10% 涨跌幅；ST 5%；创业板/科创板 20%。

    Notes:
        对于上市首日及部分特殊情形的“无涨跌幅限制”，优先使用数据源
        提供的涨跌停价/布尔标志。若数据源无法提供，则本模块只做常规
        价格笼子与涨跌幅的静态推断，不将例外情形伪装成已精确覆盖。
    """

    MAIN_BOARD_LOT_SIZE: int = 100
    STAR_BOARD_LOT_SIZE: int = 200

    @classmethod
    def get_lot_size(cls, security: Security | None) -> int:
        """返回证券最小交易单位。"""
        if security and security.board in {"科创板", "STAR", "STAR Market"}:
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
        """按卖出规则规范化数量。

        Boundary Behavior:
            - 常规情况下按最小交易单位向下取整。
            - 若本次卖出即清仓，则允许最后不足一手的残余股一次性卖出。
        """
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

    @staticmethod
    def price_limit_ratio(security: Security | None) -> float:
        """返回常规日涨跌幅比例。"""
        if security and security.is_st:
            return 0.05
        if security and security.board in {"科创板", "创业板", "STAR", "ChiNext", "STAR Market"}:
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
