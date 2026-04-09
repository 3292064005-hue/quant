"""滑点模型实现。"""
from __future__ import annotations

from a_share_quant.domain.models import OrderSide
from a_share_quant.engines.execution_models.base import SlippageModel


class BpsSlippageModel(SlippageModel):
    """按基点收缩/抬升成交价。"""

    def __init__(self, slippage_bps: float = 0.0) -> None:
        self.slippage_ratio = slippage_bps / 10000.0

    def apply(self, reference_price: float, side: OrderSide) -> float:
        """返回应用买卖方向滑点后的价格。"""
        if reference_price <= 0:
            return reference_price
        if side == OrderSide.BUY:
            return reference_price * (1.0 + self.slippage_ratio)
        return reference_price * (1.0 - self.slippage_ratio)
