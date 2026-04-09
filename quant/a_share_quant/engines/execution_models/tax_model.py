"""税费模型实现。"""
from __future__ import annotations

from a_share_quant.domain.models import OrderRequest, OrderSide
from a_share_quant.engines.execution_models.base import TaxModel


class AShareSellTaxModel(TaxModel):
    """A 股卖出侧印花税估算。"""

    def __init__(self, tax_bps: float = 0.0) -> None:
        self.tax_ratio = tax_bps / 10000.0

    def estimate(self, order: OrderRequest, price: float, quantity: int) -> float:
        """买单返回 0，卖单按成交额估算。"""
        if order.side != OrderSide.SELL:
            return 0.0
        return max(price, 0.0) * max(quantity, 0) * self.tax_ratio
