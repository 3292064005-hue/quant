"""手续费模型实现。"""
from __future__ import annotations

from a_share_quant.domain.models import OrderRequest
from a_share_quant.engines.execution_models.base import FeeModel


class BpsFeeModel(FeeModel):
    """按成交额基点估算手续费。"""

    def __init__(self, fee_bps: float = 0.0) -> None:
        self.fee_ratio = fee_bps / 10000.0

    def estimate(self, order: OrderRequest, price: float, quantity: int) -> float:
        """按成交额估算手续费。"""
        return max(price, 0.0) * max(quantity, 0) * self.fee_ratio
