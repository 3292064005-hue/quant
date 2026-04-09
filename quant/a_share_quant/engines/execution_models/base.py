"""执行模型抽象。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from a_share_quant.domain.models import Bar, OrderRequest, OrderSide


@dataclass(slots=True)
class FillPlan:
    """执行计划。"""

    requested_quantity: int
    executable_quantity: int
    reference_price: float
    executable_price: float
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_reject(self) -> bool:
        """当前计划是否等价于拒单。"""
        return self.executable_quantity <= 0


class SlippageModel(ABC):
    """滑点模型。"""

    @abstractmethod
    def apply(self, reference_price: float, side: OrderSide) -> float:
        """返回应用滑点后的可执行价格。"""


class FeeModel(ABC):
    """手续费模型。"""

    @abstractmethod
    def estimate(self, order: OrderRequest, price: float, quantity: int) -> float:
        """估算费用。"""


class TaxModel(ABC):
    """税费模型。"""

    @abstractmethod
    def estimate(self, order: OrderRequest, price: float, quantity: int) -> float:
        """估算税费。"""


class FillModel(ABC):
    """撮合/成交模型。"""

    @abstractmethod
    def build_plan(self, order: OrderRequest, bar: Bar, trade_date: date, executable_price: float) -> FillPlan:
        """基于订单与 bar 生成执行计划。"""
