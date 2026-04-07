"""执行服务。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import Bar, OrderRequest
from a_share_quant.engines.execution_engine import ExecutionEngine, ExecutionOutcome


class ExecutionService:
    """执行服务封装。

    Notes:
        历史版本只保留了一个空包装对象，容易让边界看起来比实际能力更完整。
        当前实现显式暴露执行入口，避免继续保留无意义薄封装。
    """

    def __init__(self, engine: ExecutionEngine) -> None:
        self.engine = engine

    def execute(self, orders: list[OrderRequest], bars: dict[str, Bar], trade_date: date) -> ExecutionOutcome:
        """将订单委托给执行引擎。"""
        return self.engine.execute(orders, bars, trade_date)
