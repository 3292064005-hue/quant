"""执行服务。"""
from __future__ import annotations

from a_share_quant.engines.execution_engine import ExecutionEngine


class ExecutionService:
    """执行服务封装。"""

    def __init__(self, engine: ExecutionEngine) -> None:
        self.engine = engine
