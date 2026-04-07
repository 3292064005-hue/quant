"""固定审计动作定义。"""
from __future__ import annotations

from enum import Enum


class BacktestAuditAction(str, Enum):
    """回测链路固定审计动作。"""

    TARGETS_GENERATED = "targets_generated"
    REBALANCE_SKIPPED = "rebalance_skipped"
    ORDER_EVALUATED = "order_evaluated"
    ORDER_REJECTED = "order_rejected"
    QUOTE_DEGRADED = "quote_degraded"
