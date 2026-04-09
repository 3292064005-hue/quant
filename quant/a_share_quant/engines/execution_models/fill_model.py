"""撮合/成交模型实现。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import Bar, OrderRequest
from a_share_quant.engines.execution_models.base import FillModel, FillPlan


class VolumeShareFillModel(FillModel):
    """按成交量占比和手数约束生成执行计划。"""

    def __init__(self, *, max_volume_share: float = 1.0, lot_size: int = 100, allow_partial_fill: bool = True) -> None:
        if max_volume_share <= 0:
            raise ValueError("max_volume_share 必须大于 0")
        if lot_size <= 0:
            raise ValueError("lot_size 必须大于 0")
        self.max_volume_share = max_volume_share
        self.lot_size = lot_size
        self.allow_partial_fill = allow_partial_fill

    def build_plan(self, order: OrderRequest, bar: Bar, trade_date: date, executable_price: float) -> FillPlan:
        """根据 bar 成交量和整数手约束计算本次可执行数量。"""
        if order.trade_date != trade_date:
            raise ValueError("order.trade_date 与当前执行 trade_date 不一致")
        if executable_price <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="执行价格必须大于 0",
                metadata={"reason": "invalid_price"},
            )
        if bar.suspended:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="停牌 bar 不允许成交",
                metadata={"reason": "suspended_bar"},
            )
        remaining = order.remaining_quantity
        if remaining <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="订单已无剩余可成交数量",
                metadata={"reason": "already_filled"},
            )
        if bar.volume <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="bar.volume <= 0，无法成交",
                metadata={"reason": "zero_volume"},
            )
        allowed_raw = int(bar.volume * self.max_volume_share)
        if allowed_raw <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="当前 volume share 约束下无可成交数量",
                metadata={"reason": "volume_share_limit"},
            )
        lot_aligned = max((allowed_raw // self.lot_size) * self.lot_size, 0)
        if lot_aligned <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="volume share 无法满足最小整数手约束",
                metadata={"reason": "lot_blocked"},
            )
        executable_quantity = min(remaining, lot_aligned)
        if executable_quantity < remaining and not self.allow_partial_fill:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="当前配置禁止部分成交",
                metadata={"reason": "partial_fill_disabled", "partial_candidate": executable_quantity},
            )
        if executable_quantity <= 0:
            return FillPlan(
                requested_quantity=order.quantity,
                executable_quantity=0,
                reference_price=bar.close,
                executable_price=executable_price,
                message="无可成交数量",
                metadata={"reason": "no_executable_quantity"},
            )
        return FillPlan(
            requested_quantity=order.quantity,
            executable_quantity=executable_quantity,
            reference_price=bar.close,
            executable_price=executable_price,
            message="部分成交" if executable_quantity < remaining else "全部成交",
            metadata={
                "reason": "ok_partial" if executable_quantity < remaining else "ok_full",
                "remaining_after_execution": max(remaining - executable_quantity, 0),
                "bar_volume": bar.volume,
                "max_volume_share": self.max_volume_share,
                "lot_size": self.lot_size,
            },
        )
