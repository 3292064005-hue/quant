"""执行引擎。"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.core.events import EventBus, EventType
from a_share_quant.core.exceptions import OrderRejectedError
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import Bar, ExecutionReport, Fill, OrderRequest, OrderStatus, OrderTicket
from a_share_quant.engines.execution_models import (
    AShareSellTaxModel,
    BpsFeeModel,
    BpsSlippageModel,
    FeeModel,
    FillModel,
    SlippageModel,
    TaxModel,
    VolumeShareFillModel,
)


@dataclass(slots=True)
class ExecutionOutcome:
    """执行结果。"""

    fills: list[Fill] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)
    tickets: dict[str, OrderTicket] = field(default_factory=dict)
    reports: list[ExecutionReport] = field(default_factory=list)


class ExecutionEngine:
    """将订单路由到券商适配器。

    当前引擎显式拆分了滑点、撮合、手续费、税费四类执行模型；
    其中手续费/税费模型当前用于生成正式执行回报的预估值，真实成交结果仍以 broker 返回为准。
    """

    def __init__(
        self,
        broker: BrokerBase,
        event_bus: EventBus | None = None,
        *,
        slippage_model: SlippageModel | None = None,
        fill_model: FillModel | None = None,
        fee_model: FeeModel | None = None,
        tax_model: TaxModel | None = None,
        slippage_bps: float = 0.0,
    ) -> None:
        self.broker = broker
        self.event_bus = event_bus or EventBus()
        self.slippage_model = slippage_model or BpsSlippageModel(slippage_bps)
        self.fill_model = fill_model or VolumeShareFillModel()
        self.fee_model = fee_model or BpsFeeModel()
        self.tax_model = tax_model or AShareSellTaxModel()

    def execute(self, orders: list[OrderRequest], bars: dict[str, Bar], trade_date: date) -> ExecutionOutcome:
        """执行订单。

        Args:
            orders: 待执行订单列表。
            bars: 当日行情映射。
            trade_date: 当前交易日。

        Returns:
            `ExecutionOutcome`，其中包含成交回报、拒单原因、票据以及执行状态回报。

        Boundary Behavior:
            - 缺少行情、bar 无法成交、券商拒绝均会收敛为订单级拒单，不中断整批执行；
            - 部分成交不会生成伪第二张订单，仍沿用同一 `order_id`，剩余量通过 `OrderTicket` 与订单状态体现；
            - 手续费/税费估算只进入 `ExecutionReport`，真实成交成本仍以 `Fill` 为准。
        """
        outcome = ExecutionOutcome()
        for order in orders:
            ticket = OrderTicket.from_order(order)
            outcome.tickets[order.order_id] = ticket
            self._publish_order_event(
                event_type=EventType.ORDER_SUBMITTED,
                order=order,
                ticket=ticket,
                message="订单已进入执行引擎",
            )
            order.mark_submitted(order.broker_order_id)
            submitted_report = self._build_report(order, trade_date, status=order.status, message="订单已提交至执行引擎")
            ticket.append_report(submitted_report)
            outcome.reports.append(submitted_report)
            self.event_bus.publish_type(EventType.EXECUTION_REPORT, self._report_payload(submitted_report))

            bar = bars.get(order.ts_code)
            if bar is None:
                self._reject_order(order, ticket, trade_date, outcome, reason=f"缺少行情数据: {order.ts_code}")
                continue

            executable_price = self.slippage_model.apply(bar.close, order.side)
            plan = self.fill_model.build_plan(order, bar, trade_date, executable_price)
            if plan.is_reject:
                self._reject_order(order, ticket, trade_date, outcome, reason=plan.message, metadata=plan.metadata)
                continue

            est_fee = self.fee_model.estimate(order, plan.executable_price, plan.executable_quantity)
            est_tax = self.tax_model.estimate(order, plan.executable_price, plan.executable_quantity)
            order.mark_accepted(order.broker_order_id)
            accepted_report = self._build_report(
                order,
                trade_date,
                status=OrderStatus.ACCEPTED,
                message=plan.message,
                fee_estimate=est_fee,
                tax_estimate=est_tax,
                metadata=plan.metadata,
            )
            ticket.append_report(accepted_report)
            outcome.reports.append(accepted_report)
            self._publish_order_event(
                event_type=EventType.ORDER_ACCEPTED,
                order=order,
                ticket=ticket,
                message=plan.message,
                metadata={**plan.metadata, "fee_estimate": est_fee, "tax_estimate": est_tax},
            )
            self.event_bus.publish_type(EventType.EXECUTION_REPORT, self._report_payload(accepted_report))

            execution_order = replace(order, quantity=plan.executable_quantity)
            try:
                fill = self.broker.submit_order(execution_order, plan.executable_price, trade_date)
            except OrderRejectedError as exc:
                self._reject_order(order, ticket, trade_date, outcome, reason=str(exc), metadata=plan.metadata)
                continue

            order.apply_fill(
                fill_quantity=fill.fill_quantity,
                fill_price=fill.fill_price,
                broker_order_id=execution_order.broker_order_id or order.broker_order_id,
            )
            fill_status = OrderStatus.FILLED if order.remaining_quantity == 0 else OrderStatus.PARTIALLY_FILLED
            fill_report = self._build_report(
                order,
                trade_date,
                status=fill_status,
                message="订单全部成交" if fill_status == OrderStatus.FILLED else "订单部分成交，保留剩余未成交数量",
                fill_price=fill.fill_price,
                fee_estimate=fill.fee,
                tax_estimate=fill.tax,
                metadata={**plan.metadata, "fill_id": fill.fill_id},
            )
            ticket.append_report(fill_report)
            outcome.reports.append(fill_report)
            outcome.fills.append(fill)
            event_type = EventType.ORDER_FILLED if fill_status == OrderStatus.FILLED else EventType.ORDER_PARTIALLY_FILLED
            self._publish_order_event(
                event_type=event_type,
                order=order,
                ticket=ticket,
                message=fill_report.message,
                metadata={**plan.metadata, "fill_id": fill.fill_id, "fill_quantity": fill.fill_quantity},
            )
            self.event_bus.publish_type(EventType.EXECUTION_REPORT, self._report_payload(fill_report))
        return outcome

    def _reject_order(
        self,
        order: OrderRequest,
        ticket: OrderTicket,
        trade_date: date,
        outcome: ExecutionOutcome,
        *,
        reason: str,
        metadata: dict | None = None,
    ) -> None:
        order.mark_rejected(OrderStatus.EXECUTION_REJECTED, reason)
        report = self._build_report(order, trade_date, status=OrderStatus.EXECUTION_REJECTED, message=reason, metadata=metadata)
        ticket.append_report(report)
        outcome.reports.append(report)
        outcome.rejected[order.order_id] = reason
        self._publish_order_event(
            event_type=EventType.ORDER_REJECTED,
            order=order,
            ticket=ticket,
            message=reason,
            metadata=metadata,
        )
        self.event_bus.publish_type(EventType.EXECUTION_REPORT, self._report_payload(report))

    def _build_report(
        self,
        order: OrderRequest,
        trade_date: date,
        *,
        status: OrderStatus,
        message: str,
        fill_price: float | None = None,
        fee_estimate: float | None = None,
        tax_estimate: float | None = None,
        metadata: dict | None = None,
    ) -> ExecutionReport:
        return ExecutionReport(
            report_id=new_id("exec"),
            order_id=order.order_id,
            trade_date=trade_date,
            status=status,
            requested_quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            remaining_quantity=order.remaining_quantity,
            message=message,
            fill_price=fill_price,
            fee_estimate=fee_estimate,
            tax_estimate=tax_estimate,
            broker_order_id=order.broker_order_id,
            metadata=dict(metadata or {}),
        )

    def _publish_order_event(
        self,
        *,
        event_type: str,
        order: OrderRequest,
        ticket: OrderTicket,
        message: str,
        metadata: dict | None = None,
    ) -> None:
        self.event_bus.publish_type(
            event_type,
            {
                "order_id": order.order_id,
                "ts_code": order.ts_code,
                "status": order.status.value,
                "requested_quantity": order.quantity,
                "filled_quantity": order.filled_quantity,
                "remaining_quantity": order.remaining_quantity,
                "broker_order_id": order.broker_order_id,
                "ticket_status": ticket.status.value,
                "message": message,
                **dict(metadata or {}),
            },
        )

    @staticmethod
    def _report_payload(report: ExecutionReport) -> dict[str, object]:
        return {
            "report_id": report.report_id,
            "order_id": report.order_id,
            "status": report.status.value,
            "requested_quantity": report.requested_quantity,
            "filled_quantity": report.filled_quantity,
            "remaining_quantity": report.remaining_quantity,
            "message": report.message,
            "fill_price": report.fill_price,
            "fee_estimate": report.fee_estimate,
            "tax_estimate": report.tax_estimate,
            "broker_order_id": report.broker_order_id,
            "metadata": dict(report.metadata),
        }
