"""operator broker 提交与状态汇总服务。"""
from __future__ import annotations

from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import ExecutionReport, LiveOrderSubmission, OrderRequest, OrderStatus, TradeSessionStatus


class OperatorSubmissionService:
    """封装 broker 生命周期兼容、订单回放与会话状态统计。"""

    def __init__(self, broker: LiveBrokerPort) -> None:
        self.broker = broker

    def submit_order_lifecycle(self, order: OrderRequest) -> LiveOrderSubmission:
        """兼容 broker 生命周期接口并返回统一 submission 聚合。"""
        submit_lifecycle = getattr(self.broker, "submit_order_lifecycle", None)
        if callable(submit_lifecycle):
            return submit_lifecycle(order, order.price, order.trade_date)
        fill = self.broker.submit_order(order, order.price, order.trade_date)
        broker_order_id = order.broker_order_id or fill.broker_order_id or order.order_id
        accepted_report = ExecutionReport(
            report_id=new_id("report"),
            order_id=order.order_id,
            trade_date=order.trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="broker accepted order",
            broker_order_id=broker_order_id,
            account_id=order.account_id,
            metadata={"source": "service_legacy_submit_order"},
        )
        filled_quantity = max(min(int(fill.fill_quantity), int(order.quantity)), 0)
        final_status = OrderStatus.FILLED if filled_quantity >= int(order.quantity) else OrderStatus.PARTIALLY_FILLED
        final_report = ExecutionReport(
            report_id=new_id("report"),
            order_id=order.order_id,
            trade_date=order.trade_date,
            status=final_status,
            requested_quantity=int(order.quantity),
            filled_quantity=filled_quantity,
            remaining_quantity=max(int(order.quantity) - filled_quantity, 0),
            message="broker fill received",
            fill_price=float(fill.fill_price),
            fee_estimate=float(fill.fee),
            tax_estimate=float(fill.tax),
            broker_order_id=broker_order_id,
            account_id=order.account_id,
            metadata={"source": "service_legacy_submit_order"},
        )
        ticket = self.build_submission_ticket(
            order,
            broker_order_id=broker_order_id,
            filled_quantity=filled_quantity,
            fill_price=float(fill.fill_price),
            status=final_status,
        )
        return LiveOrderSubmission(ticket=ticket, reports=[accepted_report, final_report], fills=[fill])

    @staticmethod
    def build_submission_ticket(
        order: OrderRequest,
        *,
        broker_order_id: str | None,
        filled_quantity: int,
        fill_price: float | None,
        status: OrderStatus,
    ):
        from a_share_quant.domain.models import OrderTicket

        return OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=status,
            broker_order_id=broker_order_id,
            filled_quantity=filled_quantity,
            avg_fill_price=fill_price if filled_quantity > 0 else None,
        )

    def apply_submission_to_order(self, order: OrderRequest, submission: LiveOrderSubmission) -> None:
        """把 broker 生命周期结果回放到本地订单对象。"""
        broker_order_id = (
            submission.ticket.broker_order_id
            or next((report.broker_order_id for report in submission.reports if report.broker_order_id), None)
            or next((fill.broker_order_id for fill in submission.fills if fill.broker_order_id), None)
        )
        if broker_order_id:
            order.mark_submitted(broker_order_id)
        for report in submission.reports:
            if report.status == OrderStatus.SUBMITTED:
                order.mark_submitted(report.broker_order_id or order.broker_order_id)
            elif report.status == OrderStatus.ACCEPTED:
                order.mark_accepted(report.broker_order_id or order.broker_order_id)
            elif report.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}:
                order.mark_rejected(report.status, report.message or "broker rejected order")
        if submission.fills:
            if order.status not in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
                order.mark_accepted(broker_order_id)
            for fill in submission.fills:
                order.apply_fill(
                    fill_quantity=int(fill.fill_quantity),
                    fill_price=float(fill.fill_price),
                    broker_order_id=fill.broker_order_id or broker_order_id,
                )
            return
        latest_report = submission.latest_report
        if latest_report is not None:
            if latest_report.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
                order.status = latest_report.status
                order.filled_quantity = max(int(latest_report.filled_quantity), 0)
                order.avg_fill_price = latest_report.fill_price if latest_report.fill_price is not None else order.avg_fill_price
                order.last_error = None
            elif latest_report.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}:
                order.mark_rejected(latest_report.status, latest_report.message or "broker rejected order")
                return
        if latest_report is None and submission.ticket.status not in {OrderStatus.CREATED, OrderStatus.SUBMITTED}:
            order.status = submission.ticket.status
            order.filled_quantity = max(int(submission.ticket.filled_quantity), 0)
            order.avg_fill_price = submission.ticket.avg_fill_price

    @staticmethod
    def count_rejected_orders(orders: list[OrderRequest]) -> int:
        """统计正式拒单数量。"""
        return len([order for order in orders if order.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}])

    @staticmethod
    def count_submitted_orders(orders: list[OrderRequest]) -> int:
        """统计已进入 broker 生命周期的订单数量。"""
        terminal_submitted_statuses = {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.PENDING_CANCEL,
            OrderStatus.CANCELLED,
            OrderStatus.CANCEL_REJECTED,
            OrderStatus.EXPIRED,
        }
        return len([order for order in orders if order.status in terminal_submitted_statuses or bool(order.broker_order_id)])

    @staticmethod
    def count_pending_follow_up_orders(orders: list[OrderRequest]) -> int:
        """统计仍需后续 reconciliation/backfill 的订单数量。"""
        pending_statuses = {OrderStatus.SUBMITTED, OrderStatus.ACCEPTED, OrderStatus.PENDING_CANCEL}
        return len([order for order in orders if order.status in pending_statuses])

    @staticmethod
    def resolve_final_error_message(
        final_status: TradeSessionStatus,
        *,
        risk_summary: dict[str, Any],
        pending_follow_up_count: int,
    ) -> str | None:
        """根据会话终态生成最终错误/告警信息。"""
        if final_status == TradeSessionStatus.RECOVERY_REQUIRED:
            return f"存在 {pending_follow_up_count} 笔未终结 broker 订单，需要后续 reconciliation"
        if final_status in {TradeSessionStatus.REJECTED, TradeSessionStatus.FAILED}:
            return risk_summary.get("terminal_error")
        return None
