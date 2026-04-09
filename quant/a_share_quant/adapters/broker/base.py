"""券商/执行端口抽象。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import (
    AccountSnapshot,
    ExecutionReport,
    Fill,
    LiveOrderSubmission,
    OrderRequest,
    OrderStatus,
    OrderTicket,
    PositionSnapshot,
)


ExecutionReportHandler = Callable[[list[ExecutionReport], str | None], None]


@dataclass(slots=True)
class ExecutionReportSubscription:
    """broker 执行回报订阅句柄。

    Notes:
        - 该句柄本身不规定具体线程/网络模型；
        - 只负责向上层暴露统一的关闭/活跃状态语义；
        - ``cursor`` 表示当前订阅已经消费到的 broker 事件游标，供 supervisor 续跑时回传。
    """

    close_callback: Callable[[], None] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    cursor: str | None = None
    active: bool = True

    def close(self) -> None:
        """关闭订阅。

        Boundary Behavior:
            - 重复关闭幂等；
            - 关闭回调异常不吞掉，保持 supervisor 可观测。
        """
        if not self.active:
            return
        self.active = False
        if self.close_callback is not None:
            self.close_callback()

    def update_cursor(self, cursor: str | None) -> None:
        """更新当前游标。"""
        if cursor:
            self.cursor = cursor


class ConnectionPort(ABC):
    """共享的连接生命周期与查询能力。"""

    @abstractmethod
    def connect(self) -> None:
        """建立连接。"""

    @abstractmethod
    def close(self) -> None:
        """关闭连接并释放资源。"""

    @abstractmethod
    def heartbeat(self) -> bool:
        """心跳检测。"""


class SimulatedExecutionPort(ConnectionPort, ABC):
    """研究回测/回放执行端口。

    该端口只承载可由历史数据驱动的账户状态推进语义，供 ``BacktestEngine`` 使用。
    未来若继续扩展 paper/live，应避免把真实 broker 状态机直接塞回该端口。
    """

    @abstractmethod
    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """返回账户快照。"""

    @abstractmethod
    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """返回持仓快照。"""

    @abstractmethod
    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """提交订单并返回成交结果。"""

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """撤单。"""

    @abstractmethod
    def query_orders(self) -> list[OrderRequest]:
        """查询订单。"""

    def query_orders_scoped(self, *, account_id: str | None = None) -> list[OrderRequest]:
        """按账户作用域查询订单。"""
        orders = self.query_orders()
        if account_id is None:
            return orders
        return [item for item in orders if getattr(item, "account_id", None) in {None, "", account_id}]

    @abstractmethod
    def query_trades(self) -> list[Fill]:
        """查询成交。"""

    def query_trades_scoped(self, *, account_id: str | None = None) -> list[Fill]:
        """按账户作用域查询成交。"""
        fills = self.query_trades()
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]


class LiveBrokerPort(ConnectionPort, ABC):
    """真实 broker 边界端口。

    该端口保留真实客户端的统一领域映射能力，用于 runtime 校验、paper/live orchestration
    以及未来的 operator workflow，但不应被研究回测引擎直接依赖。
    """

    @abstractmethod
    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """返回账户快照。"""

    def get_account_snapshot(
        self,
        *,
        account_id: str | None = None,
        last_prices: dict[str, float] | None = None,
    ) -> AccountSnapshot:
        """按账户作用域读取账户快照。

        Boundary Behavior:
            - 默认回退到历史 ``get_account``，保持单账户 broker 兼容；
            - 多账户适配器应覆盖该方法，向下透传 ``account_id``。
        """
        return self.get_account(last_prices=last_prices)

    @abstractmethod
    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """返回持仓快照。"""

    def get_position_snapshots(
        self,
        *,
        account_id: str | None = None,
        last_prices: dict[str, float] | None = None,
    ) -> list[PositionSnapshot]:
        """按账户作用域读取持仓快照。"""
        return self.get_positions(last_prices=last_prices)

    @abstractmethod
    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """提交订单并返回成交结果。"""

    def submit_order_lifecycle(self, order: OrderRequest, fill_price: float, trade_date: date) -> LiveOrderSubmission:
        """提交订单并返回正式生命周期结果。

        Args:
            order: 待提交订单。
            fill_price: 提交时使用的目标价格。
            trade_date: 业务交易日。

        Returns:
            ``LiveOrderSubmission``。默认实现用于兼容历史同步成交 broker：
            先调用 ``submit_order`` 拿到成交，再补一张最小 ``ticket`` 和两条正式 ``ExecutionReport``
            （``ACCEPTED`` 与终态 ``FILLED``/``PARTIALLY_FILLED``）。

        Boundary Behavior:
            - 若下层 broker 只支持同步成交，该默认实现不会改变旧适配器签名；
            - 若具体适配器能表达更丰富的生命周期，应覆盖该方法直接返回 broker 原生 ticket/report/fill 聚合结果。
        """
        fill = self.submit_order(order, fill_price, trade_date)
        broker_order_id = order.broker_order_id or fill.broker_order_id or order.order_id
        filled_quantity = max(min(int(fill.fill_quantity), int(order.quantity)), 0)
        final_status = OrderStatus.FILLED if filled_quantity >= int(order.quantity) else OrderStatus.PARTIALLY_FILLED
        accepted_report = ExecutionReport(
            report_id=new_id("report"),
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="broker accepted order",
            broker_order_id=broker_order_id,
            metadata={"source": "legacy_submit_order"},
        )
        final_report = ExecutionReport(
            report_id=new_id("report"),
            order_id=order.order_id,
            trade_date=trade_date,
            status=final_status,
            requested_quantity=int(order.quantity),
            filled_quantity=filled_quantity,
            remaining_quantity=max(int(order.quantity) - filled_quantity, 0),
            message="broker fill received",
            fill_price=float(fill.fill_price),
            fee_estimate=float(fill.fee),
            tax_estimate=float(fill.tax),
            broker_order_id=broker_order_id,
            metadata={"source": "legacy_submit_order"},
        )
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=final_status,
            broker_order_id=broker_order_id,
            filled_quantity=filled_quantity,
            avg_fill_price=float(fill.fill_price) if filled_quantity > 0 else None,
            reports=[accepted_report, final_report],
        )
        return LiveOrderSubmission(ticket=ticket, reports=[accepted_report, final_report], fills=[fill])

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """撤单。"""

    @abstractmethod
    def query_orders(self) -> list[OrderRequest]:
        """查询订单。"""

    def query_orders_scoped(self, *, account_id: str | None = None) -> list[OrderRequest]:
        """按账户作用域查询订单。"""
        orders = self.query_orders()
        if account_id is None:
            return orders
        return [item for item in orders if getattr(item, "account_id", None) in {None, "", account_id}]

    @abstractmethod
    def query_trades(self) -> list[Fill]:
        """查询成交。"""

    def query_trades_scoped(self, *, account_id: str | None = None) -> list[Fill]:
        """按账户作用域查询成交。"""
        fills = self.query_trades()
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]

    def supports_execution_report_subscription(self) -> bool:
        """返回当前 broker 是否支持 push/subscription 式执行回报。

        Boundary Behavior:
            - 默认返回 ``False``，保持历史 broker 仅轮询即可运行；
            - 适配器若能表达 broker 原生订阅语义，应覆盖该方法并实现 ``subscribe_execution_reports``。
        """
        return False

    def subscribe_execution_reports(
        self,
        handler: ExecutionReportHandler,
        *,
        account_id: str | None = None,
        broker_order_ids: list[str] | None = None,
        cursor: str | None = None,
    ) -> ExecutionReportSubscription | None:
        """订阅 broker 执行回报。

        Args:
            handler: 回调函数，接收 ``(reports, cursor)``。
            account_id: 账户作用域。
            broker_order_ids: 限定的 broker order id 列表。
            cursor: 上次 supervisor 已消费到的游标；broker 若支持断点续跑，可据此补发遗漏事件。

        Returns:
            ``ExecutionReportSubscription``；若当前 broker 不支持订阅，则返回 ``None``。
        """
        return None

    def poll_execution_reports(
        self,
        *,
        account_id: str | None = None,
        broker_order_ids: list[str] | None = None,
    ) -> list[ExecutionReport]:
        """轮询外部订单状态。

        默认实现基于 ``query_orders`` 构造当前快照级执行回报，供 operator event pump 轮询。
        适配器若能提供 broker 原生事件流，应覆盖该方法返回更细粒度、可递增的执行回报。
        """
        allowed_ids = set(broker_order_ids or [])
        reports: list[ExecutionReport] = []
        for order in self.query_orders_scoped(account_id=account_id):
            if allowed_ids and (order.broker_order_id or order.order_id) not in allowed_ids:
                continue
            reports.append(
                ExecutionReport(
                    report_id=new_id("report"),
                    order_id=order.order_id,
                    trade_date=order.trade_date,
                    status=order.status,
                    requested_quantity=int(order.quantity),
                    filled_quantity=int(order.filled_quantity),
                    remaining_quantity=max(int(order.quantity) - int(order.filled_quantity), 0),
                    message="broker poll snapshot",
                    fill_price=order.avg_fill_price,
                    broker_order_id=order.broker_order_id,
                    account_id=getattr(order, "account_id", None),
                    metadata={"source": "query_orders"},
                )
            )
        return reports


class BrokerBase(SimulatedExecutionPort, ABC):
    """兼容别名。

    历史代码广泛使用 ``BrokerBase``。当前将其固定为研究回测端口别名，
    以便在不破坏导入路径的前提下，把研究/真实 broker 语义逐步拆开。
    """


__all__ = [
    "ConnectionPort",
    "SimulatedExecutionPort",
    "LiveBrokerPort",
    "BrokerBase",
    "ExecutionReportHandler",
    "ExecutionReportSubscription",
]
