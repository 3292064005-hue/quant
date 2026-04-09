"""operator session 推进正式 contract。"""
from __future__ import annotations

from typing import Protocol

from a_share_quant.domain.models import ExecutionReport, Fill, OrderRequest, TradeSessionResult


class SessionAdvancementPort(Protocol):
    """为 supervisor 暴露正式 session 推进 contract。"""

    def list_session_orders(self, session_id: str) -> list[OrderRequest]:
        """返回某个交易会话的正式订单视图。"""

    def derive_broker_event_cursor(self, previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        """从执行回报中推导下一游标。"""

    def sync_session_events(
        self,
        session_id: str,
        *,
        requested_by: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        """使用 poll/query 路径推进会话。"""

    def advance_session_from_reports(
        self,
        session_id: str,
        *,
        reports: list[ExecutionReport],
        external_fills: list[Fill],
        requested_by: str | None = None,
        source: str = "poll",
        broker_event_cursor: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        """基于 broker 回报推进会话。"""
