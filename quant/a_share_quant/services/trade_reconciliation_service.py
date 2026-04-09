"""operator 交易会话恢复与 reconciliation 服务。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import Fill, OrderRequest, OrderSide, OrderStatus, TradeSessionResult, TradeSessionStatus
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.account_repository import AccountRepository


class TradeReconciliationService:
    """恢复本地账本与 broker 外部副作用之间的断裂。

    该服务用于两类场景：
        1. 提交命令过程中 broker 已接单/成交，但本地订单与成交持久化失败；
        2. 进程异常退出后，需要依据 ``ORDER_INTENT_REGISTERED`` 事件重建会话上下文并回补本地账本。
    """

    def __init__(
        self,
        *,
        broker: LiveBrokerPort,
        order_repository: OrderRepository,
        execution_session_repository: ExecutionSessionRepository,
        audit_repository: AuditRepository,
        account_repository: AccountRepository | None = None,
    ) -> None:
        self.broker = broker
        self.order_repository = order_repository
        self.execution_session_repository = execution_session_repository
        self.audit_repository = audit_repository
        self.account_repository = account_repository

    def reconcile_session(
        self,
        session_id: str,
        *,
        expected_orders: list[OrderRequest] | None = None,
        requested_by: str = "system.reconcile",
        failure_reason: str | None = None,
    ) -> TradeSessionResult:
        """按会话标识回补订单/成交并刷新终态。

        Args:
            session_id: 待恢复的会话标识。
            expected_orders: 可选的内存态订单；若为空，则回退到会话 intent 事件重建。
            requested_by: 审计操作者标识。
            failure_reason: 触发恢复的原始错误原因，写入事件与审计链。

        Returns:
            ``TradeSessionResult``，其中 ``summary.status`` 可能是终态，或 ``RECOVERY_REQUIRED``。

        Raises:
            ValueError: 当会话不存在或找不到任何 order intent 时抛出。
            RuntimeError: 当查询 broker 失败且无法完成最小恢复时抛出。
        """
        summary = self.execution_session_repository.get(session_id)
        if summary is None:
            raise ValueError(f"交易会话不存在: {session_id}")
        candidate_orders = list(expected_orders or self._rebuild_orders_from_events(session_id))
        if not candidate_orders:
            raise ValueError(f"交易会话缺少可恢复的订单意图: {session_id}")

        try:
            external_orders = self._query_orders_scoped(summary.account_id)
            external_fills = self._query_trades_scoped(summary.account_id)
            broker_query_error = None
        except Exception as exc:
            external_orders = []
            external_fills = []
            broker_query_error = str(exc)

        recovered_orders = self._merge_external_orders(candidate_orders, external_orders, external_fills)
        recovered_fills = self._match_fills(recovered_orders, external_fills)
        recovered_count = self._count_submitted_orders(recovered_orders)
        rejected_count = sum(1 for order in recovered_orders if order.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED})
        pending_follow_up_count = self._count_pending_follow_up_orders(recovered_orders)

        recovery_payload = {
            "failure_reason": failure_reason,
            "broker_query_error": broker_query_error,
            "recovered_order_count": recovered_count,
            "recovered_fill_count": len(recovered_fills),
        }
        if broker_query_error is None and (recovered_count > 0 or rejected_count > 0):
            final_status = self._resolve_status(recovered_orders)
        else:
            final_status = TradeSessionStatus.RECOVERY_REQUIRED
            recovery_payload["recovery_required"] = True

        with self.order_repository.store.transaction():
            self.order_repository.save_execution_batch(None, recovered_orders, recovered_fills, execution_session_id=session_id)
            self.execution_session_repository.append_event(
                session_id,
                event_type="RECOVERY_RECONCILED" if final_status != TradeSessionStatus.RECOVERY_REQUIRED else "RECOVERY_REQUIRED",
                level="INFO" if final_status != TradeSessionStatus.RECOVERY_REQUIRED else "ERROR",
                payload=recovery_payload,
            )
            risk_summary = dict(summary.risk_summary)
            risk_summary.update(recovery_payload)
            self.execution_session_repository.update_session(
                session_id,
                status=final_status,
                submitted_count=recovered_count,
                rejected_count=rejected_count,
                risk_summary=risk_summary,
                error_message=None if final_status != TradeSessionStatus.RECOVERY_REQUIRED else (broker_query_error or failure_reason or f"存在 {pending_follow_up_count} 笔未终结 broker 订单，需要后续 reconciliation"),
            )

        final_summary = self.execution_session_repository.get(session_id)
        assert final_summary is not None
        events = self.execution_session_repository.list_events(session_id)
        self.audit_repository.write(
            run_id=None,
            trace_id=new_id("trace"),
            module="trade_reconciliation",
            action="session_reconciled" if final_status != TradeSessionStatus.RECOVERY_REQUIRED else "session_recovery_required",
            entity_type="trade_session",
            entity_id=session_id,
            payload={
                "status": final_summary.status.value,
                "recovered_order_count": recovered_count,
                "recovered_fill_count": len(recovered_fills),
                "broker_query_error": broker_query_error,
            },
            operator=requested_by,
            level="INFO" if final_status != TradeSessionStatus.RECOVERY_REQUIRED else "ERROR",
        )
        try:
            self._capture_operator_account_state(session_id, account_id=final_summary.account_id, source="reconcile")
        except Exception as exc:
            self.execution_session_repository.append_event(
                session_id,
                event_type="ACCOUNT_SNAPSHOT_CAPTURE_FAILED",
                level="ERROR",
                payload={"account_id": final_summary.account_id, "source": "reconcile", "error": str(exc)},
            )
        return TradeSessionResult(summary=final_summary, orders=recovered_orders, fills=recovered_fills, events=events, replayed=False)

    def reconcile_latest_recovery_required(self, *, requested_by: str = "system.reconcile") -> TradeSessionResult:
        """恢复最近一个 ``RUNNING`` / ``RECOVERY_REQUIRED`` 会话。"""
        sessions = self.execution_session_repository.list_sessions(
            statuses=[TradeSessionStatus.RECOVERY_REQUIRED, TradeSessionStatus.RUNNING, TradeSessionStatus.CREATED],
            limit=1,
        )
        if not sessions:
            raise ValueError("当前没有待恢复的交易会话")
        return self.reconcile_session(sessions[0].session_id, requested_by=requested_by)

    def _capture_operator_account_state(self, session_id: str, *, account_id: str | None, source: str) -> None:
        if self.account_repository is None:
            return
        summary = self.execution_session_repository.get(session_id)
        if summary is None:
            return
        trade_date = date.fromisoformat(summary.requested_trade_date) if summary.requested_trade_date else date.today()
        getter = getattr(self.broker, "get_account_snapshot", None)
        account = getter(account_id=account_id, last_prices=None) if callable(getter) else self.broker.get_account(last_prices=None)
        pos_getter = getattr(self.broker, "get_position_snapshots", None)
        positions = list(pos_getter(account_id=account_id, last_prices=None)) if callable(pos_getter) else list(self.broker.get_positions(last_prices=None))
        capture_id = self.account_repository.save_operator_account_snapshot(
            session_id,
            trade_date,
            account,
            account_id=account_id,
            source=source,
        )
        self.account_repository.save_operator_position_snapshots(
            session_id,
            trade_date,
            positions,
            account_id=account_id,
            source=source,
            capture_id=capture_id,
        )
        self.execution_session_repository.append_event(
            session_id,
            event_type="ACCOUNT_SNAPSHOT_CAPTURED",
            level="INFO",
            payload={"account_id": account_id, "source": source, "capture_id": capture_id, "position_count": len(positions)},
        )

    def _query_orders_scoped(self, account_id: str | None) -> list[OrderRequest]:
        query_scoped = getattr(self.broker, "query_orders_scoped", None)
        if callable(query_scoped):
            return list(query_scoped(account_id=account_id))
        orders = list(self.broker.query_orders())
        if account_id is None:
            return orders
        return [item for item in orders if getattr(item, "account_id", None) in {None, "", account_id}]

    def _query_trades_scoped(self, account_id: str | None) -> list[Fill]:
        query_scoped = getattr(self.broker, "query_trades_scoped", None)
        if callable(query_scoped):
            return list(query_scoped(account_id=account_id))
        fills = list(self.broker.query_trades())
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]

    def _rebuild_orders_from_events(self, session_id: str) -> list[OrderRequest]:
        events = self.execution_session_repository.list_events(session_id, limit=500)
        rebuilt: list[OrderRequest] = []
        for event in events:
            if event.event_type != "ORDER_INTENT_REGISTERED":
                continue
            payload = event.payload
            rebuilt.append(
                OrderRequest(
                    order_id=str(payload["order_id"]),
                    trade_date=date.fromisoformat(str(payload["trade_date"])),
                    strategy_id=str(payload["strategy_id"]),
                    ts_code=str(payload["ts_code"]),
                    side=OrderSide(str(payload["side"])),
                    price=float(payload["price"]),
                    quantity=int(payload["quantity"]),
                    reason=str(payload["reason"]),
                    status=OrderStatus(str(payload.get("status") or OrderStatus.CREATED.value)),
                    broker_order_id=payload.get("broker_order_id"),
                    filled_quantity=int(payload.get("filled_quantity") or 0),
                    avg_fill_price=float(payload["avg_fill_price"]) if payload.get("avg_fill_price") is not None else None,
                    last_error=payload.get("last_error"),
                    account_id=payload.get("account_id"),
                )
            )
        return rebuilt

    def _merge_external_orders(
        self,
        candidate_orders: list[OrderRequest],
        external_orders: list[OrderRequest],
        external_fills: list[Fill],
    ) -> list[OrderRequest]:
        by_order_id = {item.order_id: item for item in external_orders}
        by_broker_order_id = {item.broker_order_id: item for item in external_orders if item.broker_order_id}
        by_signature: dict[tuple[str, str, str, int, float], list[OrderRequest]] = defaultdict(list)
        for item in external_orders:
            by_signature[self._order_signature(item)].append(item)
        merged: list[OrderRequest] = []
        for order in candidate_orders:
            external_order = by_order_id.get(order.order_id)
            if external_order is None and order.broker_order_id:
                external_order = by_broker_order_id.get(order.broker_order_id)
            if external_order is None:
                signature_matches = by_signature.get(self._order_signature(order), [])
                if len(signature_matches) == 1:
                    external_order = signature_matches[0]
            if external_order is not None:
                order.status = external_order.status
                resolved_broker_order_id = external_order.broker_order_id
                if not resolved_broker_order_id and external_order.order_id != order.order_id:
                    resolved_broker_order_id = external_order.order_id
                order.broker_order_id = resolved_broker_order_id or order.broker_order_id
                order.account_id = getattr(external_order, "account_id", None) or order.account_id
                order.filled_quantity = external_order.filled_quantity
                order.avg_fill_price = external_order.avg_fill_price
                order.last_error = external_order.last_error
            matched_fills = self._fills_for_order(order, external_fills)
            if matched_fills:
                total_quantity = sum(item.fill_quantity for item in matched_fills)
                total_notional = sum(item.fill_quantity * item.fill_price for item in matched_fills)
                order.filled_quantity = min(total_quantity, order.quantity)
                order.avg_fill_price = total_notional / total_quantity if total_quantity > 0 else order.avg_fill_price
                order.status = OrderStatus.FILLED if order.filled_quantity >= order.quantity else OrderStatus.PARTIALLY_FILLED
            merged.append(order)
        return merged

    def _match_fills(self, candidate_orders: list[OrderRequest], external_fills: list[Fill]) -> list[Fill]:
        matched: list[Fill] = []
        seen_fill_ids: set[str] = set()
        for order in candidate_orders:
            for fill in self._fills_for_order(order, external_fills):
                if fill.fill_id in seen_fill_ids:
                    continue
                matched.append(
                    Fill(
                        fill_id=fill.fill_id,
                        order_id=order.order_id,
                        trade_date=fill.trade_date,
                        ts_code=fill.ts_code,
                        side=fill.side,
                        fill_price=fill.fill_price,
                        fill_quantity=fill.fill_quantity,
                        fee=fill.fee,
                        tax=fill.tax,
                        run_id=fill.run_id,
                        broker_order_id=fill.broker_order_id or (fill.order_id if fill.order_id != order.order_id else order.broker_order_id),
                        account_id=fill.account_id or order.account_id,
                    )
                )
                seen_fill_ids.add(fill.fill_id)
        return matched

    @staticmethod
    def _order_signature(order: OrderRequest) -> tuple[str, str, str, int, float]:
        return (
            order.trade_date.isoformat(),
            order.ts_code,
            order.side.value,
            int(order.quantity),
            round(float(order.price), 6),
        )

    def _fills_for_order(self, order: OrderRequest, external_fills: list[Fill]) -> list[Fill]:
        matched: list[Fill] = []
        for fill in external_fills:
            if fill.order_id == order.order_id:
                matched.append(fill)
                continue
            if order.broker_order_id and (fill.order_id == order.broker_order_id or fill.broker_order_id == order.broker_order_id):
                matched.append(fill)
                continue
            if self._fill_matches_signature(order, fill):
                matched.append(fill)
        return matched

    @staticmethod
    def _fill_matches_signature(order: OrderRequest, fill: Fill) -> bool:
        return (
            fill.trade_date == order.trade_date
            and fill.ts_code == order.ts_code
            and fill.side == order.side
            and int(fill.fill_quantity) == int(order.quantity)
            and round(float(fill.fill_price), 6) == round(float(order.price), 6)
        )

    @staticmethod
    def _count_submitted_orders(orders: list[OrderRequest]) -> int:
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
    def _count_pending_follow_up_orders(orders: list[OrderRequest]) -> int:
        pending_statuses = {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PENDING_CANCEL,
        }
        return len([order for order in orders if order.status in pending_statuses])

    def _resolve_status(self, orders: list[OrderRequest]) -> TradeSessionStatus:
        order_count = len(orders)
        rejected_count = sum(1 for order in orders if order.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED})
        filled_count = sum(1 for order in orders if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED})
        pending_follow_up_count = self._count_pending_follow_up_orders(orders)
        if order_count <= 0:
            return TradeSessionStatus.RECOVERY_REQUIRED
        if rejected_count >= order_count:
            return TradeSessionStatus.REJECTED
        if pending_follow_up_count > 0:
            return TradeSessionStatus.RECOVERY_REQUIRED
        if filled_count > 0 and rejected_count > 0:
            return TradeSessionStatus.PARTIALLY_COMPLETED
        if filled_count > 0:
            return TradeSessionStatus.COMPLETED
        return TradeSessionStatus.RECOVERY_REQUIRED
