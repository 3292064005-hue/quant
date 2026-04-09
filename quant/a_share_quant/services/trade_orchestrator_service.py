"""paper/live 正式交易编排服务。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import (
    AccountSnapshot,
    Bar,
    ExecutionReport,
    Fill,
    LiveOrderSubmission,
    OrderRequest,
    OrderSide,
    OrderStatus,
    PositionSnapshot,
    RiskResult,
    Security,
    TradeCommandEvent,
    TradeSessionResult,
    TradeSessionStatus,
)
from a_share_quant.engines.risk_engine import RiskEngine
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.risk_service import RiskService
from a_share_quant.services.trade_reconciliation_service import TradeReconciliationService


class TradeOrchestratorService:
    """为 paper/live lane 提供正式命令编排、幂等、审计与恢复入口。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        risk_engine: RiskEngine | None = None,
        market_repository: MarketRepository,
        order_repository: OrderRepository,
        audit_repository: AuditRepository,
        execution_session_repository: ExecutionSessionRepository,
        reconciliation_service: TradeReconciliationService,
        account_repository: AccountRepository | None = None,
        execution_contract_service: SharedExecutionContractService | None = None,
    ) -> None:
        self.config = config
        self.broker = broker
        self.risk_engine = risk_engine or RiskService(config.risk, config.backtest).build_engine()
        self.market_repository = market_repository
        self.order_repository = order_repository
        self.audit_repository = audit_repository
        self.execution_session_repository = execution_session_repository
        self.reconciliation_service = reconciliation_service
        self.account_repository = account_repository
        self.execution_contract_service = execution_contract_service or SharedExecutionContractService()

    def submit_orders(
        self,
        orders: list[OrderRequest],
        *,
        command_source: str,
        requested_by: str | None = None,
        idempotency_key: str | None = None,
        approved: bool = False,
        account_id: str | None = None,
    ) -> TradeSessionResult:
        """提交 operator 订单批次。

        Args:
            orders: 待提交订单，允许同会话多笔。
            command_source: 命令来源，用于审计与回滚定位。
            requested_by: 操作者标识。
            idempotency_key: 幂等键；重复提交时返回已有会话结果。
            approved: 当配置要求人工批准时必须显式为 True。

        Returns:
            ``TradeSessionResult``。当本地持久化失败但恢复链成功回补时，也会返回恢复后的正式结果。

        Raises:
            ValueError: lane 非 paper/live、订单非法、或风控前置校验失败。
            RuntimeError: broker 心跳失败、命令链失败，或恢复链仍无法确认最终状态。
        """
        if self.config.app.runtime_mode not in {"paper_trade", "live_trade"}:
            raise ValueError(
                f"TradeOrchestratorService 仅支持 paper/live lane；收到 app.runtime_mode={self.config.app.runtime_mode}"
            )
        if not orders:
            raise ValueError("operator trade 提交不能为空")
        if len(orders) > self.config.operator.max_batch_orders:
            raise ValueError(
                f"单次 operator trade 数量超过上限: {len(orders)} > {self.config.operator.max_batch_orders}"
            )
        if self.config.operator.require_approval and not approved:
            raise ValueError("当前配置要求 operator 显式批准；请传入 approved=True")
        if self.config.risk.kill_switch:
            raise ValueError("risk.kill_switch 已开启，禁止提交 operator trade")
        if idempotency_key:
            existing = self.execution_session_repository.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return TradeSessionResult(
                    summary=existing,
                    orders=self._list_session_orders(existing.session_id),
                    fills=self._list_session_fills(existing.session_id),
                    events=self.execution_session_repository.list_events(existing.session_id),
                    replayed=True,
                )
        if not self.broker.heartbeat():
            raise RuntimeError("broker heartbeat 失败，禁止提交 operator trade")

        requested_by = (requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by
        resolved_account_id = self._resolve_account_id(account_id)
        self._bind_orders_account_id(orders, resolved_account_id)
        self._normalize_operator_order_ids(orders)
        trade_date = self._resolve_trade_date(orders)
        risk_summary, accepted_orders, rejected_orders = self._pre_trade_validate(orders)
        summary = self.execution_session_repository.create_session(
            runtime_mode=self.config.app.runtime_mode,
            broker_provider=self.config.broker.provider,
            command_type="submit_orders",
            command_source=command_source,
            requested_by=requested_by,
            requested_trade_date=trade_date.isoformat(),
            idempotency_key=idempotency_key,
            risk_summary=risk_summary,
            order_count=len(orders),
            status=TradeSessionStatus.RUNNING,
            account_id=resolved_account_id,
        )
        events: list[TradeCommandEvent] = []
        fills: list[Fill] = []
        try:
            self._persist_order_intents(summary.session_id, orders, rejected_orders)
            self.order_repository.save_orders(None, orders, execution_session_id=summary.session_id)
            if accepted_orders:
                events.append(
                    self.execution_session_repository.append_event(
                        summary.session_id,
                        event_type="BROKER_SUBMISSION_STARTED",
                        level="INFO",
                        payload={"accepted_order_count": len(accepted_orders), "account_id": resolved_account_id},
                    )
                )
            for index, order in enumerate(accepted_orders):
                try:
                    submission = self._submit_order_lifecycle(order)
                    fills.extend(submission.fills)
                    events.extend(self._record_submission_events(summary.session_id, order, submission, sequence=index))
                    self._apply_submission_to_order(order, submission)
                except Exception as exc:
                    order.mark_rejected(OrderStatus.EXECUTION_REJECTED, str(exc))
                    events.append(
                        self.execution_session_repository.append_event(
                            summary.session_id,
                            event_type="ORDER_SUBMIT_FAILED",
                            level="ERROR",
                            payload={"order_id": order.order_id, "error": str(exc), "sequence": index},
                        )
                    )
                    if self.config.operator.fail_fast:
                        break
            self.order_repository.save_execution_batch(None, orders, fills, execution_session_id=summary.session_id)
            rejected_count = self._count_rejected_orders(orders)
            submitted_count = self._count_submitted_orders(orders)
            pending_follow_up_count = self._count_pending_follow_up_orders(orders)
            final_status = self._resolve_session_status(orders)
            final_error_message = self._resolve_final_error_message(
                final_status,
                risk_summary=risk_summary,
                pending_follow_up_count=pending_follow_up_count,
            )
            self.execution_session_repository.update_session(
                summary.session_id,
                status=final_status,
                submitted_count=submitted_count,
                rejected_count=rejected_count,
                risk_summary=risk_summary,
                error_message=final_error_message,
            )
            final_summary = self.execution_session_repository.get(summary.session_id)
            assert final_summary is not None
            self.audit_repository.write(
                run_id=None,
                trace_id=new_id("trace"),
                module="trade_orchestrator",
                action="session_completed",
                entity_type="trade_session",
                entity_id=final_summary.session_id,
                payload={
                    "status": final_summary.status.value,
                    "risk_summary": final_summary.risk_summary,
                    "submitted_count": final_summary.submitted_count,
                    "rejected_count": final_summary.rejected_count,
                    "account_id": final_summary.account_id,
                },
                operator=requested_by,
                level="INFO" if final_summary.status in {TradeSessionStatus.COMPLETED, TradeSessionStatus.PARTIALLY_COMPLETED} else "ERROR",
            )
            self._capture_operator_account_state(final_summary.session_id, account_id=final_summary.account_id, source="submit_orders")
            return TradeSessionResult(summary=final_summary, orders=list(orders), fills=fills, events=events, replayed=False)
        except Exception as exc:
            terminal_message = str(exc)
            recovered = self._attempt_recovery(
                summary.session_id,
                orders=orders,
                submitted_orders=orders,
                fills=fills,
                requested_by=requested_by,
                terminal_message=terminal_message,
            )
            if recovered is not None:
                return recovered
            risk_summary = dict(risk_summary)
            risk_summary.setdefault("terminal_error", terminal_message)
            recovery_status = TradeSessionStatus.RECOVERY_REQUIRED if fills or any(order.broker_order_id for order in orders) else TradeSessionStatus.FAILED
            try:
                self.execution_session_repository.append_event(
                    summary.session_id,
                    event_type="RECOVERY_REQUIRED" if recovery_status == TradeSessionStatus.RECOVERY_REQUIRED else "SESSION_ABORTED",
                    level="ERROR",
                    payload={"error": terminal_message, "recovery_required": recovery_status == TradeSessionStatus.RECOVERY_REQUIRED},
                )
                self.execution_session_repository.update_session(
                    summary.session_id,
                    status=recovery_status,
                    submitted_count=self._count_submitted_orders(orders),
                    rejected_count=self._count_rejected_orders(orders),
                    risk_summary=risk_summary,
                    error_message=terminal_message,
                )
                self.audit_repository.write(
                    run_id=None,
                    trace_id=new_id("trace"),
                    module="trade_orchestrator",
                    action="session_failed" if recovery_status == TradeSessionStatus.FAILED else "session_recovery_required",
                    entity_type="trade_session",
                    entity_id=summary.session_id,
                    payload={
                        "error": terminal_message,
                        "submitted_count": self._count_submitted_orders(orders),
                        "rejected_count": self._count_rejected_orders(orders),
                        "account_id": resolved_account_id,
                    },
                    operator=requested_by,
                    level="ERROR",
                )
            except Exception:
                pass
            raise

    def reconcile_session(self, session_id: str, *, requested_by: str | None = None) -> TradeSessionResult:
        """显式触发某个交易会话的 reconciliation。"""
        return self.reconciliation_service.reconcile_session(
            session_id,
            requested_by=(requested_by or self.config.operator.default_requested_by),
        )

    def reconcile_latest_recovery_required(self, *, requested_by: str | None = None) -> TradeSessionResult:
        """恢复最近一个待恢复会话。"""
        return self.reconciliation_service.reconcile_latest_recovery_required(
            requested_by=(requested_by or self.config.operator.default_requested_by),
        )

    def sync_session_events(
        self,
        session_id: str,
        *,
        requested_by: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionResult:
        """轮询 broker 当前状态并把会话推进到最新正式状态。

        该入口用于 paper/live lane 的事件泵：即使 broker 只提供 query_orders/query_trades，
        也可以通过重复轮询把 ACCEPTED/PARTIALLY_FILLED/FILLED 逐步收口到本地账本。
        """
        session = self.execution_session_repository.get(session_id)
        if session is None:
            raise ValueError(f"未找到交易会话: {session_id}")
        orders = self.list_session_orders(session_id)
        if not orders:
            raise ValueError(f"交易会话 {session_id} 不存在可同步订单")
        broker_ids = [item.broker_order_id or item.order_id for item in orders if item.broker_order_id or item.order_id]
        reports = self.broker.poll_execution_reports(account_id=session.account_id, broker_order_ids=broker_ids)
        external_fills = self._query_trades_scoped(session.account_id)
        broker_event_cursor = self.derive_broker_event_cursor(session.broker_event_cursor, reports)
        return self.advance_session_from_reports(
            session_id,
            reports=reports,
            external_fills=external_fills,
            requested_by=requested_by,
            source="poll",
            broker_event_cursor=broker_event_cursor,
            supervisor_mode=supervisor_mode,
        )

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
        """基于一批 broker 执行回报推进本地会话。

        Args:
            session_id: 待推进会话 ID。
            reports: 本次收到的正式执行回报；允许为空，表示只做 fill/backfill 收口。
            external_fills: 当前可见成交列表。
            requested_by: 操作者/监督者标识。
            source: 推进来源，例如 ``poll`` / ``subscription``。
            broker_event_cursor: broker 事件游标；当提供时会写回会话摘要。
            supervisor_mode: supervisor 使用的推进模式，例如 ``poll`` / ``subscription``。

        Returns:
            ``TradeSessionResult``。

        Raises:
            ValueError: 当会话不存在或不存在可推进订单时抛出。
        """
        session = self.execution_session_repository.get(session_id)
        if session is None:
            raise ValueError(f"未找到交易会话: {session_id}")
        requested_by = (requested_by or self.config.operator.default_requested_by).strip() or self.config.operator.default_requested_by
        orders = self._list_session_orders(session_id)
        if not orders:
            raise ValueError(f"交易会话 {session_id} 不存在可同步订单")
        new_fills, _events = self._apply_polled_progress(session_id, orders=orders, reports=reports, external_fills=external_fills)
        self.order_repository.save_execution_batch(None, orders, new_fills, execution_session_id=session_id)
        final_status = self._resolve_session_status(orders)
        risk_summary = dict(session.risk_summary)
        risk_summary.update({
            "last_sync_source": source,
            "last_sync_report_count": len(reports),
            "last_sync_new_fill_count": len(new_fills),
        })
        pending_follow_up_count = self._count_pending_follow_up_orders(orders)
        self.execution_session_repository.update_session(
            session_id,
            status=final_status,
            submitted_count=self._count_submitted_orders(orders),
            rejected_count=self._count_rejected_orders(orders),
            risk_summary=risk_summary,
            error_message=self._resolve_final_error_message(final_status, risk_summary=risk_summary, pending_follow_up_count=pending_follow_up_count),
            broker_event_cursor=broker_event_cursor,
            last_synced_at=self._now_iso(),
            supervisor_mode=supervisor_mode,
            last_supervised_at=self._now_iso() if supervisor_mode else None,
        )
        self.execution_session_repository.append_event(
            session_id,
            event_type="SESSION_SYNC_COMPLETED",
            level="INFO",
            payload={
                "source": source,
                "report_count": len(reports),
                "new_fill_count": len(new_fills),
                "account_id": session.account_id,
                "broker_event_cursor": broker_event_cursor,
                "supervisor_mode": supervisor_mode,
            },
        )
        final_summary = self.execution_session_repository.get(session_id)
        assert final_summary is not None
        self.audit_repository.write(
            run_id=None,
            trace_id=new_id("trace"),
            module="trade_orchestrator",
            action="session_synced",
            entity_type="trade_session",
            entity_id=session_id,
            payload={
                "status": final_summary.status.value,
                "source": source,
                "report_count": len(reports),
                "new_fill_count": len(new_fills),
                "account_id": session.account_id,
                "broker_event_cursor": broker_event_cursor,
                "supervisor_mode": supervisor_mode,
            },
            operator=requested_by,
            level="INFO",
        )
        self._capture_operator_account_state(session_id, account_id=session.account_id, source=f"sync:{source}")
        return TradeSessionResult(summary=final_summary, orders=orders, fills=new_fills, events=self.execution_session_repository.list_events(session_id), replayed=False)

    def sync_latest_open_session(self, *, requested_by: str | None = None) -> TradeSessionResult:
        """同步最近一个仍需后续事件推进的会话。"""
        sessions = self.execution_session_repository.list_sessions(
            statuses=[TradeSessionStatus.RUNNING, TradeSessionStatus.RECOVERY_REQUIRED],
            limit=1,
        )
        if not sessions:
            raise ValueError("当前没有需要同步事件的交易会话")
        return self.sync_session_events(sessions[0].session_id, requested_by=requested_by)

    @staticmethod
    def _now_iso() -> str:
        from a_share_quant.core.utils import now_iso
        return now_iso()

    def _query_trades_scoped(self, account_id: str | None) -> list[Fill]:
        query_scoped = getattr(self.broker, "query_trades_scoped", None)
        if callable(query_scoped):
            return list(query_scoped(account_id=account_id))
        trades = list(self.broker.query_trades())
        if account_id is None:
            return trades
        return [item for item in trades if getattr(item, "account_id", None) in {None, "", account_id}]


    def _capture_operator_account_state(self, session_id: str, *, account_id: str | None, source: str) -> None:
        """采样并持久化 operator 账户/持仓快照。

        Boundary Behavior:
            - 快照采样失败不会破坏 submit/sync 主链；
            - 失败会写入正式 session event 与 audit，避免静默丢失观测。
        """
        if self.account_repository is None:
            return
        session = self.execution_session_repository.get(session_id)
        if session is None:
            return
        trade_date = date.fromisoformat(session.requested_trade_date) if session.requested_trade_date else date.today()
        try:
            account = self._get_account_snapshot_scoped(account_id)
            positions = self._get_positions_scoped(account_id)
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
        except Exception as exc:
            self.execution_session_repository.append_event(
                session_id,
                event_type="ACCOUNT_SNAPSHOT_CAPTURE_FAILED",
                level="ERROR",
                payload={"account_id": account_id, "source": source, "error": str(exc)},
            )
            self.audit_repository.write(
                run_id=None,
                trace_id=new_id("trace"),
                module="trade_orchestrator",
                action="operator_account_snapshot_failed",
                entity_type="trade_session",
                entity_id=session_id,
                payload={"account_id": account_id, "source": source, "error": str(exc)},
                operator=self.config.operator.default_requested_by,
                level="ERROR",
            )

    def _get_account_snapshot_scoped(self, account_id: str | None) -> AccountSnapshot:
        getter = getattr(self.broker, "get_account_snapshot", None)
        if callable(getter):
            return getter(account_id=account_id, last_prices=None)
        return self.broker.get_account(last_prices=None)

    def _get_positions_scoped(self, account_id: str | None) -> list[PositionSnapshot]:
        getter = getattr(self.broker, "get_position_snapshots", None)
        if callable(getter):
            return list(getter(account_id=account_id, last_prices=None))
        return list(self.broker.get_positions(last_prices=None))

    def _resolve_account_id(self, account_id: str | None) -> str | None:
        candidate = (account_id or self.config.broker.account_id or "").strip() or None
        allowed = set(self.config.broker.allowed_account_ids or [])
        if self.config.broker.account_id:
            allowed.add(self.config.broker.account_id)
        if candidate is not None and allowed and candidate not in allowed:
            raise ValueError(f"account_id 不在允许列表内: {candidate}; allowed={sorted(allowed)}")
        return candidate

    @staticmethod
    def _bind_orders_account_id(orders: list[OrderRequest], account_id: str | None) -> None:
        for order in orders:
            if order.account_id and account_id and order.account_id != account_id:
                raise ValueError(f"订单 account_id 与本次提交账户不一致: {order.order_id} {order.account_id} != {account_id}")
            order.account_id = order.account_id or account_id

    @staticmethod
    def derive_broker_event_cursor(previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        """从执行回报中提取新的 broker 事件游标。"""
        cursor = previous_cursor
        for report in reports:
            metadata_cursor = report.metadata.get("cursor") if isinstance(report.metadata, dict) else None
            cursor = str(metadata_cursor) if metadata_cursor else report.report_id or cursor
        return cursor

    def _apply_polled_progress(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        reports: list[ExecutionReport],
        external_fills: list[Fill],
    ) -> tuple[list[Fill], list[TradeCommandEvent]]:
        existing_fill_ids = {row["fill_id"] for row in self.order_repository.list_fills(execution_session_id=session_id, limit=2000)}
        report_map: dict[str, list[ExecutionReport]] = defaultdict(list)
        order_by_broker_id = {item.broker_order_id: item for item in orders if item.broker_order_id}
        for report in reports:
            matched_order = next((item for item in orders if report.order_id == item.order_id), None)
            if matched_order is None and report.broker_order_id:
                matched_order = order_by_broker_id.get(report.broker_order_id)
            if matched_order is None:
                continue
            if report.account_id is None:
                report.account_id = matched_order.account_id
            report_map[matched_order.order_id].append(report)
        new_fills: list[Fill] = []
        events: list[TradeCommandEvent] = []
        for order in orders:
            previous_status = order.status
            previous_filled = int(order.filled_quantity)
            previous_broker_order_id = order.broker_order_id
            latest_report = report_map.get(order.order_id, [])[-1] if report_map.get(order.order_id) else None
            if latest_report is not None:
                order.account_id = order.account_id or latest_report.account_id
                if latest_report.broker_order_id:
                    order.broker_order_id = latest_report.broker_order_id
                if latest_report.status == OrderStatus.ACCEPTED:
                    order.mark_accepted(order.broker_order_id)
                elif latest_report.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
                    order.status = latest_report.status
                    order.filled_quantity = max(int(latest_report.filled_quantity), order.filled_quantity)
                    if latest_report.fill_price is not None:
                        order.avg_fill_price = latest_report.fill_price
                elif latest_report.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}:
                    order.mark_rejected(latest_report.status, latest_report.message or "broker rejected order")
            matched_fills = []
            for fill in external_fills:
                if order.account_id and fill.account_id not in {None, '', order.account_id}:
                    continue
                if fill.order_id == order.order_id or (order.broker_order_id and fill.broker_order_id == order.broker_order_id) or (order.broker_order_id and fill.order_id == order.broker_order_id):
                    matched_fills.append(fill)
            total_quantity = sum(int(item.fill_quantity) for item in matched_fills)
            if matched_fills and total_quantity >= order.filled_quantity:
                order.filled_quantity = min(total_quantity, order.quantity)
                total_notional = sum(float(item.fill_price) * int(item.fill_quantity) for item in matched_fills)
                order.avg_fill_price = total_notional / total_quantity if total_quantity > 0 else order.avg_fill_price
                order.status = OrderStatus.FILLED if order.filled_quantity >= order.quantity else OrderStatus.PARTIALLY_FILLED
            for fill in matched_fills:
                fill.account_id = fill.account_id or order.account_id
                if fill.fill_id in existing_fill_ids:
                    continue
                fill.order_id = order.order_id
                new_fills.append(fill)
                existing_fill_ids.add(fill.fill_id)
                events.append(self.execution_session_repository.append_event(session_id, event_type="ORDER_FILLED" if order.status == OrderStatus.FILLED else "ORDER_PARTIALLY_FILLED", level="INFO", payload={"order_id": order.order_id, "fill_id": fill.fill_id, "fill_quantity": fill.fill_quantity, "fill_price": fill.fill_price, "broker_order_id": fill.broker_order_id, "account_id": fill.account_id}))
            if order.broker_order_id and order.broker_order_id != previous_broker_order_id:
                events.append(self.execution_session_repository.append_event(session_id, event_type="ORDER_ACCEPTED", level="INFO", payload={"order_id": order.order_id, "broker_order_id": order.broker_order_id, "account_id": order.account_id}))
            if latest_report is not None:
                events.append(self.execution_session_repository.append_event(session_id, event_type="EXECUTION_REPORT", level="INFO" if latest_report.status not in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED} else "ERROR", payload={"order_id": order.order_id, "report_id": latest_report.report_id, "status": latest_report.status.value, "filled_quantity": latest_report.filled_quantity, "remaining_quantity": latest_report.remaining_quantity, "broker_order_id": latest_report.broker_order_id, "account_id": latest_report.account_id}))
            if order.status != previous_status or int(order.filled_quantity) != previous_filled:
                event_type = {OrderStatus.ACCEPTED: "ORDER_ACCEPTED", OrderStatus.PARTIALLY_FILLED: "ORDER_PARTIALLY_FILLED", OrderStatus.FILLED: "ORDER_FILLED", OrderStatus.PRE_TRADE_REJECTED: "ORDER_REJECTED", OrderStatus.EXECUTION_REJECTED: "ORDER_REJECTED", OrderStatus.REJECTED: "ORDER_REJECTED"}.get(order.status, "ORDER_REPORT_RECEIVED")
                level = "INFO" if order.status not in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED} else "ERROR"
                events.append(self.execution_session_repository.append_event(session_id, event_type=event_type, level=level, payload={"order_id": order.order_id, "status": order.status.value, "filled_quantity": order.filled_quantity, "avg_fill_price": order.avg_fill_price, "broker_order_id": order.broker_order_id, "account_id": order.account_id}))
        return new_fills, events

    def _attempt_recovery(
        self,
        session_id: str,
        *,
        orders: list[OrderRequest],
        submitted_orders: list[OrderRequest],
        fills: list[Fill],
        requested_by: str,
        terminal_message: str,
    ) -> TradeSessionResult | None:
        if not submitted_orders and not fills:
            return None
        try:
            return self.reconciliation_service.reconcile_session(
                session_id,
                expected_orders=list(submitted_orders or orders),
                requested_by=requested_by,
                failure_reason=terminal_message,
            )
        except Exception:
            return None

    def _persist_order_intents(
        self,
        session_id: str,
        orders: list[OrderRequest],
        rejected_orders: list[OrderRequest],
    ) -> None:
        """在进入 broker side effect 之前先落持久化命令意图。"""
        self.execution_session_repository.append_event(session_id, event_type="SESSION_CREATED", level="INFO", payload={"order_count": len(orders)})
        rejected_ids = {item.order_id for item in rejected_orders}
        for order in orders:
            self.execution_session_repository.append_event(
                session_id,
                event_type="ORDER_INTENT_REGISTERED",
                level="INFO",
                payload=self._order_to_event_payload(order),
            )
            if order.order_id in rejected_ids:
                self.execution_session_repository.append_event(
                    session_id,
                    event_type="ORDER_REJECTED_PRE_TRADE",
                    level="WARNING",
                    payload={"order_id": order.order_id, "reason": order.last_error, "ts_code": order.ts_code},
                )

    def _record_submission_events(
        self,
        session_id: str,
        order: OrderRequest,
        submission: LiveOrderSubmission,
        *,
        sequence: int,
    ) -> list[TradeCommandEvent]:
        """把 broker 生命周期结果写入会话事件流。

        Args:
            session_id: 交易会话 ID。
            order: 本地领域订单。
            submission: broker 生命周期结果。
            sequence: 该订单在批次中的顺序。

        Returns:
            当前订单新增的事件列表。
        """
        events: list[TradeCommandEvent] = []
        ticket = submission.ticket
        if ticket.broker_order_id:
            events.append(
                self.execution_session_repository.append_event(
                    session_id,
                    event_type="ORDER_TICKET_RECEIVED",
                    level="INFO",
                    payload={
                        "order_id": order.order_id,
                        "broker_order_id": ticket.broker_order_id,
                        "status": ticket.status.value,
                        "requested_quantity": ticket.requested_quantity,
                        "sequence": sequence,
                    },
                )
            )
        for report in submission.reports:
            event_type = "ORDER_ACCEPTED" if report.status == OrderStatus.ACCEPTED else "ORDER_REPORT_RECEIVED"
            level = "INFO" if report.status not in {OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED} else "ERROR"
            events.append(
                self.execution_session_repository.append_event(
                    session_id,
                    event_type=event_type,
                    level=level,
                    payload={
                        "order_id": order.order_id,
                        "report_id": report.report_id,
                        "status": report.status.value,
                        "message": report.message,
                        "filled_quantity": report.filled_quantity,
                        "remaining_quantity": report.remaining_quantity,
                        "broker_order_id": report.broker_order_id,
                        "sequence": sequence,
                    },
                )
            )
        for fill in submission.fills:
            events.append(
                self.execution_session_repository.append_event(
                    session_id,
                    event_type="ORDER_FILLED",
                    level="INFO",
                    payload={
                        "order_id": order.order_id,
                        "fill_id": fill.fill_id,
                        "fill_quantity": fill.fill_quantity,
                        "fill_price": fill.fill_price,
                        "broker_order_id": fill.broker_order_id,
                        "sequence": sequence,
                    },
                )
            )
        return events

    def _submit_order_lifecycle(self, order: OrderRequest) -> LiveOrderSubmission:
        """兼容调用 broker 生命周期接口。

        Boundary Behavior:
            - 优先使用具备正式 ``submit_order_lifecycle`` 契约的 broker；
            - 若注入对象仍停留在历史 ``submit_order`` 同步成交接口，则在服务层回填最小 ticket/report 聚合，
              以避免直接注入的测试 broker 或手写 adapter 因缺少新方法而整体失效。
        """
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
        ticket = self._build_submission_ticket(
            order,
            broker_order_id=broker_order_id,
            filled_quantity=filled_quantity,
            fill_price=float(fill.fill_price),
            status=final_status,
        )
        return LiveOrderSubmission(ticket=ticket, reports=[accepted_report, final_report], fills=[fill])

    @staticmethod
    def _build_submission_ticket(
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

    def _apply_submission_to_order(self, order: OrderRequest, submission: LiveOrderSubmission) -> None:
        """把 broker 生命周期结果回放到本地订单对象。

        Args:
            order: 待更新订单。
            submission: broker 返回的 ticket/report/fill 聚合结果。

        Boundary Behavior:
            - 若 ``fills`` 存在，以成交明细为最终真相源推进 ``filled_quantity`` / ``avg_fill_price``；
            - 若仅有 ``reports`` / ``ticket``，则退化为基于快照推进订单状态；
            - 若 broker 已返回 ``broker_order_id``，则始终优先回填到本地订单，便于后续 reconciliation。
        """
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
    def _count_rejected_orders(orders: list[OrderRequest]) -> int:
        """统计正式拒单数量。"""
        return len([order for order in orders if order.status in {OrderStatus.PRE_TRADE_REJECTED, OrderStatus.EXECUTION_REJECTED, OrderStatus.REJECTED}])

    @staticmethod
    def _count_submitted_orders(orders: list[OrderRequest]) -> int:
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
    def _count_pending_follow_up_orders(orders: list[OrderRequest]) -> int:
        """统计仍需后续 reconciliation/backfill 的订单数量。"""
        pending_statuses = {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PENDING_CANCEL,
        }
        return len([order for order in orders if order.status in pending_statuses])

    @staticmethod
    def _resolve_final_error_message(
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

    @staticmethod
    def _order_to_event_payload(order: OrderRequest) -> dict[str, Any]:
        payload = asdict(order)
        payload["trade_date"] = order.trade_date.isoformat()
        payload["side"] = order.side.value
        payload["status"] = order.status.value
        payload["order_type"] = order.order_type.value
        payload["time_in_force"] = order.time_in_force.value
        return payload

    def _normalize_operator_order_ids(self, orders: list[OrderRequest]) -> None:
        """为 operator 提交链规范化内部订单 ID。

        Args:
            orders: 待提交订单列表。

        Boundary Behavior:
            - ``order_id`` 在 operator lane 中视为内部领域标识，而不是外部稳定主键；
            - 当调用方未提供 ``order_id``、同批次重复，或与库内既有订单冲突时，会重签发新的 ``operator_order`` 前缀 ID；
            - 该步骤在会话创建与持久化之前执行，避免不同入口把审计完整性寄托在 CLI 上。
        """
        assigned_ids: set[str] = set()
        for order in orders:
            candidate = str(order.order_id).strip() if order.order_id else ""
            needs_reissue = not candidate or candidate in assigned_ids or self.order_repository.get_order_by_id(candidate) is not None
            if needs_reissue:
                candidate = self._generate_unique_operator_order_id(assigned_ids)
                order.order_id = candidate
            assigned_ids.add(candidate)

    def _generate_unique_operator_order_id(self, assigned_ids: set[str]) -> str:
        """生成当前批次与历史库内都不冲突的 operator 订单 ID。"""
        while True:
            candidate = new_id("operator_order")
            if candidate in assigned_ids:
                continue
            if self.order_repository.get_order_by_id(candidate) is not None:
                continue
            return candidate

    def _resolve_trade_date(self, orders: list[OrderRequest]) -> date:
        trade_dates = {item.trade_date for item in orders}
        if len(trade_dates) != 1:
            raise ValueError("operator trade 当前要求同一批次订单的 trade_date 完全一致")
        return next(iter(trade_dates))

    def _pre_trade_validate(self, orders: list[OrderRequest]) -> tuple[dict[str, Any], list[OrderRequest], list[OrderRequest]]:
        positions = {item.ts_code: item for item in self.broker.get_positions(last_prices=None)}
        account = self.broker.get_account(last_prices=None)
        trade_date = self._resolve_trade_date(orders)
        ts_codes = sorted({order.ts_code for order in orders})
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=trade_date, active_only=False)
        bars = self._load_trade_date_bars(trade_date=trade_date, ts_codes=ts_codes)
        accepted: list[OrderRequest] = []
        validation = self.execution_contract_service.validate_basic_order_inputs(
            orders,
            trade_date=trade_date,
            securities=securities,
            bars=bars,
        )
        rejected = list(validation.rejected_orders)
        reasons_by_symbol: dict[str, list[str]] = defaultdict(list, validation.reasons_by_symbol)
        audit_results: dict[str, list[RiskResult]] = dict(validation.audit_results)
        candidate_orders = list(validation.candidate_orders)
        for order in rejected:
            rejection_reason = self._resolve_rejection_reason(audit_results.get(order.order_id, [])) or "未知输入拒绝"
            order.mark_rejected(OrderStatus.PRE_TRADE_REJECTED, rejection_reason)

        if candidate_orders:
            target_weights = self.execution_contract_service.build_projected_target_weights(candidate_orders, positions=positions, account=account)
            engine_accepted, engine_audit = self.risk_engine.validate_orders(
                candidate_orders,
                securities={code: securities[code] for code in {order.ts_code for order in candidate_orders}},
                bars={code: bars[code] for code in {order.ts_code for order in candidate_orders}},
                positions=positions,
                account=account,
                target_weights=target_weights,
            )
            accepted_ids = {item.order_id for item in engine_accepted}
            accepted.extend(engine_accepted)
            for order in candidate_orders:
                results = engine_audit.get(order.order_id, [])
                audit_results[order.order_id] = list(results)
                if order.order_id in accepted_ids:
                    order.last_error = None
                    continue
                rejection_reason = self._resolve_rejection_reason(results) or "未知风控拒绝"
                order.mark_rejected(OrderStatus.PRE_TRADE_REJECTED, rejection_reason)
                rejected.append(order)
                reasons_by_symbol[order.ts_code].append(rejection_reason)

        total_assets = float(account.total_assets)
        max_weight_value = max(total_assets * float(self.config.risk.max_position_weight), 0.0)
        risk_summary: dict[str, Any] = {
            "trade_date": trade_date.isoformat(),
            "accepted_order_count": len(accepted),
            "rejected_order_count": len(rejected),
            "reasons_by_symbol": dict(reasons_by_symbol),
            "available_cash": float(account.available_cash),
            "total_assets": total_assets,
            "max_order_value": float(self.config.risk.max_order_value),
            "max_weight_value": max_weight_value,
            "results_by_order": {
                order_id: [asdict(result) for result in results]
                for order_id, results in audit_results.items()
                if results
            },
        }
        if len(rejected) == len(orders):
            risk_summary["terminal_error"] = "全部订单在 pre-trade 阶段被拒绝"
        return risk_summary, accepted, rejected

    def _load_trade_date_bars(self, *, trade_date: date, ts_codes: list[str]) -> dict[str, Bar]:
        """读取 operator 批次对应交易日的行情快照。"""
        grouped = self.market_repository.load_bars_grouped(start_date=trade_date, end_date=trade_date, ts_codes=ts_codes)
        return {code: bars[-1] for code, bars in grouped.items() if bars}

    @staticmethod
    def _resolve_rejection_reason(results: list[RiskResult]) -> str | None:
        """提取第一条失败风控原因。"""
        for result in results:
            if not result.passed:
                return result.reason
        return None

    def _resolve_session_status(self, orders: list[OrderRequest]) -> TradeSessionStatus:
        order_count = len(orders)
        rejected_count = self._count_rejected_orders(orders)
        filled_count = len(
            [
                order
                for order in orders
                if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}
            ]
        )
        pending_follow_up_count = self._count_pending_follow_up_orders(orders)
        if order_count <= 0:
            return TradeSessionStatus.FAILED
        if rejected_count >= order_count:
            return TradeSessionStatus.REJECTED
        if pending_follow_up_count > 0:
            return TradeSessionStatus.RECOVERY_REQUIRED
        if filled_count > 0 and rejected_count > 0:
            return TradeSessionStatus.PARTIALLY_COMPLETED
        if filled_count > 0:
            return TradeSessionStatus.COMPLETED
        return TradeSessionStatus.FAILED

    def list_session_orders(self, session_id: str) -> list[OrderRequest]:
        rows = self.order_repository.list_orders(execution_session_id=session_id, limit=500)
        result: list[OrderRequest] = []
        for row in rows:
            result.append(
                OrderRequest(
                    order_id=row["order_id"],
                    run_id=row.get("run_id"),
                    trade_date=date.fromisoformat(row["trade_date"]),
                    strategy_id=row["strategy_id"],
                    ts_code=row["ts_code"],
                    side=OrderSide(row["side"]),
                    price=float(row["price"]),
                    quantity=int(row["quantity"]),
                    reason=row["reason"],
                    status=OrderStatus(row["status"]),
                    broker_order_id=row.get("broker_order_id"),
                    filled_quantity=int(row.get("filled_quantity") or 0),
                    avg_fill_price=row.get("avg_fill_price"),
                    last_error=row.get("last_error"),
                    account_id=row.get("account_id"),
                )
            )
        return result

    def list_session_fills(self, session_id: str) -> list[Fill]:
        rows = self.order_repository.list_fills(execution_session_id=session_id, limit=500)
        fills: list[Fill] = []
        for row in rows:
            fills.append(
                Fill(
                    fill_id=row["fill_id"],
                    run_id=row.get("run_id"),
                    order_id=row["order_id"],
                    trade_date=date.fromisoformat(row["trade_date"]),
                    ts_code=row["ts_code"],
                    side=OrderSide(row["side"]),
                    fill_price=float(row["fill_price"]),
                    fill_quantity=int(row["fill_quantity"]),
                    fee=float(row["fee"]),
                    tax=float(row["tax"]),
                    broker_order_id=row.get("broker_order_id"),
                    account_id=row.get("account_id"),
                )
            )
        return fills

    def _list_session_orders(self, session_id: str) -> list[OrderRequest]:
        """兼容旧调用方的私有别名；新代码请改用 ``list_session_orders``。"""
        return self.list_session_orders(session_id)

    def _list_session_fills(self, session_id: str) -> list[Fill]:
        """兼容旧调用方的私有别名；新代码请改用 ``list_session_fills``。"""
        return self.list_session_fills(session_id)

    def _derive_broker_event_cursor(self, previous_cursor: str | None, reports: list[ExecutionReport]) -> str | None:
        """兼容旧调用方的私有别名；新代码请改用 ``derive_broker_event_cursor``。"""
        return self.derive_broker_event_cursor(previous_cursor, reports)
