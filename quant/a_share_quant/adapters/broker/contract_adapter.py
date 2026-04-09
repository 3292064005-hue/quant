"""带契约映射的券商边界适配器基类。"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import ExecutionReportSubscription, LiveBrokerPort
from a_share_quant.adapters.broker.mappers import (
    map_account_snapshot,
    map_execution_report,
    map_execution_report_list,
    map_fill,
    map_fill_list,
    map_order_request_list,
    map_position_snapshots,
)
from a_share_quant.core.runtime_checks import check_broker_runtime
from a_share_quant.core.timeout_utils import call_with_timeout
from a_share_quant.domain.models import AccountSnapshot, ExecutionReport, Fill, LiveOrderSubmission, OrderRequest, OrderSide, OrderStatus, OrderTicket, PositionSnapshot

logger = logging.getLogger(__name__)


class MappedBrokerAdapter(LiveBrokerPort):
    """对外暴露统一领域对象的券商适配器。

    子类只需声明 ``provider_name``，其余行为由该基类统一完成：
    运行时契约校验、连接生命周期、超时控制以及载荷到领域对象的映射。

    `strict_contract_mapping` 语义：
        - True：第三方 payload 只要不能严格映射为领域对象，就立即失败。
        - False：进入兼容模式，尽量回退到 best-effort 领域对象，并记录 warning。
    """

    provider_name = "broker"

    def __init__(self, client: object, timeout_seconds: float | None = None, strict_contract_mapping: bool = True) -> None:
        validation = check_broker_runtime(
            self.provider_name,
            endpoint="validated_by_runtime",
            account_id="validated_by_runtime",
            injected_client=client,
        )
        if not validation.ok:
            raise ValueError(validation.message)
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._strict_contract_mapping = strict_contract_mapping
        self._closed = False

    def connect(self) -> None:
        """建立券商连接。"""
        self._closed = False
        connect = getattr(self._client, "connect", None)
        if callable(connect):
            call_with_timeout(connect, timeout_seconds=self._timeout_seconds, operation_name="broker.connect")

    def close(self) -> None:
        """关闭券商连接。

        Boundary Behavior:
            若客户端既没有 ``close`` 也没有 ``disconnect``，则静默视为无资源需要释放；
            重复关闭是幂等的。
        """
        if self._closed:
            return
        for method_name in ("close", "disconnect"):
            candidate = getattr(self._client, method_name, None)
            if callable(candidate):
                call_with_timeout(candidate, timeout_seconds=self._timeout_seconds, operation_name=f"broker.{method_name}")
                break
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError(f"{self.__class__.__name__} 已关闭")

    def _call_client_with_optional_account_scope(self, method_name: str, *, account_id: str | None = None, last_prices: dict[str, float] | None = None):
        """优先透传 ``account_id``，若底层客户端尚未支持则自动回退。"""
        self._ensure_open()
        method = getattr(self._client, method_name)
        kwargs = {}
        if last_prices is not None:
            kwargs["last_prices"] = last_prices
        if account_id is not None:
            kwargs["account_id"] = account_id
        try:
            return call_with_timeout(
                method,
                timeout_seconds=self._timeout_seconds,
                operation_name=f"broker.{method_name}",
                **kwargs,
            )
        except TypeError:
            fallback_kwargs = {"last_prices": last_prices or {}} if last_prices is not None else {}
            return call_with_timeout(
                method,
                timeout_seconds=self._timeout_seconds,
                operation_name=f"broker.{method_name}",
                **fallback_kwargs,
            )

    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """获取账户快照并完成契约映射。"""
        payload = self._call_client_with_optional_account_scope("get_account", last_prices=last_prices or {})
        try:
            return map_account_snapshot(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s account payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_account_lenient(payload)

    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """获取持仓快照并完成契约映射。"""
        payload = self._call_client_with_optional_account_scope("get_positions", last_prices=last_prices or {})
        try:
            return map_position_snapshots(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s positions payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_positions_lenient(payload)

    def get_account_snapshot(
        self,
        *,
        account_id: str | None = None,
        last_prices: dict[str, float] | None = None,
    ) -> AccountSnapshot:
        """按账户作用域读取账户快照并映射。"""
        payload = self._call_client_with_optional_account_scope("get_account", account_id=account_id, last_prices=last_prices or {})
        try:
            return map_account_snapshot(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s scoped account payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_account_lenient(payload)

    def get_position_snapshots(
        self,
        *,
        account_id: str | None = None,
        last_prices: dict[str, float] | None = None,
    ) -> list[PositionSnapshot]:
        """按账户作用域读取持仓快照并映射。"""
        payload = self._call_client_with_optional_account_scope("get_positions", account_id=account_id, last_prices=last_prices or {})
        try:
            return map_position_snapshots(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s scoped positions payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_positions_lenient(payload)

    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        """提交订单并将成交结果映射为 ``Fill``。"""
        self._ensure_open()
        payload = call_with_timeout(
            self._client.submit_order,
            order,
            fill_price,
            trade_date,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.submit_order",
        )
        try:
            fill = map_fill(payload, fallback_order=order, fallback_trade_date=trade_date)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s submit_order payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            fill = self._map_fill_lenient(payload, fallback_order=order, fill_price=fill_price, trade_date=trade_date)
        if fill.order_id != order.order_id and not fill.broker_order_id:
            fill.broker_order_id = fill.order_id
        fill.order_id = order.order_id
        if fill.broker_order_id and not order.broker_order_id:
            order.broker_order_id = fill.broker_order_id
        return fill

    def submit_order_lifecycle(self, order: OrderRequest, fill_price: float, trade_date: date) -> LiveOrderSubmission:
        """优先透传底层客户端的正式订单生命周期结果。"""
        self._ensure_open()
        submit_lifecycle = getattr(self._client, "submit_order_lifecycle", None)
        if not callable(submit_lifecycle):
            return super().submit_order_lifecycle(order, fill_price, trade_date)
        payload = call_with_timeout(
            submit_lifecycle,
            order,
            fill_price,
            trade_date,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.submit_order_lifecycle",
        )
        if isinstance(payload, LiveOrderSubmission):
            return payload
        if not isinstance(payload, dict):
            if self._strict_contract_mapping:
                raise TypeError(f"submit_order_lifecycle 返回值必须是 LiveOrderSubmission 或 dict；收到 {type(payload)!r}")
            logger.warning("%s submit_order_lifecycle 返回类型未知，回退到 legacy submit_order", self.provider_name)
            return super().submit_order_lifecycle(order, fill_price, trade_date)

        report_payload = payload.get("reports") or []
        fill_payload = payload.get("fills") or []
        reports = map_execution_report_list(report_payload)
        fills = [
            map_fill(item, fallback_order=order, fallback_trade_date=trade_date)
            for item in fill_payload
        ]
        ticket_payload = payload.get("ticket") or {}
        if isinstance(ticket_payload, OrderTicket):
            ticket = ticket_payload
        else:
            broker_order_id = ticket_payload.get("broker_order_id") if isinstance(ticket_payload, dict) else None
            status_raw = ticket_payload.get("status") if isinstance(ticket_payload, dict) else None
            filled_quantity_raw = ticket_payload.get("filled_quantity") if isinstance(ticket_payload, dict) else None
            avg_fill_price_raw = ticket_payload.get("avg_fill_price") if isinstance(ticket_payload, dict) else None
            latest_report = reports[-1] if reports else None
            ticket = OrderTicket(
                order_id=order.order_id,
                requested_quantity=int(ticket_payload.get("requested_quantity", order.quantity)) if isinstance(ticket_payload, dict) else int(order.quantity),
                status=(status_raw if isinstance(status_raw, OrderStatus) else OrderStatus(str(status_raw).strip().upper())) if status_raw is not None else (latest_report.status if latest_report is not None else order.status),
                broker_order_id=str(broker_order_id) if broker_order_id else (latest_report.broker_order_id if latest_report is not None else order.broker_order_id),
                filled_quantity=int(filled_quantity_raw if filled_quantity_raw is not None else (latest_report.filled_quantity if latest_report is not None else 0)),
                avg_fill_price=float(avg_fill_price_raw) if avg_fill_price_raw is not None else (latest_report.fill_price if latest_report is not None else None),
                reports=reports,
            )
        return LiveOrderSubmission(ticket=ticket, reports=reports, fills=fills)

    def cancel_order(self, broker_order_id: str) -> None:
        """撤单。"""
        self._ensure_open()
        call_with_timeout(
            self._client.cancel_order,
            broker_order_id,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.cancel_order",
        )

    def query_orders(self) -> list[OrderRequest]:
        """查询外部订单并映射到领域对象。"""
        self._ensure_open()
        payload = call_with_timeout(
            self._client.query_orders,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.query_orders",
        )
        try:
            return map_order_request_list(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s query_orders payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_orders_lenient(payload)

    def query_trades(self) -> list[Fill]:
        """查询外部成交并映射到领域对象。"""
        self._ensure_open()
        payload = call_with_timeout(
            self._client.query_trades,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.query_trades",
        )
        try:
            return map_fill_list(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s query_trades payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_trades_lenient(payload)

    def query_orders_scoped(self, *, account_id: str | None = None) -> list[OrderRequest]:
        """按账户作用域查询订单；若底层未支持则自动退化过滤。"""
        self._ensure_open()
        query_orders = getattr(self._client, "query_orders")
        try:
            payload = call_with_timeout(
                query_orders,
                account_id=account_id,
                timeout_seconds=self._timeout_seconds,
                operation_name="broker.query_orders_scoped",
            )
        except TypeError:
            payload = call_with_timeout(
                query_orders,
                timeout_seconds=self._timeout_seconds,
                operation_name="broker.query_orders",
            )
        try:
            orders = map_order_request_list(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s scoped query_orders payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            orders = self._map_orders_lenient(payload)
        if account_id is None:
            return orders
        return [item for item in orders if getattr(item, "account_id", None) in {None, "", account_id}]

    def query_trades_scoped(self, *, account_id: str | None = None) -> list[Fill]:
        """按账户作用域查询成交；若底层未支持则自动退化过滤。"""
        self._ensure_open()
        query_trades = getattr(self._client, "query_trades")
        try:
            payload = call_with_timeout(
                query_trades,
                account_id=account_id,
                timeout_seconds=self._timeout_seconds,
                operation_name="broker.query_trades_scoped",
            )
        except TypeError:
            payload = call_with_timeout(
                query_trades,
                timeout_seconds=self._timeout_seconds,
                operation_name="broker.query_trades",
            )
        try:
            fills = map_fill_list(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s scoped query_trades payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            fills = self._map_trades_lenient(payload)
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]

    def heartbeat(self) -> bool:
        """执行心跳检测。"""
        self._ensure_open()
        return bool(
            call_with_timeout(
                self._client.heartbeat,
                timeout_seconds=self._timeout_seconds,
                operation_name="broker.heartbeat",
            )
        )

    def supports_execution_report_subscription(self) -> bool:
        """返回底层客户端是否暴露正式执行回报订阅能力。"""
        return callable(getattr(self._client, "subscribe_execution_reports", None))

    def subscribe_execution_reports(
        self,
        handler,
        *,
        account_id: str | None = None,
        broker_order_ids: list[str] | None = None,
        cursor: str | None = None,
    ) -> ExecutionReportSubscription | None:
        """订阅底层 broker 的执行回报并完成领域映射。"""
        self._ensure_open()
        subscribe = getattr(self._client, "subscribe_execution_reports", None)
        if not callable(subscribe):
            return None

        latest_cursor = cursor

        def _handle(payload, callback_cursor=None):
            nonlocal latest_cursor
            reports: list[ExecutionReport]
            if isinstance(payload, (list, tuple)):
                reports = map_execution_report_list(list(payload))
            else:
                reports = [map_execution_report(payload)]
            latest_cursor = callback_cursor or latest_cursor
            handler(reports, latest_cursor)

        raw_subscription = call_with_timeout(
            subscribe,
            _handle,
            account_id=account_id,
            broker_order_ids=broker_order_ids,
            cursor=cursor,
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.subscribe_execution_reports",
        )
        if raw_subscription is None:
            return None

        def _close() -> None:
            close = getattr(raw_subscription, "close", None)
            if callable(close):
                call_with_timeout(close, timeout_seconds=self._timeout_seconds, operation_name="broker.subscription.close")

        subscription = ExecutionReportSubscription(close_callback=_close, cursor=latest_cursor, metadata={"provider": self.provider_name})
        return subscription

    def _map_account_lenient(self, payload: Any) -> AccountSnapshot:
        cash = _coerce_float(_read_field(payload, "cash", "available_cash", "available", "availableAmount", default=0.0), default=0.0)
        available_cash = _coerce_float(_read_field(payload, "available_cash", "available", "availableAmount", default=cash), default=cash)
        market_value = _coerce_float(_read_field(payload, "market_value", "marketValue", "market_val", "value", default=0.0), default=0.0)
        total_assets = _coerce_float(_read_field(payload, "total_assets", "totalAssets", "asset", "assets", default=cash + market_value), default=cash + market_value)
        pnl = _coerce_float(_read_field(payload, "pnl", "profit", "floating_pnl", default=0.0), default=0.0)
        cum_pnl = _coerce_optional_float(_read_field(payload, "cum_pnl", "cumProfit", "cumulative_pnl", default=pnl))
        daily_pnl = _coerce_optional_float(_read_field(payload, "daily_pnl", "dailyProfit", "today_pnl", default=0.0))
        drawdown = _coerce_float(_read_field(payload, "drawdown", default=0.0), default=0.0)
        return AccountSnapshot(
            cash=cash,
            available_cash=available_cash,
            market_value=market_value,
            total_assets=total_assets,
            pnl=pnl,
            cum_pnl=cum_pnl,
            daily_pnl=daily_pnl,
            drawdown=drawdown,
        )

    def _map_positions_lenient(self, payload: Any) -> list[PositionSnapshot]:
        if payload is None:
            return []
        if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
            return []
        results: list[PositionSnapshot] = []
        for item in payload:
            ts_code = str(_read_field(item, "ts_code", "symbol", "security_code", "stock_code", default="")).strip()
            if not ts_code:
                continue
            quantity = _coerce_int(_read_field(item, "quantity", "qty", "current_qty", "total_quantity", default=0), default=0)
            available_quantity = _coerce_int(_read_field(item, "available_quantity", "available_qty", "available", default=quantity), default=quantity)
            avg_cost = _coerce_float(_read_field(item, "avg_cost", "cost_price", "avgPrice", "cost", default=0.0), default=0.0)
            market_value = _coerce_float(_read_field(item, "market_value", "marketValue", "value", default=0.0), default=0.0)
            unrealized_pnl = _coerce_float(_read_field(item, "unrealized_pnl", "floating_pnl", "profit", default=0.0), default=0.0)
            results.append(
                PositionSnapshot(
                    ts_code=ts_code,
                    quantity=quantity,
                    available_quantity=available_quantity,
                    avg_cost=avg_cost,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                )
            )
        return results

    def _map_fill_lenient(self, payload: Any, *, fallback_order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        fill_id = str(_read_field(payload, "fill_id", "trade_id", "deal_id", default=f"{fallback_order.order_id}_fill")).strip() or f"{fallback_order.order_id}_fill"
        broker_order_id = _read_field(payload, "broker_order_id", "entrust_no", default=fallback_order.broker_order_id)
        order_id = str(_read_field(payload, "order_id", default=fallback_order.order_id)).strip() or fallback_order.order_id
        ts_code = str(_read_field(payload, "ts_code", "symbol", "security_code", "stock_code", default=fallback_order.ts_code)).strip() or fallback_order.ts_code
        side = _coerce_side(_read_field(payload, "side", "direction", "bs_type", default=fallback_order.side), default=fallback_order.side)
        fill_quantity = _coerce_int(_read_field(payload, "fill_quantity", "quantity", "qty", "deal_qty", default=fallback_order.quantity), default=fallback_order.quantity)
        fee = _coerce_float(_read_field(payload, "fee", "commission", default=0.0), default=0.0)
        tax = _coerce_float(_read_field(payload, "tax", "stamp_tax", default=0.0), default=0.0)
        run_id = _read_field(payload, "run_id", default=fallback_order.run_id)
        account_id = _read_field(payload, "account_id", "account", "fund_account", default=fallback_order.account_id)
        return Fill(
            fill_id=fill_id,
            order_id=order_id,
            trade_date=trade_date,
            ts_code=ts_code,
            side=side,
            fill_price=_coerce_float(_read_field(payload, "fill_price", "price", "deal_price", default=fill_price), default=fill_price),
            fill_quantity=fill_quantity,
            fee=fee,
            tax=tax,
            run_id=None if run_id is None else str(run_id),
            broker_order_id=None if broker_order_id is None else str(broker_order_id),
            account_id=None if account_id is None else str(account_id),
        )

    def _map_orders_lenient(self, payload: Any) -> list[OrderRequest]:
        if payload is None:
            return []
        if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
            return []
        results: list[OrderRequest] = []
        for index, item in enumerate(payload):
            ts_code = str(_read_field(item, "ts_code", "symbol", "security_code", "stock_code", default="")).strip()
            trade_date = _coerce_date(_read_field(item, "trade_date", "date", "trade_day", default=None))
            side = _coerce_side(_read_field(item, "side", "direction", "bs_type", default=None))
            if not ts_code or trade_date is None or side is None:
                continue
            broker_order_id = _read_field(item, "broker_order_id", "entrust_no", default=None)
            order_id = str(_read_field(item, "order_id", default=(broker_order_id or f"external_order_{index}"))).strip() or str(broker_order_id or f"external_order_{index}")
            strategy_id = str(_read_field(item, "strategy_id", default="external")).strip() or "external"
            reason = str(_read_field(item, "reason", default="external")).strip() or "external"
            price = _coerce_float(_read_field(item, "price", "order_price", default=0.0), default=0.0)
            quantity = _coerce_int(_read_field(item, "quantity", "qty", "order_qty", default=0), default=0)
            status = _coerce_status(_read_field(item, "status", "order_status", default=OrderStatus.SUBMITTED), default=OrderStatus.SUBMITTED)
            run_id = _read_field(item, "run_id", default=None)
            account_id = _read_field(item, "account_id", "account", "fund_account", default=None)
            results.append(
                OrderRequest(
                    order_id=order_id,
                    trade_date=trade_date,
                    strategy_id=strategy_id,
                    ts_code=ts_code,
                    side=side,
                    price=price,
                    quantity=quantity,
                    reason=reason,
                    status=status,
                    run_id=None if run_id is None else str(run_id),
                    broker_order_id=None if broker_order_id is None else str(broker_order_id),
                    account_id=None if account_id is None else str(account_id),
                )
            )
        return results

    def _map_trades_lenient(self, payload: Any) -> list[Fill]:
        if payload is None:
            return []
        if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
            return []
        results: list[Fill] = []
        for index, item in enumerate(payload):
            trade_date = _coerce_date(_read_field(item, "trade_date", "date", "trade_day", default=None))
            ts_code = str(_read_field(item, "ts_code", "symbol", "security_code", "stock_code", default="")).strip()
            side = _coerce_side(_read_field(item, "side", "direction", "bs_type", default=None))
            if trade_date is None or not ts_code or side is None:
                continue
            fill_id = str(_read_field(item, "fill_id", "trade_id", "deal_id", default=f"external_fill_{index}")).strip() or f"external_fill_{index}"
            broker_order_id = _read_field(item, "broker_order_id", "entrust_no", default=None)
            order_id = str(_read_field(item, "order_id", default=(broker_order_id or f"external_order_{index}"))).strip() or str(broker_order_id or f"external_order_{index}")
            fill_price = _coerce_float(_read_field(item, "fill_price", "price", "deal_price", default=0.0), default=0.0)
            fill_quantity = _coerce_int(_read_field(item, "fill_quantity", "quantity", "qty", "deal_qty", default=0), default=0)
            fee = _coerce_float(_read_field(item, "fee", "commission", default=0.0), default=0.0)
            tax = _coerce_float(_read_field(item, "tax", "stamp_tax", default=0.0), default=0.0)
            run_id = _read_field(item, "run_id", default=None)
            account_id = _read_field(item, "account_id", "account", "fund_account", default=None)
            results.append(
                Fill(
                    fill_id=fill_id,
                    order_id=order_id,
                    trade_date=trade_date,
                    ts_code=ts_code,
                    side=side,
                    fill_price=fill_price,
                    fill_quantity=fill_quantity,
                    fee=fee,
                    tax=tax,
                    run_id=None if run_id is None else str(run_id),
                    broker_order_id=None if broker_order_id is None else str(broker_order_id),
                    account_id=None if account_id is None else str(account_id),
                )
            )
        return results


def _read_field(payload: Any, *aliases: str, default: Any = None) -> Any:
    for alias in aliases:
        if isinstance(payload, dict) and alias in payload:
            return payload[alias]
        if hasattr(payload, alias):
            return getattr(payload, alias)
    return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        try:
            return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}")
        except ValueError:
            return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _coerce_side(value: Any, *, default: OrderSide | None = None) -> OrderSide | None:
    if isinstance(value, OrderSide):
        return value
    mapping = {
        "BUY": OrderSide.BUY,
        "B": OrderSide.BUY,
        "1": OrderSide.BUY,
        "LONG": OrderSide.BUY,
        "SELL": OrderSide.SELL,
        "S": OrderSide.SELL,
        "2": OrderSide.SELL,
        "SHORT": OrderSide.SELL,
    }
    normalized = str(value).strip().upper() if value is not None else ""
    return mapping.get(normalized, default)


def _coerce_status(value: Any, *, default: OrderStatus) -> OrderStatus:
    if isinstance(value, OrderStatus):
        return value
    try:
        return OrderStatus(str(value).strip().upper())
    except ValueError:
        return default
