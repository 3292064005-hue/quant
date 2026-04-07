"""带契约映射的券商边界适配器基类。"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Iterable

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.adapters.broker.mappers import (
    map_account_snapshot,
    map_fill,
    map_fill_list,
    map_order_request_list,
    map_position_snapshots,
)
from a_share_quant.core.runtime_checks import check_broker_runtime
from a_share_quant.core.timeout_utils import call_with_timeout
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, OrderStatus, PositionSnapshot

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

    def get_account(self, last_prices: dict[str, float] | None = None) -> AccountSnapshot:
        """获取账户快照并完成契约映射。"""
        self._ensure_open()
        payload = call_with_timeout(
            self._client.get_account,
            last_prices or {},
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.get_account",
        )
        try:
            return map_account_snapshot(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s account payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_account_lenient(payload)

    def get_positions(self, last_prices: dict[str, float] | None = None) -> list[PositionSnapshot]:
        """获取持仓快照并完成契约映射。"""
        self._ensure_open()
        payload = call_with_timeout(
            self._client.get_positions,
            last_prices or {},
            timeout_seconds=self._timeout_seconds,
            operation_name="broker.get_positions",
        )
        try:
            return map_position_snapshots(payload)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s positions payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
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
            return map_fill(payload, fallback_order=order, fallback_trade_date=trade_date)
        except Exception:
            if self._strict_contract_mapping:
                raise
            logger.warning("%s submit_order payload 映射失败，已退化为兼容模式", self.provider_name, exc_info=True)
            return self._map_fill_lenient(payload, fallback_order=order, fill_price=fill_price, trade_date=trade_date)

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
        order_id = str(_read_field(payload, "order_id", "broker_order_id", "entrust_no", default=fallback_order.order_id)).strip() or fallback_order.order_id
        ts_code = str(_read_field(payload, "ts_code", "symbol", "security_code", "stock_code", default=fallback_order.ts_code)).strip() or fallback_order.ts_code
        side = _coerce_side(_read_field(payload, "side", "direction", "bs_type", default=fallback_order.side), default=fallback_order.side)
        fill_quantity = _coerce_int(_read_field(payload, "fill_quantity", "quantity", "qty", "deal_qty", default=fallback_order.quantity), default=fallback_order.quantity)
        fee = _coerce_float(_read_field(payload, "fee", "commission", default=0.0), default=0.0)
        tax = _coerce_float(_read_field(payload, "tax", "stamp_tax", default=0.0), default=0.0)
        run_id = _read_field(payload, "run_id", default=fallback_order.run_id)
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
            order_id = str(_read_field(item, "order_id", "broker_order_id", "entrust_no", default=f"external_order_{index}")).strip() or f"external_order_{index}"
            strategy_id = str(_read_field(item, "strategy_id", default="external")).strip() or "external"
            reason = str(_read_field(item, "reason", default="external")).strip() or "external"
            price = _coerce_float(_read_field(item, "price", "order_price", default=0.0), default=0.0)
            quantity = _coerce_int(_read_field(item, "quantity", "qty", "order_qty", default=0), default=0)
            status = _coerce_status(_read_field(item, "status", "order_status", default=OrderStatus.SUBMITTED), default=OrderStatus.SUBMITTED)
            run_id = _read_field(item, "run_id", default=None)
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
            order_id = str(_read_field(item, "order_id", "broker_order_id", "entrust_no", default=f"external_order_{index}")).strip() or f"external_order_{index}"
            fill_price = _coerce_float(_read_field(item, "fill_price", "price", "deal_price", default=0.0), default=0.0)
            fill_quantity = _coerce_int(_read_field(item, "fill_quantity", "quantity", "qty", "deal_qty", default=0), default=0)
            fee = _coerce_float(_read_field(item, "fee", "commission", default=0.0), default=0.0)
            tax = _coerce_float(_read_field(item, "tax", "stamp_tax", default=0.0), default=0.0)
            run_id = _read_field(item, "run_id", default=None)
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
