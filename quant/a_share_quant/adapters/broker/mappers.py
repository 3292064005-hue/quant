"""券商返回载荷到领域对象的映射工具。"""
from __future__ import annotations

from dataclasses import is_dataclass
from datetime import date
from enum import Enum
from typing import Any, Iterable

from a_share_quant.core.exceptions import BrokerContractError
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, OrderStatus, PositionSnapshot


_ACCOUNT_FIELD_ALIASES = {
    "cash": ("cash",),
    "available_cash": ("available_cash", "available", "availableAmount"),
    "market_value": ("market_value", "marketValue", "market_val"),
    "total_assets": ("total_assets", "totalAssets", "asset", "assets"),
    "pnl": ("pnl", "profit", "floating_pnl"),
    "cum_pnl": ("cum_pnl", "cumProfit", "cumulative_pnl"),
    "daily_pnl": ("daily_pnl", "dailyProfit", "today_pnl"),
    "drawdown": ("drawdown",),
}

_POSITION_FIELD_ALIASES = {
    "ts_code": ("ts_code", "symbol", "security_code", "stock_code"),
    "quantity": ("quantity", "qty", "current_qty", "total_quantity"),
    "available_quantity": ("available_quantity", "available_qty", "available"),
    "avg_cost": ("avg_cost", "cost_price", "avgPrice", "cost"),
    "market_value": ("market_value", "marketValue", "value"),
    "unrealized_pnl": ("unrealized_pnl", "floating_pnl", "profit"),
}

_FILL_FIELD_ALIASES = {
    "fill_id": ("fill_id", "trade_id", "deal_id"),
    "order_id": ("order_id", "broker_order_id", "entrust_no"),
    "trade_date": ("trade_date", "date", "trade_day"),
    "ts_code": ("ts_code", "symbol", "security_code", "stock_code"),
    "side": ("side", "direction", "bs_type"),
    "fill_price": ("fill_price", "price", "deal_price"),
    "fill_quantity": ("fill_quantity", "quantity", "qty", "deal_qty"),
    "fee": ("fee", "commission"),
    "tax": ("tax", "stamp_tax"),
    "run_id": ("run_id",),
}

_ORDER_FIELD_ALIASES = {
    "order_id": ("order_id", "broker_order_id", "entrust_no"),
    "trade_date": ("trade_date", "date", "trade_day"),
    "strategy_id": ("strategy_id",),
    "ts_code": ("ts_code", "symbol", "security_code", "stock_code"),
    "side": ("side", "direction", "bs_type"),
    "price": ("price", "order_price"),
    "quantity": ("quantity", "qty", "order_qty"),
    "reason": ("reason",),
    "status": ("status", "order_status"),
    "run_id": ("run_id",),
}


def _read_alias(payload: Any, aliases: Iterable[str], *, default: Any = None, required: bool = True) -> Any:
    for alias in aliases:
        if isinstance(payload, dict) and alias in payload:
            return payload[alias]
        if hasattr(payload, alias):
            return getattr(payload, alias)
    if required:
        raise BrokerContractError(f"券商载荷缺少必要字段 aliases={list(aliases)} payload_type={type(payload)!r}")
    return default



def _as_float(value: Any, *, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(f"字段 {field_name} 不能转换为 float: {value!r}") from exc



def _as_int(value: Any, *, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(f"字段 {field_name} 不能转换为 int: {value!r}") from exc



def _as_date(value: Any, *, field_name: str) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        raise BrokerContractError(f"字段 {field_name} 不能为空")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise BrokerContractError(f"字段 {field_name} 不能解析为日期: {value!r}") from exc



def _as_order_side(value: Any, *, field_name: str) -> OrderSide:
    if isinstance(value, OrderSide):
        return value
    text = str(value).strip().upper()
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
    if text not in mapping:
        raise BrokerContractError(f"字段 {field_name} 不是受支持的买卖方向: {value!r}")
    return mapping[text]



def _as_order_status(value: Any, *, field_name: str, default: OrderStatus = OrderStatus.SUBMITTED) -> OrderStatus:
    if value is None:
        return default
    if isinstance(value, OrderStatus):
        return value
    text = str(value).strip().upper()
    try:
        return OrderStatus(text)
    except ValueError as exc:
        raise BrokerContractError(f"字段 {field_name} 不是受支持的订单状态: {value!r}") from exc



def map_account_snapshot(payload: Any) -> AccountSnapshot:
    """将券商账户载荷映射为 ``AccountSnapshot``。"""
    if isinstance(payload, AccountSnapshot):
        return payload
    cash = _as_float(_read_alias(payload, _ACCOUNT_FIELD_ALIASES["cash"]), field_name="cash")
    available_cash = _as_float(
        _read_alias(payload, _ACCOUNT_FIELD_ALIASES["available_cash"], default=cash, required=False),
        field_name="available_cash",
    )
    market_value = _as_float(
        _read_alias(payload, _ACCOUNT_FIELD_ALIASES["market_value"], default=0.0, required=False),
        field_name="market_value",
    )
    total_assets = _as_float(
        _read_alias(payload, _ACCOUNT_FIELD_ALIASES["total_assets"], default=cash + market_value, required=False),
        field_name="total_assets",
    )
    pnl = _as_float(_read_alias(payload, _ACCOUNT_FIELD_ALIASES["pnl"], default=0.0, required=False), field_name="pnl")
    cum_pnl = _read_alias(payload, _ACCOUNT_FIELD_ALIASES["cum_pnl"], default=pnl, required=False)
    daily_pnl = _read_alias(payload, _ACCOUNT_FIELD_ALIASES["daily_pnl"], default=0.0, required=False)
    drawdown = _read_alias(payload, _ACCOUNT_FIELD_ALIASES["drawdown"], default=0.0, required=False)
    return AccountSnapshot(
        cash=cash,
        available_cash=available_cash,
        market_value=market_value,
        total_assets=total_assets,
        pnl=pnl,
        cum_pnl=None if cum_pnl is None else _as_float(cum_pnl, field_name="cum_pnl"),
        daily_pnl=None if daily_pnl is None else _as_float(daily_pnl, field_name="daily_pnl"),
        drawdown=_as_float(drawdown, field_name="drawdown"),
    )



def map_position_snapshot(payload: Any) -> PositionSnapshot:
    """将单条持仓载荷映射为 ``PositionSnapshot``。"""
    if isinstance(payload, PositionSnapshot):
        return payload
    ts_code = str(_read_alias(payload, _POSITION_FIELD_ALIASES["ts_code"])).strip()
    if not ts_code:
        raise BrokerContractError("字段 ts_code 不能为空")
    quantity = _as_int(_read_alias(payload, _POSITION_FIELD_ALIASES["quantity"]), field_name="quantity")
    available_quantity = _as_int(
        _read_alias(payload, _POSITION_FIELD_ALIASES["available_quantity"], default=quantity, required=False),
        field_name="available_quantity",
    )
    avg_cost = _as_float(_read_alias(payload, _POSITION_FIELD_ALIASES["avg_cost"], default=0.0, required=False), field_name="avg_cost")
    market_value = _as_float(_read_alias(payload, _POSITION_FIELD_ALIASES["market_value"], default=0.0, required=False), field_name="market_value")
    unrealized_pnl = _as_float(
        _read_alias(payload, _POSITION_FIELD_ALIASES["unrealized_pnl"], default=0.0, required=False),
        field_name="unrealized_pnl",
    )
    return PositionSnapshot(
        ts_code=ts_code,
        quantity=quantity,
        available_quantity=available_quantity,
        avg_cost=avg_cost,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
    )



def map_position_snapshots(payload: Any) -> list[PositionSnapshot]:
    """将券商持仓列表映射为 ``PositionSnapshot`` 序列。"""
    if payload is None:
        return []
    if isinstance(payload, list) and all(isinstance(item, PositionSnapshot) for item in payload):
        return payload
    if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
        raise BrokerContractError(f"持仓载荷必须是可迭代对象，当前类型={type(payload)!r}")
    return [map_position_snapshot(item) for item in payload]



def map_fill(payload: Any, *, fallback_order: OrderRequest | None = None, fallback_trade_date: date | None = None) -> Fill:
    """将券商成交载荷映射为 ``Fill``。"""
    if isinstance(payload, Fill):
        return payload
    order_id_default = fallback_order.order_id if fallback_order is not None else None
    trade_date_default = fallback_trade_date or (fallback_order.trade_date if fallback_order is not None else None)
    ts_code_default = fallback_order.ts_code if fallback_order is not None else None
    side_default = fallback_order.side if fallback_order is not None else None
    quantity_default = fallback_order.quantity if fallback_order is not None else None
    fill_id = str(_read_alias(payload, _FILL_FIELD_ALIASES["fill_id"], default="external_fill", required=False)).strip() or "external_fill"
    order_id = str(_read_alias(payload, _FILL_FIELD_ALIASES["order_id"], default=order_id_default, required=order_id_default is None)).strip()
    trade_date = _as_date(_read_alias(payload, _FILL_FIELD_ALIASES["trade_date"], default=trade_date_default, required=trade_date_default is None), field_name="trade_date")
    ts_code = str(_read_alias(payload, _FILL_FIELD_ALIASES["ts_code"], default=ts_code_default, required=ts_code_default is None)).strip()
    side = _as_order_side(_read_alias(payload, _FILL_FIELD_ALIASES["side"], default=side_default, required=side_default is None), field_name="side")
    fill_price = _as_float(_read_alias(payload, _FILL_FIELD_ALIASES["fill_price"]), field_name="fill_price")
    fill_quantity = _as_int(
        _read_alias(payload, _FILL_FIELD_ALIASES["fill_quantity"], default=quantity_default, required=quantity_default is None),
        field_name="fill_quantity",
    )
    fee = _as_float(_read_alias(payload, _FILL_FIELD_ALIASES["fee"], default=0.0, required=False), field_name="fee")
    tax = _as_float(_read_alias(payload, _FILL_FIELD_ALIASES["tax"], default=0.0, required=False), field_name="tax")
    run_id = _read_alias(payload, _FILL_FIELD_ALIASES["run_id"], default=(fallback_order.run_id if fallback_order is not None else None), required=False)
    return Fill(
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



def map_fill_list(payload: Any) -> list[Fill]:
    """将成交列表载荷映射为 ``Fill`` 列表。"""
    if payload is None:
        return []
    if isinstance(payload, list) and all(isinstance(item, Fill) for item in payload):
        return payload
    if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
        raise BrokerContractError(f"成交载荷必须是可迭代对象，当前类型={type(payload)!r}")
    return [map_fill(item) for item in payload]



def map_order_request(payload: Any) -> OrderRequest:
    """将外部订单载荷映射为 ``OrderRequest``。"""
    if isinstance(payload, OrderRequest):
        return payload
    order_id = str(_read_alias(payload, _ORDER_FIELD_ALIASES["order_id"])).strip()
    trade_date = _as_date(_read_alias(payload, _ORDER_FIELD_ALIASES["trade_date"]), field_name="trade_date")
    strategy_id = str(_read_alias(payload, _ORDER_FIELD_ALIASES["strategy_id"], default="external", required=False)).strip() or "external"
    ts_code = str(_read_alias(payload, _ORDER_FIELD_ALIASES["ts_code"])).strip()
    side = _as_order_side(_read_alias(payload, _ORDER_FIELD_ALIASES["side"]), field_name="side")
    price = _as_float(_read_alias(payload, _ORDER_FIELD_ALIASES["price"]), field_name="price")
    quantity = _as_int(_read_alias(payload, _ORDER_FIELD_ALIASES["quantity"]), field_name="quantity")
    reason = str(_read_alias(payload, _ORDER_FIELD_ALIASES["reason"], default="external", required=False)).strip() or "external"
    status = _as_order_status(_read_alias(payload, _ORDER_FIELD_ALIASES["status"], default=OrderStatus.SUBMITTED, required=False), field_name="status")
    run_id = _read_alias(payload, _ORDER_FIELD_ALIASES["run_id"], default=None, required=False)
    return OrderRequest(
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



def map_order_request_list(payload: Any) -> list[OrderRequest]:
    """将外部订单列表映射为 ``OrderRequest`` 列表。"""
    if payload is None:
        return []
    if isinstance(payload, list) and all(isinstance(item, OrderRequest) for item in payload):
        return payload
    if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, dict)):
        raise BrokerContractError(f"订单载荷必须是可迭代对象，当前类型={type(payload)!r}")
    return [map_order_request(item) for item in payload]
