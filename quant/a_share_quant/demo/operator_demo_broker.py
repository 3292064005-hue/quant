"""仓内自带的 operator acceptance broker client factory。

该模块提供一个**文件状态驱动**的 demo broker client，目标不是模拟真实券商的所有细节，
而是为 paper/live operator 命令链提供一个可重复、跨进程、仓内自给的 acceptance profile：

- snapshot 可读取账户/持仓；
- submit_order_lifecycle 可返回 ACCEPTED 订单生命周期；
- sync_session / supervisor 可在独立进程中继续推进到 FILLED；
- 所有状态写入配置目录关联的 runtime data 目录，不污染源码树。
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from a_share_quant.adapters.broker.base import ExecutionReportSubscription
from a_share_quant.domain.models import AccountSnapshot, ExecutionReport, Fill, LiveOrderSubmission, OrderRequest, OrderSide, OrderStatus, OrderTicket, PositionSnapshot

_DEFAULT_ACCOUNT_CASH = 100_000.0


class DemoOperatorBrokerClient:
    """面向 operator CLI 的跨进程 demo broker client。

    Args:
        state_path: 用于持久化 demo broker 状态的 JSON 文件。
        provider: 当前 broker provider，仅用于元信息记录。

    Boundary Behavior:
        - 每次 ``submit_order_lifecycle`` 只会把订单推进到 ``ACCEPTED``；
          后续成交由 ``query_trades`` / ``subscribe_execution_reports`` 触发并写回状态文件。
        - ``query_trades`` 会把仍未完成的 demo 订单一次性收口为 ``FILLED``，
          以便 ``operator_sync_session`` 在独立进程中也能完成账本推进。
        - ``subscribe_execution_reports`` 会对指定 ``broker_order_ids`` 立即推送 ``FILLED`` 报告，
          以便 ``operator_run_supervisor`` 覆盖订阅主路径。
    """

    def __init__(self, state_path: Path, *, provider: str = "qmt") -> None:
        self._state_path = Path(state_path)
        self._provider = provider
        self._closed = False
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._state_path.exists():
            self._write_state({"provider": provider, "orders": []})

    def connect(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def heartbeat(self) -> bool:
        return not self._closed

    def get_account(self, last_prices: dict[str, float] | None = None, account_id: str | None = None) -> AccountSnapshot:
        del last_prices
        orders = self._list_orders(account_id=account_id)
        positions = self._build_positions(orders)
        market_value = sum(item.market_value for item in positions)
        cash_delta = sum(
            item["price"] * item["quantity"] if item["side"] == OrderSide.BUY.value else -item["price"] * item["quantity"]
            for item in orders
            if item["status"] == OrderStatus.FILLED.value
        )
        cash = _DEFAULT_ACCOUNT_CASH - cash_delta
        return AccountSnapshot(
            cash=float(cash),
            available_cash=float(cash),
            market_value=float(market_value),
            total_assets=float(cash + market_value),
            pnl=0.0,
        )

    def get_positions(self, last_prices: dict[str, float] | None = None, account_id: str | None = None) -> list[PositionSnapshot]:
        del last_prices
        return self._build_positions(self._list_orders(account_id=account_id))

    def submit_order(self, order: OrderRequest, fill_price: float, trade_date: date) -> Fill:
        submission = self.submit_order_lifecycle(order, fill_price, trade_date)
        broker_order_id = submission.ticket.broker_order_id or order.order_id
        fill = self._materialize_fill(self._get_order_state(broker_order_id), source="legacy_submit")
        self._upsert_order(broker_order_id, status=OrderStatus.FILLED.value)
        return fill

    def submit_order_lifecycle(self, order: OrderRequest, fill_price: float, trade_date: date) -> LiveOrderSubmission:
        broker_order_id = order.broker_order_id or f"demo_{order.order_id}"
        order.account_id = order.account_id or None
        self._upsert_order(
            broker_order_id,
            payload={
                "order_id": order.order_id,
                "broker_order_id": broker_order_id,
                "trade_date": trade_date.isoformat(),
                "ts_code": order.ts_code,
                "side": order.side.value,
                "price": float(fill_price),
                "quantity": int(order.quantity),
                "account_id": order.account_id,
                "status": OrderStatus.ACCEPTED.value,
                "reason": order.reason,
                "strategy_id": order.strategy_id,
                "filled_quantity": 0,
                "avg_fill_price": None,
                "fill_emitted": False,
                "report_cursor": None,
            },
        )
        ticket = OrderTicket(
            order_id=order.order_id,
            requested_quantity=int(order.quantity),
            status=OrderStatus.ACCEPTED,
            broker_order_id=broker_order_id,
            filled_quantity=0,
        )
        report = ExecutionReport(
            report_id=f"{broker_order_id}_accepted",
            order_id=order.order_id,
            trade_date=trade_date,
            status=OrderStatus.ACCEPTED,
            requested_quantity=int(order.quantity),
            filled_quantity=0,
            remaining_quantity=int(order.quantity),
            message="accepted by demo operator broker",
            broker_order_id=broker_order_id,
            account_id=order.account_id,
            metadata={"provider": self._provider, "source": "demo_accept"},
        )
        return LiveOrderSubmission(ticket=ticket, reports=[report], fills=[])

    def cancel_order(self, broker_order_id: str) -> bool:
        self._upsert_order(broker_order_id, status=OrderStatus.CANCELLED.value)
        return True

    def query_orders(self, account_id: str | None = None) -> list[OrderRequest]:
        orders: list[OrderRequest] = []
        for item in self._list_orders(account_id=account_id):
            orders.append(
                OrderRequest(
                    order_id=item["order_id"],
                    trade_date=date.fromisoformat(item["trade_date"]),
                    strategy_id=item.get("strategy_id") or "operator.manual",
                    ts_code=item["ts_code"],
                    side=OrderSide(item["side"]),
                    price=float(item["price"]),
                    quantity=int(item["quantity"]),
                    reason=item.get("reason") or "demo_operator",
                    status=OrderStatus(item["status"]),
                    broker_order_id=item["broker_order_id"],
                    filled_quantity=int(item.get("filled_quantity", 0) or 0),
                    avg_fill_price=item.get("avg_fill_price"),
                    account_id=item.get("account_id"),
                )
            )
        return orders

    def query_trades(self, account_id: str | None = None) -> list[Fill]:
        fills: list[Fill] = []
        state = self._read_state()
        mutated = False
        for item in state.get("orders", []):
            if account_id not in {None, "", item.get("account_id")}:
                continue
            if item.get("status") == OrderStatus.CANCELLED.value:
                continue
            if not item.get("fill_emitted"):
                item["fill_emitted"] = True
                item["status"] = OrderStatus.FILLED.value
                item["filled_quantity"] = int(item["quantity"])
                item["avg_fill_price"] = float(item["price"])
                item["report_cursor"] = item.get("report_cursor") or f"cursor_{item['broker_order_id']}"
                mutated = True
            if item.get("status") == OrderStatus.FILLED.value:
                fills.append(self._materialize_fill(item, source="demo_query_trades"))
        if mutated:
            self._write_state(state)
        return fills

    def supports_execution_report_subscription(self) -> bool:
        return True

    def subscribe_execution_reports(
        self,
        handler,
        *,
        account_id: str | None = None,
        broker_order_ids: list[str] | None = None,
        cursor: str | None = None,
    ) -> ExecutionReportSubscription:
        del cursor
        emitted_cursor = None
        state = self._read_state()
        allowed_ids = set(broker_order_ids or [])
        mutated = False
        for item in state.get("orders", []):
            if allowed_ids and item.get("broker_order_id") not in allowed_ids:
                continue
            if account_id not in {None, "", item.get("account_id")}:
                continue
            if item.get("status") == OrderStatus.CANCELLED.value:
                continue
            item["fill_emitted"] = True
            item["status"] = OrderStatus.FILLED.value
            item["filled_quantity"] = int(item["quantity"])
            item["avg_fill_price"] = float(item["price"])
            emitted_cursor = f"cursor_{item['broker_order_id']}"
            item["report_cursor"] = emitted_cursor
            mutated = True
            handler(
                [
                    ExecutionReport(
                        report_id=f"{item['broker_order_id']}_filled",
                        order_id=item["order_id"],
                        trade_date=date.fromisoformat(item["trade_date"]),
                        status=OrderStatus.FILLED,
                        requested_quantity=int(item["quantity"]),
                        filled_quantity=int(item["quantity"]),
                        remaining_quantity=0,
                        message="filled by demo operator supervisor subscription",
                        fill_price=float(item["price"]),
                        broker_order_id=item["broker_order_id"],
                        account_id=item.get("account_id"),
                        metadata={"provider": self._provider, "source": "demo_subscription", "cursor": emitted_cursor},
                    )
                ],
                emitted_cursor,
            )
        if mutated:
            self._write_state(state)
        return ExecutionReportSubscription(cursor=emitted_cursor, metadata={"provider": self._provider, "source": "demo_subscription"})

    def _build_positions(self, orders: list[dict[str, Any]]) -> list[PositionSnapshot]:
        by_symbol: dict[str, dict[str, float]] = {}
        for item in orders:
            if item.get("status") != OrderStatus.FILLED.value:
                continue
            entry = by_symbol.setdefault(item["ts_code"], {"quantity": 0.0, "notional": 0.0})
            direction = 1.0 if item["side"] == OrderSide.BUY.value else -1.0
            entry["quantity"] += direction * float(item["quantity"])
            entry["notional"] += direction * float(item["quantity"]) * float(item["price"])
        positions: list[PositionSnapshot] = []
        for ts_code, payload in by_symbol.items():
            quantity = int(payload["quantity"])
            if quantity <= 0:
                continue
            avg_cost = float(payload["notional"] / quantity) if quantity else 0.0
            market_value = float(quantity * avg_cost)
            positions.append(
                PositionSnapshot(
                    ts_code=ts_code,
                    quantity=quantity,
                    available_quantity=quantity,
                    avg_cost=avg_cost,
                    market_value=market_value,
                    unrealized_pnl=0.0,
                )
            )
        return positions

    def _materialize_fill(self, item: dict[str, Any], *, source: str) -> Fill:
        return Fill(
            fill_id=f"fill_{item['broker_order_id']}",
            order_id=item["order_id"],
            trade_date=date.fromisoformat(item["trade_date"]),
            ts_code=item["ts_code"],
            side=OrderSide(item["side"]),
            fill_price=float(item["price"]),
            fill_quantity=int(item["quantity"]),
            fee=0.0,
            tax=0.0,
            broker_order_id=item["broker_order_id"],
            account_id=item.get("account_id")
        )

    def _list_orders(self, *, account_id: str | None = None) -> list[dict[str, Any]]:
        state = self._read_state()
        orders = list(state.get("orders", []))
        if account_id is None:
            return orders
        return [item for item in orders if item.get("account_id") in {None, "", account_id}]

    def _get_order_state(self, broker_order_id: str) -> dict[str, Any]:
        state = self._read_state()
        for item in state.get("orders", []):
            if item.get("broker_order_id") == broker_order_id:
                return item
        raise ValueError(f"demo broker 未找到订单 {broker_order_id}")

    def _upsert_order(self, broker_order_id: str, *, payload: dict[str, Any] | None = None, status: str | None = None) -> None:
        state = self._read_state()
        orders = state.setdefault("orders", [])
        for item in orders:
            if item.get("broker_order_id") == broker_order_id:
                if payload is not None:
                    item.update(payload)
                if status is not None:
                    item["status"] = status
                self._write_state(state)
                return
        if payload is None:
            raise ValueError(f"demo broker 未找到订单 {broker_order_id}")
        orders.append(payload)
        self._write_state(state)

    def _read_state(self) -> dict[str, Any]:
        if not self._state_path.exists():
            return {"provider": self._provider, "orders": []}
        with self._state_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_state(self, payload: dict[str, Any]) -> None:
        temp_path = self._state_path.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        temp_path.replace(self._state_path)


def create_client(*, config=None, provider: str = "qmt") -> DemoOperatorBrokerClient:
    """创建仓内自带 demo operator broker client。

    Args:
        config: 应用配置；用于解析 runtime data 目录。
        provider: broker provider 标识，默认 ``qmt``。

    Returns:
        ``DemoOperatorBrokerClient``。

    Raises:
        ValueError: 当未提供配置对象或无法解析 storage_dir 时抛出。
    """
    if config is None:
        raise ValueError("demo operator broker factory 需要传入 config 以解析状态文件目录")
    storage_dir = getattr(getattr(config, "data", None), "storage_dir", None)
    if not storage_dir:
        raise ValueError("demo operator broker factory 无法解析 data.storage_dir")
    state_path = Path(storage_dir) / "demo_operator_broker_state.json"
    return DemoOperatorBrokerClient(state_path=state_path, provider=provider)
