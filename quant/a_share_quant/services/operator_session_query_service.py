"""operator session 查询服务。"""
from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import Fill, OrderRequest, OrderSide, OrderStatus
from a_share_quant.repositories.order_repository import OrderRepository


class OperatorSessionQueryService:
    """集中封装 session order/fill 回放查询。"""

    def __init__(self, order_repository: OrderRepository) -> None:
        self.order_repository = order_repository

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
