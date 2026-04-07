"""订单与成交仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import Fill, OrderRequest
from a_share_quant.storage.sqlite_store import SQLiteStore


class OrderRepository:
    """持久化订单与成交。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def save_orders(self, run_id: str, orders: list[OrderRequest]) -> None:
        """批量写入订单。"""
        now = now_iso()
        rows = [
            (run_id, order.order_id, order.trade_date.isoformat(), order.strategy_id, order.ts_code, order.side.value, order.price, order.quantity, order.reason, order.status.value, now, now)
            for order in orders
        ]
        if rows:
            self.store.executemany(
                """
                INSERT OR REPLACE INTO orders
                (run_id, order_id, trade_date, strategy_id, ts_code, side, price, quantity, reason, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def save_fills(self, run_id: str, fills: list[Fill]) -> None:
        """批量写入成交回报。"""
        now = now_iso()
        rows = [
            (run_id, fill.fill_id, fill.order_id, fill.trade_date.isoformat(), fill.ts_code, fill.side.value, fill.fill_price, fill.fill_quantity, fill.fee, fill.tax, now)
            for fill in fills
        ]
        if rows:
            self.store.executemany(
                """
                INSERT OR REPLACE INTO fills
                (run_id, fill_id, order_id, trade_date, ts_code, side, fill_price, fill_quantity, fee, tax, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
