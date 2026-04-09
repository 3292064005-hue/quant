"""订单与成交仓储。"""
from __future__ import annotations

from collections import Counter
from typing import Any

from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import Fill, OrderRequest
from a_share_quant.storage.sqlite_store import SQLiteStore


class OrderRepository:
    """持久化订单与成交。"""

    _ORDER_UPSERT_SQL = """
        INSERT INTO orders
        (
            run_id, execution_session_id, order_id, trade_date, strategy_id, ts_code, side, price, quantity, reason, status,
            broker_order_id, account_id, order_type, time_in_force, filled_quantity, avg_fill_price, last_error, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            status = excluded.status,
            broker_order_id = excluded.broker_order_id,
            account_id = COALESCE(excluded.account_id, orders.account_id),
            filled_quantity = excluded.filled_quantity,
            avg_fill_price = excluded.avg_fill_price,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
    """

    _FILL_UPSERT_SQL = """
        INSERT INTO fills
        (run_id, execution_session_id, fill_id, order_id, broker_order_id, account_id, trade_date, ts_code, side, fill_price, fill_quantity, fee, tax, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fill_id) DO UPDATE SET
            run_id = excluded.run_id,
            execution_session_id = excluded.execution_session_id,
            order_id = excluded.order_id,
            broker_order_id = excluded.broker_order_id,
            account_id = COALESCE(excluded.account_id, fills.account_id),
            trade_date = excluded.trade_date,
            ts_code = excluded.ts_code,
            side = excluded.side,
            fill_price = excluded.fill_price,
            fill_quantity = excluded.fill_quantity,
            fee = excluded.fee,
            tax = excluded.tax
    """

    _IMMUTABLE_ORDER_FIELDS = (
        "run_id",
        "execution_session_id",
        "trade_date",
        "strategy_id",
        "ts_code",
        "side",
        "price",
        "quantity",
        "reason",
        "account_id",
        "order_type",
        "time_in_force",
    )

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def save_orders(self, run_id: str | None, orders: list[OrderRequest], *, execution_session_id: str | None = None) -> None:
        rows = self._build_order_rows(run_id, orders, execution_session_id=execution_session_id)
        if not rows:
            return
        with self.store.transaction():
            self._validate_order_rows(rows)
            self.store.executemany(self._ORDER_UPSERT_SQL, rows)

    def save_fills(self, run_id: str | None, fills: list[Fill], *, execution_session_id: str | None = None) -> None:
        rows = self._build_fill_rows(run_id, fills, execution_session_id=execution_session_id)
        if rows:
            self.store.executemany(self._FILL_UPSERT_SQL, rows)

    def save_execution_batch(
        self,
        run_id: str | None,
        orders: list[OrderRequest],
        fills: list[Fill],
        *,
        execution_session_id: str | None = None,
    ) -> None:
        order_rows = self._build_order_rows(run_id, orders, execution_session_id=execution_session_id)
        fill_rows = self._build_fill_rows(run_id, fills, execution_session_id=execution_session_id)
        with self.store.transaction():
            if order_rows:
                self._validate_order_rows(order_rows)
                self.store.executemany(self._ORDER_UPSERT_SQL, order_rows)
            if fill_rows:
                self.store.executemany(self._FILL_UPSERT_SQL, fill_rows)

    def count_orders(self, run_id: str | None = None, *, execution_session_id: str | None = None) -> int:
        return self._count_rows("orders", run_id=run_id, execution_session_id=execution_session_id)

    def count_fills(self, run_id: str | None = None, *, execution_session_id: str | None = None) -> int:
        return self._count_rows("fills", run_id=run_id, execution_session_id=execution_session_id)

    def _count_rows(self, table_name: str, *, run_id: str | None, execution_session_id: str | None) -> int:
        if run_id is None and execution_session_id is None:
            raise ValueError("run_id 与 execution_session_id 不能同时为空")
        if execution_session_id is not None:
            rows = self.store.query(f"SELECT COUNT(*) AS count FROM {table_name} WHERE execution_session_id = ?", (execution_session_id,))
        else:
            rows = self.store.query(f"SELECT COUNT(*) AS count FROM {table_name} WHERE run_id = ?", (run_id,))
        return int(rows[0]["count"]) if rows else 0

    def get_order_by_id(self, order_id: str) -> dict[str, Any] | None:
        rows = self.store.query(
            """
            SELECT order_id, run_id, execution_session_id, trade_date, strategy_id, ts_code, side, price, quantity, reason, status,
                   broker_order_id, account_id, order_type, time_in_force, filled_quantity, avg_fill_price, last_error, created_at, updated_at
            FROM orders WHERE order_id = ?
            """,
            (order_id,),
        )
        return dict(rows[0]) if rows else None

    def list_orders(
        self,
        run_id: str | None = None,
        *,
        execution_session_id: str | None = None,
        account_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if run_id is None and execution_session_id is None:
            raise ValueError("run_id 与 execution_session_id 不能同时为空")
        clauses: list[str] = []
        params: list[Any] = []
        if execution_session_id is not None:
            clauses.append("execution_session_id = ?")
            params.append(execution_session_id)
        else:
            clauses.append("run_id = ?")
            params.append(run_id)
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        rows = self.store.query(
            f"""
            SELECT order_id, run_id, execution_session_id, trade_date, strategy_id, ts_code, side, price, quantity, reason, status,
                   broker_order_id, account_id, order_type, time_in_force, filled_quantity, avg_fill_price, last_error, created_at, updated_at
            FROM orders WHERE {' AND '.join(clauses)}
            ORDER BY trade_date DESC, created_at DESC, order_id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        return [dict(row) for row in rows]

    def list_fills(
        self,
        run_id: str | None = None,
        *,
        execution_session_id: str | None = None,
        account_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if run_id is None and execution_session_id is None:
            raise ValueError("run_id 与 execution_session_id 不能同时为空")
        clauses: list[str] = []
        params: list[Any] = []
        if execution_session_id is not None:
            clauses.append("execution_session_id = ?")
            params.append(execution_session_id)
        else:
            clauses.append("run_id = ?")
            params.append(run_id)
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        rows = self.store.query(
            f"""
            SELECT fill_id, run_id, execution_session_id, order_id, broker_order_id, account_id, trade_date, ts_code, side, fill_price, fill_quantity, fee, tax, created_at
            FROM fills WHERE {' AND '.join(clauses)}
            ORDER BY trade_date DESC, created_at DESC, fill_id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        return [dict(row) for row in rows]

    def list_open_session_orders(self, execution_session_id: str) -> list[dict[str, Any]]:
        return self.store.query(
            """
            SELECT order_id, run_id, execution_session_id, trade_date, strategy_id, ts_code, side, price, quantity, reason, status,
                   broker_order_id, account_id, order_type, time_in_force, filled_quantity, avg_fill_price, last_error, created_at, updated_at
            FROM orders
            WHERE execution_session_id = ?
              AND status IN ('SUBMITTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'PENDING_CANCEL')
            ORDER BY created_at ASC, order_id ASC
            """,
            (execution_session_id,),
        )

    def _validate_order_rows(self, rows: list[tuple[object, ...]]) -> None:
        order_ids = [str(row[2]) for row in rows]
        duplicates = [order_id for order_id, count in Counter(order_ids).items() if count > 1]
        if duplicates:
            joined = ", ".join(sorted(duplicates))
            raise ValueError(f"当前批次存在重复 order_id，禁止入库: {joined}")
        existing_rows = self._load_existing_orders(order_ids)
        for row in rows:
            existing = existing_rows.get(str(row[2]))
            if existing is None:
                continue
            self._assert_same_order_identity(existing, row)

    def _load_existing_orders(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not order_ids:
            return {}
        placeholders = ",".join("?" for _ in order_ids)
        rows = self.store.query(
            f"""
            SELECT order_id, run_id, execution_session_id, trade_date, strategy_id, ts_code, side, price, quantity, reason,
                   account_id, order_type, time_in_force
            FROM orders WHERE order_id IN ({placeholders})
            """,
            tuple(order_ids),
        )
        return {str(row["order_id"]): dict(row) for row in rows}

    def _assert_same_order_identity(self, existing: dict[str, Any], row: tuple[object, ...]) -> None:
        candidate = self._row_to_immutable_map(row)
        for field in self._IMMUTABLE_ORDER_FIELDS:
            if self._normalize_identity_value(existing.get(field)) != self._normalize_identity_value(candidate.get(field)):
                raise ValueError(
                    "检测到 order_id 冲突，禁止用既有订单 ID 覆盖另一笔订单: "
                    f"order_id={candidate['order_id']} field={field} existing={existing.get(field)!r} candidate={candidate.get(field)!r}"
                )

    @staticmethod
    def _normalize_identity_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float):
            return round(value, 10)
        return str(value) if not isinstance(value, (int, str)) else value

    @staticmethod
    def _row_to_immutable_map(row: tuple[object, ...]) -> dict[str, object]:
        return {
            "run_id": row[0],
            "execution_session_id": row[1],
            "order_id": row[2],
            "trade_date": row[3],
            "strategy_id": row[4],
            "ts_code": row[5],
            "side": row[6],
            "price": row[7],
            "quantity": row[8],
            "reason": row[9],
            "account_id": row[12],
            "order_type": row[13],
            "time_in_force": row[14],
        }

    def _build_order_rows(
        self,
        run_id: str | None,
        orders: list[OrderRequest],
        *,
        execution_session_id: str | None = None,
    ) -> list[tuple[object, ...]]:
        now = now_iso()
        return [
            (
                run_id,
                execution_session_id,
                order.order_id,
                order.trade_date.isoformat(),
                order.strategy_id,
                order.ts_code,
                order.side.value,
                order.price,
                order.quantity,
                order.reason,
                order.status.value,
                order.broker_order_id,
                order.account_id,
                order.order_type.value,
                order.time_in_force.value,
                order.filled_quantity,
                order.avg_fill_price,
                order.last_error,
                now,
                now,
            )
            for order in orders
        ]

    def _build_fill_rows(
        self,
        run_id: str | None,
        fills: list[Fill],
        *,
        execution_session_id: str | None = None,
    ) -> list[tuple[object, ...]]:
        now = now_iso()
        return [
            (
                run_id,
                execution_session_id,
                fill.fill_id,
                fill.order_id,
                fill.broker_order_id,
                fill.account_id,
                fill.trade_date.isoformat(),
                fill.ts_code,
                fill.side.value,
                fill.fill_price,
                fill.fill_quantity,
                fill.fee,
                fill.tax,
                now,
            )
            for fill in fills
        ]
