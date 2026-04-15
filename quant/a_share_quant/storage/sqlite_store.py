"""SQLite 持久层。"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from a_share_quant.core.utils import build_dataset_version_fingerprint, ensure_parent, now_iso
from a_share_quant.storage.sqlite_migrations import build_sqlite_migration_steps
from a_share_quant.storage.sqlite_schema_manager import SQLiteSchemaManager

class SQLiteStore:
    """封装 SQLite 连接、事务边界与版本化迁移。"""

    CURRENT_SCHEMA_VERSION = 25

    _FK_TABLE_SPECS: tuple[tuple[str, str], ...] = (
        (
            "orders",
            """
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                run_id TEXT,
                execution_session_id TEXT,
                trade_date TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
                broker_order_id TEXT,
                account_id TEXT,
                order_type TEXT NOT NULL DEFAULT 'MARKET',
                time_in_force TEXT NOT NULL DEFAULT 'DAY',
                filled_quantity INTEGER NOT NULL DEFAULT 0,
                avg_fill_price REAL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )
            """,
        ),
        (
            "fills",
            """
            CREATE TABLE fills (
                fill_id TEXT PRIMARY KEY,
                run_id TEXT,
                execution_session_id TEXT,
                order_id TEXT NOT NULL,
                broker_order_id TEXT,
                account_id TEXT,
                trade_date TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                side TEXT NOT NULL,
                fill_price REAL NOT NULL,
                fill_quantity INTEGER NOT NULL,
                fee REAL NOT NULL,
                tax REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            )
            """,
        ),
        (
            "position_snapshots",
            """
            CREATE TABLE position_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT,
                trade_date TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                available_quantity INTEGER NOT NULL,
                avg_cost REAL NOT NULL,
                market_value REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )
            """,
        ),
        (
            "account_snapshots",
            """
            CREATE TABLE account_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT,
                trade_date TEXT NOT NULL,
                cash REAL NOT NULL,
                available_cash REAL NOT NULL,
                market_value REAL NOT NULL,
                total_assets REAL NOT NULL,
                pnl REAL NOT NULL,
                cum_pnl REAL,
                daily_pnl REAL,
                drawdown REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )
            """,
        ),
        (
            "audit_logs",
            """
            CREATE TABLE audit_logs (
                log_id TEXT PRIMARY KEY,
                run_id TEXT,
                trace_id TEXT NOT NULL,
                module TEXT NOT NULL,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                level TEXT NOT NULL,
                operator TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )
            """,
        ),
    )

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        ensure_parent(db_path)
        self._connection = sqlite3.connect(db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        if db_path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
        self._closed = False
        self._transaction_depth = 0
        self._schema_manager = SQLiteSchemaManager(self, migrations=build_sqlite_migration_steps(self))

    def init_schema(self, schema_sql: str) -> None:
        """初始化数据库表结构并应用版本化迁移。"""
        self._schema_manager.init_schema(
            schema_sql,
            execute_script=self._connection.executescript,
            commit=self._connection.commit,
        )

    def _migration_steps(self) -> list[object]:
        """兼容旧调用方的 migration 查询入口；实际注册表位于 sqlite_migrations 模块。"""
        return build_sqlite_migration_steps(self)

    def _ensure_schema_version_table(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        rows = self._connection.execute("SELECT version FROM schema_version WHERE singleton_id = 1").fetchall()
        if not rows:
            self._connection.execute(
                "INSERT INTO schema_version (singleton_id, version, updated_at) VALUES (1, ?, ?)",
                (0, now_iso()),
            )
            self._connection.commit()

    def _get_schema_version(self) -> int:
        row = self._connection.execute("SELECT version FROM schema_version WHERE singleton_id = 1").fetchone()
        return int(row[0]) if row else 0

    def _set_schema_version(self, version: int) -> None:
        self._connection.execute(
            "UPDATE schema_version SET version = ?, updated_at = ? WHERE singleton_id = 1",
            (version, now_iso()),
        )

    def _apply_migrations(self) -> None:
        """兼容旧调用方的 migration 入口；实际逻辑委托给 schema manager。"""
        self._schema_manager.apply_migrations()

    def _rebuild_tables_with_foreign_keys_if_needed(self) -> None:
        for table_name, create_sql in self._FK_TABLE_SPECS:
            if not self._table_exists(table_name):
                continue
            if self._has_foreign_key(table_name):
                continue
            self._rebuild_table_with_foreign_key(table_name, create_sql)

    def _has_existing_application_schema(self) -> bool:
        rows = self.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != ?",
            ("schema_version",),
        )
        return bool(rows)

    def _table_exists(self, table_name: str) -> bool:
        rows = self.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,))
        return bool(rows)

    def _has_foreign_key(self, table_name: str) -> bool:
        rows = self.query(f"PRAGMA foreign_key_list({table_name})")
        return bool(rows)

    def _rebuild_table_with_foreign_key(self, table_name: str, create_sql: str) -> None:
        temp_name = f"{table_name}__legacy"
        with self.transaction():
            self._connection.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name}")
            self._connection.execute(create_sql)
            old_columns = self._table_columns(temp_name)
            new_columns = self._table_columns(table_name)
            shared_columns = [column for column in old_columns if column in new_columns]
            if shared_columns:
                column_csv = ", ".join(shared_columns)
                self._connection.execute(
                    f"INSERT OR IGNORE INTO {table_name} ({column_csv}) SELECT {column_csv} FROM {temp_name}"
                )
            self._connection.execute(f"DROP TABLE {temp_name}")

    def _table_columns(self, table_name: str) -> list[str]:
        cursor = self._connection.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        self._ensure_open()
        if not self._table_exists(table_name):
            return
        cursor = self._connection.execute(f"PRAGMA table_info({table_name})")
        columns = {row[1] for row in cursor.fetchall()}
        if column_name not in columns:
            self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def begin(self) -> None:
        """显式开启事务。"""
        self._ensure_open()
        if self._transaction_depth == 0:
            self._connection.execute("BEGIN")
        self._transaction_depth += 1

    def commit(self) -> None:
        """提交显式事务。"""
        self._ensure_open()
        if self._transaction_depth <= 0:
            raise RuntimeError("当前不存在可提交的显式事务")
        self._transaction_depth -= 1
        if self._transaction_depth == 0:
            self._connection.commit()

    def rollback(self) -> None:
        """回滚显式事务。"""
        self._ensure_open()
        if self._transaction_depth <= 0:
            return
        self._connection.rollback()
        self._transaction_depth = 0

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """提供显式事务上下文。"""
        self.begin()
        try:
            yield
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()

    @property
    def in_transaction(self) -> bool:
        return self._transaction_depth > 0

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.execute_rowcount(sql, params)

    def execute_rowcount(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        self._ensure_open()
        cursor = self._connection.execute(sql, params)
        if not self.in_transaction:
            self._connection.commit()
        return int(cursor.rowcount)

    def executemany(self, sql: str, params: list[tuple[Any, ...]]) -> None:
        if not params:
            return
        self._ensure_open()
        self._connection.executemany(sql, params)
        if not self.in_transaction:
            self._connection.commit()

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        self._ensure_open()
        cursor = self._connection.execute(sql, params)
        return list(cursor.fetchall())

    def iterate(self, sql: str, params: tuple[Any, ...] = ()) -> Iterator[sqlite3.Row]:
        """按游标顺序逐行返回查询结果。"""
        self._ensure_open()
        cursor = self._connection.execute(sql, params)
        try:
            yield from cursor
        finally:
            cursor.close()

    def close(self) -> None:
        """关闭数据库连接。"""
        if self._closed:
            return
        self._connection.close()
        self._closed = True
        self._transaction_depth = 0

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError(f"SQLiteStore 已关闭，db_path={self.db_path}")
