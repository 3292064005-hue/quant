"""SQLite 持久层。"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from a_share_quant.core.utils import ensure_parent


class SQLiteStore:
    """封装 SQLite 连接与基础执行方法。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        ensure_parent(db_path)
        self._connection = sqlite3.connect(db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._closed = False

    def init_schema(self, schema_sql: str) -> None:
        """初始化数据库表结构并应用轻量兼容迁移。"""
        self._ensure_open()
        self._connection.executescript(schema_sql)
        self._connection.commit()
        self._apply_compat_migrations()

    def _apply_compat_migrations(self) -> None:
        self._ensure_open()
        self._ensure_column("securities", "list_date", "TEXT")
        self._ensure_column("securities", "delist_date", "TEXT")
        self._ensure_column("bars_daily", "pre_close", "REAL")
        self._ensure_column("orders", "run_id", "TEXT")
        self._ensure_column("fills", "run_id", "TEXT")
        self._ensure_column("position_snapshots", "run_id", "TEXT")
        self._ensure_column("account_snapshots", "run_id", "TEXT")
        self._ensure_column("account_snapshots", "cum_pnl", "REAL")
        self._ensure_column("account_snapshots", "daily_pnl", "REAL")
        self._ensure_column("audit_logs", "run_id", "TEXT")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_calendar (
                exchange TEXT NOT NULL,
                cal_date TEXT NOT NULL,
                is_open INTEGER NOT NULL,
                pretrade_date TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (exchange, cal_date)
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                status TEXT NOT NULL,
                config_snapshot_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT,
                report_path TEXT
            )
            """
        )
        self._connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_bars_daily_trade_date ON bars_daily (trade_date);
            CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders (run_id);
            CREATE INDEX IF NOT EXISTS idx_orders_trade_date ON orders (trade_date);
            CREATE INDEX IF NOT EXISTS idx_fills_run_id ON fills (run_id);
            CREATE INDEX IF NOT EXISTS idx_position_snapshots_run_id ON position_snapshots (run_id);
            CREATE INDEX IF NOT EXISTS idx_account_snapshots_run_id ON account_snapshots (run_id);
            CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id ON audit_logs (run_id);
            """
        )
        self._connection.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        self._ensure_open()
        cursor = self._connection.execute(f"PRAGMA table_info({table_name})")
        columns = {row[1] for row in cursor.fetchall()}
        if column_name not in columns:
            self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self._ensure_open()
        self._connection.execute(sql, params)
        self._connection.commit()

    def executemany(self, sql: str, params: list[tuple[Any, ...]]) -> None:
        if not params:
            return
        self._ensure_open()
        self._connection.executemany(sql, params)
        self._connection.commit()

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        self._ensure_open()
        cursor = self._connection.execute(sql, params)
        return list(cursor.fetchall())

    def close(self) -> None:
        """关闭数据库连接。

        Boundary Behavior:
            重复关闭不会抛错。
        """
        if self._closed:
            return
        self._connection.close()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError(f"SQLiteStore 已关闭，db_path={self.db_path}")
