"""SQLite 持久层。"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from a_share_quant.core.utils import ensure_parent, now_iso


class SQLiteStore:
    """封装 SQLite 连接、事务边界与版本化迁移。"""

    CURRENT_SCHEMA_VERSION = 5

    _FK_TABLE_SPECS: tuple[tuple[str, str], ...] = (
        (
            "orders",
            """
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                run_id TEXT,
                trade_date TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
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
                order_id TEXT NOT NULL,
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

    def init_schema(self, schema_sql: str) -> None:
        """初始化数据库表结构并应用版本化迁移。"""
        self._ensure_open()
        self._connection.executescript(schema_sql)
        self._connection.commit()
        self._ensure_schema_version_table()
        self._apply_migrations()

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
        migrations: list[tuple[int, Callable[[], None]]] = [
            (1, self._migration_v1_compat_and_foreign_keys),
            (2, self._migration_v2_data_import_audit),
            (3, self._migration_v3_unique_indexes),
            (4, self._migration_v4_run_lineage_and_strategy_registry),
            (5, self._migration_v5_run_artifact_indexes),
        ]
        current_version = self._get_schema_version()
        for version, migration in migrations:
            if version <= current_version:
                continue
            with self.transaction():
                migration()
                self._set_schema_version(version)

    def _migration_v1_compat_and_foreign_keys(self) -> None:
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
        self._rebuild_tables_with_foreign_keys_if_needed()
        self._connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_bars_daily_trade_date ON bars_daily (trade_date);
            CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders (run_id);
            CREATE INDEX IF NOT EXISTS idx_orders_trade_date ON orders (trade_date);
            CREATE INDEX IF NOT EXISTS idx_fills_run_id ON fills (run_id);
            CREATE INDEX IF NOT EXISTS idx_position_snapshots_run_id ON position_snapshots (run_id);
            CREATE INDEX IF NOT EXISTS idx_account_snapshots_run_id ON account_snapshots (run_id);
            CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id ON audit_logs (run_id);
            CREATE INDEX IF NOT EXISTS idx_trading_calendar_cal_date ON trading_calendar (cal_date);
            """
        )

    def _migration_v2_data_import_audit(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS data_import_runs (
                import_run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                request_context_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                securities_count INTEGER NOT NULL DEFAULT 0,
                calendar_count INTEGER NOT NULL DEFAULT 0,
                bars_count INTEGER NOT NULL DEFAULT 0,
                degradation_flags_json TEXT NOT NULL DEFAULT '[]',
                warnings_json TEXT NOT NULL DEFAULT '[]',
                error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS data_import_quality_events (
                event_id TEXT PRIMARY KEY,
                import_run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                level TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (import_run_id) REFERENCES data_import_runs(import_run_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_data_import_runs_started_at ON data_import_runs (started_at);
            CREATE INDEX IF NOT EXISTS idx_data_import_quality_events_run_id ON data_import_quality_events (import_run_id);
            """
        )

    def _migration_v3_unique_indexes(self) -> None:
        self._connection.executescript(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_account_snapshots_run_date ON account_snapshots (run_id, trade_date);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_position_snapshots_run_date_code ON position_snapshots (run_id, trade_date, ts_code);
            """
        )


    def _migration_v4_run_lineage_and_strategy_registry(self) -> None:
        self._ensure_column("strategies", "class_path", "TEXT NOT NULL DEFAULT ''")
        for column_name, column_type in (
            ("import_run_id", "TEXT"),
            ("data_source", "TEXT"),
            ("data_start_date", "TEXT"),
            ("data_end_date", "TEXT"),
            ("dataset_digest", "TEXT"),
            ("degradation_flags_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("warnings_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("entrypoint", "TEXT"),
            ("strategy_version", "TEXT"),
            ("runtime_mode", "TEXT"),
            ("report_artifacts_json", "TEXT NOT NULL DEFAULT '[]'"),
        ):
            self._ensure_column("backtest_runs", column_name, column_type)

    def _migration_v5_run_artifact_indexes(self) -> None:
        self._connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_runs_import_run_id ON backtest_runs (import_run_id);
            CREATE INDEX IF NOT EXISTS idx_backtest_runs_dataset_digest ON backtest_runs (dataset_digest);
            CREATE INDEX IF NOT EXISTS idx_strategies_enabled_updated_at ON strategies (enabled, updated_at);
            """
        )

    def _rebuild_tables_with_foreign_keys_if_needed(self) -> None:
        for table_name, create_sql in self._FK_TABLE_SPECS:
            if not self._table_exists(table_name):
                continue
            if self._has_foreign_key(table_name):
                continue
            self._rebuild_table_with_foreign_key(table_name, create_sql)

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
        """提供显式事务上下文。

        Boundary Behavior:
            支持嵌套调用，只有最外层上下文真正提交或回滚。
        """
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
        self._ensure_open()
        self._connection.execute(sql, params)
        if not self.in_transaction:
            self._connection.commit()

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
            for row in cursor:
                yield row
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
