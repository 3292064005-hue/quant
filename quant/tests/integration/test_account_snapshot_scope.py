from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from a_share_quant.domain.models import AccountSnapshot, PositionSnapshot
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


def _load_schema_sql() -> str:
    return Path(__file__).resolve().parents[2].joinpath('a_share_quant', 'schema.sql').read_text(encoding='utf-8')


def _insert_backtest_run(store: SQLiteStore, run_id: str) -> None:
    store.execute(
        """
        INSERT INTO backtest_runs (
            run_id, strategy_id, status, config_snapshot_json, started_at, finished_at,
            error_message, report_path, dataset_version_id, import_run_id, import_run_ids_json,
            data_source, data_start_date, data_end_date, dataset_digest, degradation_flags_json,
            warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json,
            run_manifest_json, run_events_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            'strategy.demo',
            'COMPLETED',
            '{}',
            '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:00:00+00:00',
            None,
            None,
            None,
            None,
            '[]',
            'csv',
            '2026-01-01',
            '2026-01-01',
            None,
            '[]',
            '[]',
            'tests.account_snapshot_scope',
            'v1',
            'research_backtest',
            '[]',
            '{}',
            '[]',
        ),
    )


def test_account_snapshot_storage_keeps_distinct_accounts_for_same_run_and_trade_date(tmp_path: Path) -> None:
    db_path = tmp_path / 'scoped_snapshots.db'
    store = SQLiteStore(str(db_path))
    store.init_schema(_load_schema_sql())
    repository = AccountRepository(store)
    _insert_backtest_run(store, 'run_1')

    repository.save_account_snapshot(
        'run_1',
        date(2026, 1, 5),
        AccountSnapshot(cash=100.0, available_cash=90.0, market_value=10.0, total_assets=110.0, pnl=1.0, cum_pnl=1.0, daily_pnl=1.0, drawdown=0.0),
        account_id='acct_A',
    )
    repository.save_account_snapshot(
        'run_1',
        date(2026, 1, 5),
        AccountSnapshot(cash=200.0, available_cash=180.0, market_value=20.0, total_assets=220.0, pnl=2.0, cum_pnl=2.0, daily_pnl=2.0, drawdown=0.0),
        account_id='acct_B',
    )
    repository.save_position_snapshots(
        'run_1',
        date(2026, 1, 5),
        [PositionSnapshot(ts_code='000001.SZ', quantity=10, available_quantity=10, avg_cost=10.0, market_value=100.0, unrealized_pnl=0.0)],
        account_id='acct_A',
    )
    repository.save_position_snapshots(
        'run_1',
        date(2026, 1, 5),
        [PositionSnapshot(ts_code='000001.SZ', quantity=20, available_quantity=20, avg_cost=11.0, market_value=220.0, unrealized_pnl=0.0)],
        account_id='acct_B',
    )

    account_rows = store.query(
        'SELECT account_id, total_assets FROM account_snapshots WHERE run_id = ? AND trade_date = ? ORDER BY account_id',
        ('run_1', '2026-01-05'),
    )
    position_rows = store.query(
        'SELECT account_id, quantity FROM position_snapshots WHERE run_id = ? AND trade_date = ? AND ts_code = ? ORDER BY account_id',
        ('run_1', '2026-01-05', '000001.SZ'),
    )

    assert [(row['account_id'], row['total_assets']) for row in account_rows] == [('acct_A', 110.0), ('acct_B', 220.0)]
    assert [(row['account_id'], row['quantity']) for row in position_rows] == [('acct_A', 10), ('acct_B', 20)]


def test_v21_account_snapshot_schema_migrates_unique_indexes_to_account_scope(tmp_path: Path) -> None:
    db_path = tmp_path / 'legacy_v21.db'
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            '''
            CREATE TABLE schema_version (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO schema_version (singleton_id, version, updated_at) VALUES (1, 21, '2026-01-01T00:00:00+00:00');
            CREATE TABLE backtest_runs (
                run_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                status TEXT NOT NULL,
                config_snapshot_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT,
                report_path TEXT,
                dataset_version_id TEXT,
                import_run_id TEXT,
                import_run_ids_json TEXT NOT NULL DEFAULT '[]',
                data_source TEXT,
                data_start_date TEXT,
                data_end_date TEXT,
                dataset_digest TEXT,
                degradation_flags_json TEXT NOT NULL DEFAULT '[]',
                warnings_json TEXT NOT NULL DEFAULT '[]',
                entrypoint TEXT,
                strategy_version TEXT,
                runtime_mode TEXT,
                report_artifacts_json TEXT NOT NULL DEFAULT '[]',
                run_manifest_json TEXT NOT NULL DEFAULT '{}',
                run_events_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE account_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT,
                trade_date TEXT NOT NULL,
                account_id TEXT,
                cash REAL NOT NULL,
                available_cash REAL NOT NULL,
                market_value REAL NOT NULL,
                total_assets REAL NOT NULL,
                pnl REAL NOT NULL,
                cum_pnl REAL,
                daily_pnl REAL,
                drawdown REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE position_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT,
                trade_date TEXT NOT NULL,
                account_id TEXT,
                ts_code TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                available_quantity INTEGER NOT NULL,
                avg_cost REAL NOT NULL,
                market_value REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX uq_account_snapshots_run_date ON account_snapshots (run_id, trade_date);
            CREATE UNIQUE INDEX uq_position_snapshots_run_date_code ON position_snapshots (run_id, trade_date, ts_code);
            '''
        )
        connection.commit()
    finally:
        connection.close()

    store = SQLiteStore(str(db_path))
    store.init_schema(_load_schema_sql())
    repository = AccountRepository(store)
    _insert_backtest_run(store, 'run_legacy')

    repository.save_account_snapshot(
        'run_legacy',
        date(2026, 1, 6),
        AccountSnapshot(cash=300.0, available_cash=280.0, market_value=30.0, total_assets=330.0, pnl=3.0, cum_pnl=3.0, daily_pnl=3.0, drawdown=0.0),
        account_id='acct_A',
    )
    repository.save_account_snapshot(
        'run_legacy',
        date(2026, 1, 6),
        AccountSnapshot(cash=400.0, available_cash=360.0, market_value=40.0, total_assets=440.0, pnl=4.0, cum_pnl=4.0, daily_pnl=4.0, drawdown=0.0),
        account_id='acct_B',
    )

    account_rows = store.query(
        'SELECT account_id, total_assets FROM account_snapshots WHERE run_id = ? AND trade_date = ? ORDER BY account_id',
        ('run_legacy', '2026-01-06'),
    )
    index_rows = store.query("PRAGMA index_list('account_snapshots')")
    unique_names = {row['name'] for row in index_rows if row['unique']}

    assert [(row['account_id'], row['total_assets']) for row in account_rows] == [('acct_A', 330.0), ('acct_B', 440.0)]
    assert 'uq_account_snapshots_run_account_date' in unique_names
    assert 'uq_account_snapshots_run_date' not in unique_names
