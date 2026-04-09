from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.storage.sqlite_store import SQLiteStore


def test_dataset_version_distinguishes_same_digest_but_different_import_batches(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    app_path = temp_config_dir / "app.yaml"
    source_csv = project_root / "sample_data" / "daily_bars.csv"

    with bootstrap(str(app_path)) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        data_service.import_csv(source_csv)
        import_run_id_1 = data_service.last_import_run_id
        strategy = strategy_service.build_default()
        result_1 = backtest_service.run(strategy, entrypoint="tests.integration.precise_lineage.first")

        data_service.import_csv(source_csv)
        import_run_id_2 = data_service.last_import_run_id
        result_2 = backtest_service.run(strategy, entrypoint="tests.integration.precise_lineage.second")

        assert result_1.data_lineage.dataset_digest == result_2.data_lineage.dataset_digest
        assert result_1.data_lineage.import_run_ids == [import_run_id_1]
        assert result_2.data_lineage.import_run_ids == [import_run_id_2]
        assert result_1.data_lineage.dataset_version_id != result_2.data_lineage.dataset_version_id

        assert result_1.data_lineage.dataset_version_id is not None
        assert result_2.data_lineage.dataset_version_id is not None
        version_1 = context.dataset_version_repository.get_by_id(result_1.data_lineage.dataset_version_id)
        version_2 = context.dataset_version_repository.get_by_id(result_2.data_lineage.dataset_version_id)
        assert version_1 is not None and version_2 is not None
        assert json.loads(version_1.import_run_ids_json) == [import_run_id_1]
        assert json.loads(version_2.import_run_ids_json) == [import_run_id_2]


def test_legacy_database_can_bootstrap_and_migrate_to_latest_schema(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    runtime_db = temp_config_dir.parent / "runtime" / "test.db"
    runtime_db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(runtime_db)
    try:
        connection.executescript(
            """
            CREATE TABLE schema_version (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO schema_version (singleton_id, version, updated_at) VALUES (1, 5, '2026-01-01T00:00:00+00:00');

            CREATE TABLE securities (
                ts_code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                exchange TEXT NOT NULL,
                board TEXT NOT NULL,
                is_st INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'L',
                list_date TEXT,
                delist_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE trading_calendar (
                exchange TEXT NOT NULL,
                cal_date TEXT NOT NULL,
                is_open INTEGER NOT NULL,
                pretrade_date TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (exchange, cal_date)
            );
            CREATE TABLE bars_daily (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                amount REAL NOT NULL,
                pre_close REAL,
                suspended INTEGER NOT NULL DEFAULT 0,
                limit_up INTEGER NOT NULL DEFAULT 0,
                limit_down INTEGER NOT NULL DEFAULT 0,
                adj_type TEXT NOT NULL DEFAULT 'qfq',
                created_at TEXT NOT NULL,
                PRIMARY KEY (ts_code, trade_date)
            );
            CREATE TABLE strategies (
                strategy_id TEXT PRIMARY KEY,
                strategy_type TEXT NOT NULL,
                class_path TEXT NOT NULL DEFAULT '',
                params_json TEXT NOT NULL,
                version TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE backtest_runs (
                run_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                status TEXT NOT NULL,
                config_snapshot_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT,
                report_path TEXT,
                import_run_id TEXT,
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
                run_manifest_json TEXT NOT NULL DEFAULT '{}'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    with bootstrap(str(app_path)) as context:
        version_rows = context.store.query("SELECT version FROM schema_version WHERE singleton_id = 1")
        assert int(version_rows[0]["version"]) == SQLiteStore.CURRENT_SCHEMA_VERSION
        dataset_columns = set(context.store._table_columns("dataset_versions"))
        assert "version_fingerprint" in dataset_columns
        assert "dataset_digest" in dataset_columns
        assert "source_import_run_id" in set(context.store._table_columns("bars_daily"))
        assert "dataset_version_id" in set(context.store._table_columns("backtest_runs"))



def test_v9_strategy_registry_database_migrates_component_manifest_fields(temp_config_dir: Path) -> None:
    app_path = temp_config_dir / "app.yaml"
    runtime_db = temp_config_dir.parent / "runtime" / "test.db"
    runtime_db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(runtime_db)
    try:
        connection.executescript(
            """
            CREATE TABLE schema_version (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO schema_version (singleton_id, version, updated_at) VALUES (1, 9, '2026-01-01T00:00:00+00:00');

            CREATE TABLE strategies (
                strategy_id TEXT PRIMARY KEY,
                strategy_type TEXT NOT NULL,
                class_path TEXT NOT NULL DEFAULT '',
                params_json TEXT NOT NULL,
                version TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO strategies (
                strategy_id, strategy_type, class_path, params_json, version, enabled, created_at, updated_at
            ) VALUES (
                'builtin_top_n_momentum',
                'TopNMomentumStrategy',
                'builtin.top_n_momentum',
                '{}',
                '0.3.0',
                1,
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    with bootstrap(str(app_path)) as context:
        version_rows = context.store.query("SELECT version FROM schema_version WHERE singleton_id = 1")
        assert int(version_rows[0]["version"]) == SQLiteStore.CURRENT_SCHEMA_VERSION
        rows = context.store.query(
            "SELECT component_manifest_json, capability_tags_json FROM strategies WHERE strategy_id = ?",
            ("builtin_top_n_momentum",),
        )
        assert len(rows) == 1
        component_manifest = json.loads(rows[0]["component_manifest_json"])
        capability_tags = json.loads(rows[0]["capability_tags_json"])
        assert component_manifest["factor_component"] == "builtin.momentum"
        assert "momentum" in capability_tags
