"""SQLite migration 定义与注册表。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from a_share_quant.core.utils import build_dataset_version_fingerprint, now_iso
from a_share_quant.storage.sqlite_schema_manager import SQLiteMigrationStep


def build_sqlite_migration_steps(store: Any) -> list[SQLiteMigrationStep]:
    """返回 SQLiteStore 支持的 migration 步骤清单。"""
    return [
        SQLiteMigrationStep(1, lambda: migration_v1_compat_and_foreign_keys(store), description="compat and foreign keys"),
        SQLiteMigrationStep(2, lambda: migration_v2_data_import_audit(store), description="data import audit", depends_on=(1,)),
        SQLiteMigrationStep(3, lambda: migration_v3_unique_indexes(store), description="unique indexes", depends_on=(2,)),
        SQLiteMigrationStep(4, lambda: migration_v4_run_lineage_and_strategy_registry(store), description="run lineage and strategy registry", depends_on=(3,)),
        SQLiteMigrationStep(5, lambda: migration_v5_run_artifact_indexes(store), description="run artifact indexes", depends_on=(4,)),
        SQLiteMigrationStep(6, lambda: migration_v6_precise_lineage_and_dataset_versions(store), description="dataset versions", depends_on=(5,)),
        SQLiteMigrationStep(7, lambda: migration_v7_dataset_version_fingerprint(store), description="dataset version fingerprint", depends_on=(6,)),
        SQLiteMigrationStep(8, lambda: migration_v8_run_manifest_contract(store), description="run manifest contract", depends_on=(7,)),
        SQLiteMigrationStep(9, lambda: migration_v9_run_event_persistence(store), description="run event persistence", depends_on=(8,)),
        SQLiteMigrationStep(10, lambda: migration_v10_strategy_component_manifest(store), description="strategy component manifest", depends_on=(9,)),
        SQLiteMigrationStep(11, lambda: migration_v11_order_execution_fields(store), description="order execution fields", depends_on=(10,)),
        SQLiteMigrationStep(12, lambda: migration_v12_strategy_blueprint(store), description="strategy blueprint", depends_on=(11,)),
        SQLiteMigrationStep(13, lambda: migration_v13_research_runs(store), description="research runs", depends_on=(12,)),
        SQLiteMigrationStep(14, lambda: migration_v14_operator_trade_sessions(store), description="operator trade sessions", depends_on=(13,)),
        SQLiteMigrationStep(15, lambda: migration_v15_fill_broker_order_linkage(store), description="fill broker linkage", depends_on=(14,)),
        SQLiteMigrationStep(16, lambda: migration_v16_research_run_lineage(store), description="research run lineage", depends_on=(15,)),
        SQLiteMigrationStep(17, lambda: migration_v17_research_run_root_and_batch_rebind(store), description="research run root and batch", depends_on=(16,)),
        SQLiteMigrationStep(18, lambda: migration_v18_operator_account_scope(store), description="operator account scope", depends_on=(17,)),
        SQLiteMigrationStep(19, lambda: migration_v19_research_cache_entries(store), description="research cache entries", depends_on=(18,)),
        SQLiteMigrationStep(20, lambda: migration_v20_operator_supervisor(store), description="operator supervisor", depends_on=(19,)),
        SQLiteMigrationStep(21, lambda: migration_v21_account_snapshot_scope(store), description="account snapshot scope", depends_on=(20,)),
        SQLiteMigrationStep(22, lambda: migration_v22_account_snapshot_unique_scope(store), description="account snapshot unique scope", depends_on=(21,)),
        SQLiteMigrationStep(23, lambda: migration_v23_operator_account_snapshot_store(store), description="operator account snapshot store", depends_on=(22,)),
        SQLiteMigrationStep(24, lambda: migration_v24_runtime_events(store), description="runtime events", depends_on=(23,)),
        SQLiteMigrationStep(25, lambda: migration_v25_research_run_edges(store), description="research run edges", depends_on=(24,)),
    ]

def migration_v1_compat_and_foreign_keys(store: Any) -> None:
    store._ensure_column("securities", "list_date", "TEXT")
    store._ensure_column("securities", "delist_date", "TEXT")
    store._ensure_column("bars_daily", "pre_close", "REAL")
    store._ensure_column("orders", "run_id", "TEXT")
    store._ensure_column("fills", "run_id", "TEXT")
    store._ensure_column("position_snapshots", "run_id", "TEXT")
    store._ensure_column("account_snapshots", "run_id", "TEXT")
    store._ensure_column("account_snapshots", "cum_pnl", "REAL")
    store._ensure_column("account_snapshots", "daily_pnl", "REAL")
    store._ensure_column("audit_logs", "run_id", "TEXT")
    store._connection.execute(
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
    store._connection.execute(
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
    store._rebuild_tables_with_foreign_keys_if_needed()
    store._connection.executescript(
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

def migration_v2_data_import_audit(store: Any) -> None:
    store._connection.executescript(
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

def migration_v3_unique_indexes(store: Any) -> None:
    store._connection.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_account_snapshots_run_date ON account_snapshots (run_id, trade_date);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_position_snapshots_run_date_code ON position_snapshots (run_id, trade_date, ts_code);
        """
    )

def migration_v4_run_lineage_and_strategy_registry(store: Any) -> None:
    store._ensure_column("strategies", "class_path", "TEXT NOT NULL DEFAULT ''")
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
        store._ensure_column("backtest_runs", column_name, column_type)

def migration_v5_run_artifact_indexes(store: Any) -> None:
    store._connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_runs_import_run_id ON backtest_runs (import_run_id);
        CREATE INDEX IF NOT EXISTS idx_backtest_runs_dataset_digest ON backtest_runs (dataset_digest);
        CREATE INDEX IF NOT EXISTS idx_strategies_enabled_updated_at ON strategies (enabled, updated_at);
        """
    )

def migration_v6_precise_lineage_and_dataset_versions(store: Any) -> None:
    store._ensure_column("securities", "source_import_run_id", "TEXT")
    store._ensure_column("trading_calendar", "source_import_run_id", "TEXT")
    store._ensure_column("bars_daily", "source_import_run_id", "TEXT")
    store._ensure_column("backtest_runs", "dataset_version_id", "TEXT")
    store._ensure_column("backtest_runs", "import_run_ids_json", "TEXT NOT NULL DEFAULT '[]'")
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS dataset_versions (
            dataset_version_id TEXT PRIMARY KEY,
            dataset_digest TEXT NOT NULL UNIQUE,
            data_source TEXT NOT NULL,
            data_start_date TEXT,
            data_end_date TEXT,
            scope_json TEXT NOT NULL DEFAULT '{}',
            import_run_ids_json TEXT NOT NULL DEFAULT '[]',
            degradation_flags_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bars_daily_source_import_run_id ON bars_daily (source_import_run_id);
        CREATE INDEX IF NOT EXISTS idx_trading_calendar_source_import_run_id ON trading_calendar (source_import_run_id);
        CREATE INDEX IF NOT EXISTS idx_securities_source_import_run_id ON securities (source_import_run_id);
        CREATE INDEX IF NOT EXISTS idx_dataset_versions_last_used_at ON dataset_versions (last_used_at);
        CREATE INDEX IF NOT EXISTS idx_backtest_runs_dataset_version_id ON backtest_runs (dataset_version_id);
        """
    )

def migration_v9_run_event_persistence(store: Any) -> None:
    """为回测运行补充数据库内生的完整事件明细。"""
    store._ensure_column("backtest_runs", "run_events_json", "TEXT NOT NULL DEFAULT '[]'")
    if not store._table_exists("backtest_runs"):
        return
    rows = store.query("SELECT run_id, run_events_json, run_manifest_json FROM backtest_runs")
    for row in rows:
        existing_payload = row["run_events_json"]
        if existing_payload not in (None, '', '[]'):
            continue
        events: list[dict[str, Any]] = []
        try:
            manifest = json.loads(row["run_manifest_json"] or '{}')
        except json.JSONDecodeError:
            manifest = {}
        event_log_path = manifest.get("event_log_path") if isinstance(manifest, dict) else None
        if isinstance(event_log_path, str) and event_log_path:
            candidate = Path(event_log_path)
            if candidate.is_absolute() and candidate.exists():
                try:
                    payload = json.loads(candidate.read_text(encoding='utf-8'))
                except (OSError, json.JSONDecodeError):
                    payload = {}
                raw_events = payload.get('events', []) if isinstance(payload, dict) else []
                if isinstance(raw_events, list):
                    events = raw_events
        store._connection.execute(
            "UPDATE backtest_runs SET run_events_json = ? WHERE run_id = ?",
            (json.dumps(events, ensure_ascii=False), row["run_id"]),
        )

def migration_v10_strategy_component_manifest(store: Any) -> None:
    """为策略注册表补充正式组件声明与能力标签。"""
    store._ensure_column("strategies", "component_manifest_json", "TEXT NOT NULL DEFAULT '{}' ")
    store._ensure_column("strategies", "capability_tags_json", "TEXT NOT NULL DEFAULT '[]'")
    if not store._table_exists("strategies"):
        return
    rows = store.query("SELECT strategy_id, component_manifest_json, capability_tags_json, class_path FROM strategies")
    for row in rows:
        if row["component_manifest_json"] not in (None, "", "{}") and row["capability_tags_json"] not in (None, "", "[]"):
            continue
        class_path = (row["class_path"] or "").strip()
        if class_path == "builtin.top_n_momentum":
            manifest = {
                "signal_component": "builtin.top_n_selection",
                "factor_component": "builtin.momentum",
                "portfolio_construction_component": "builtin.equal_weight_top_n",
                "execution_policy_component": "builtin.close_fill_mock",
                "risk_gate_component": "builtin.pre_trade_risk",
                "benchmark_component": "builtin.daily_close_relative",
                "capability_tags": ["research", "momentum", "top_n", "daily_bar"],
            }
            tags = manifest["capability_tags"]
        else:
            manifest = {
                "signal_component": "builtin.direct_targets",
                "factor_component": "builtin.none",
                "portfolio_construction_component": "builtin.portfolio_engine",
                "execution_policy_component": "builtin.execution_engine",
                "risk_gate_component": "builtin.risk_engine",
                "benchmark_component": "builtin.daily_close_relative",
                "capability_tags": ["research", "external_strategy", "single_strategy"],
            }
            tags = manifest["capability_tags"]
        store._connection.execute(
            "UPDATE strategies SET component_manifest_json = ?, capability_tags_json = ? WHERE strategy_id = ?",
            (json.dumps(manifest, ensure_ascii=False), json.dumps(tags, ensure_ascii=False), row["strategy_id"]),
        )

def migration_v12_strategy_blueprint(store: Any) -> None:
    """为策略注册表补充正式 strategy blueprint 持久化字段。"""
    store._ensure_column("strategies", "strategy_blueprint_json", "TEXT NOT NULL DEFAULT '{}' ")
    if not store._table_exists("strategies"):
        return
    rows = store.query("SELECT strategy_id, strategy_blueprint_json, component_manifest_json FROM strategies")
    for row in rows:
        if row["strategy_blueprint_json"] not in (None, "", "{}"):
            continue
        try:
            manifest = json.loads(row["component_manifest_json"] or "{}")
        except json.JSONDecodeError:
            manifest = {}
        payload = {
            "universe": manifest.get("universe_component", "builtin.all_active_a_share"),
            "factor": manifest.get("factor_component", "builtin.none"),
            "signal": manifest.get("signal_component", "builtin.direct_targets"),
            "portfolio_construction": manifest.get("portfolio_construction_component", "builtin.portfolio_engine"),
            "execution_policy": manifest.get("execution_policy_component", "builtin.execution_engine"),
            "risk_gate": manifest.get("risk_gate_component", "builtin.risk_engine"),
            "benchmark": manifest.get("benchmark_component", "builtin.daily_close_relative"),
            "capability_tags": list(manifest.get("capability_tags") or []),
        }
        store._connection.execute(
            "UPDATE strategies SET strategy_blueprint_json = ? WHERE strategy_id = ?",
            (json.dumps(payload, ensure_ascii=False), row["strategy_id"]),
        )

def migration_v13_research_runs(store: Any) -> None:
    """新增 research workflow 运行记录表。"""
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_runs (
            research_run_id TEXT PRIMARY KEY,
            workflow_name TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            dataset_version_id TEXT,
            dataset_digest TEXT,
            request_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_research_runs_created_at ON research_runs (created_at);
        CREATE INDEX IF NOT EXISTS idx_research_runs_dataset_version_id ON research_runs (dataset_version_id);
        """
    )

def migration_v14_operator_trade_sessions(store: Any) -> None:
    """新增 operator trade 会话与订单关联字段。"""
    store._ensure_column("orders", "execution_session_id", "TEXT")
    store._ensure_column("fills", "execution_session_id", "TEXT")
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS trade_sessions (
            session_id TEXT PRIMARY KEY,
            runtime_mode TEXT NOT NULL,
            broker_provider TEXT NOT NULL,
            command_type TEXT NOT NULL,
            command_source TEXT NOT NULL,
            requested_by TEXT NOT NULL,
            status TEXT NOT NULL,
            idempotency_key TEXT UNIQUE,
            requested_trade_date TEXT,
            risk_summary_json TEXT NOT NULL DEFAULT '{}',
            order_count INTEGER NOT NULL DEFAULT 0,
            submitted_count INTEGER NOT NULL DEFAULT 0,
            rejected_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            account_id TEXT,
            broker_event_cursor TEXT,
            last_synced_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trade_command_events (
            event_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            level TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES trade_sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_trade_sessions_created_at ON trade_sessions (created_at);
        CREATE INDEX IF NOT EXISTS idx_trade_sessions_status ON trade_sessions (status);
        CREATE INDEX IF NOT EXISTS idx_trade_command_events_session_id ON trade_command_events (session_id);
        """
    )
    if store._table_exists("orders"):
        store._connection.execute("CREATE INDEX IF NOT EXISTS idx_orders_execution_session_id ON orders (execution_session_id)")
    if store._table_exists("fills"):
        store._connection.execute("CREATE INDEX IF NOT EXISTS idx_fills_execution_session_id ON fills (execution_session_id)")

def migration_v16_research_run_lineage(store: Any) -> None:
    """为 research_runs 补充会话谱系与主记录过滤字段。"""
    store._ensure_column("research_runs", "research_session_id", "TEXT")
    store._ensure_column("research_runs", "parent_research_run_id", "TEXT")
    store._ensure_column("research_runs", "root_research_run_id", "TEXT")
    store._ensure_column("research_runs", "step_name", "TEXT")
    store._ensure_column("research_runs", "is_primary_run", "INTEGER NOT NULL DEFAULT 1")
    if store._table_exists("research_runs"):
        store._connection.execute(
            "UPDATE research_runs SET step_name = COALESCE(step_name, artifact_type) WHERE COALESCE(step_name, '') = ''"
        )
        store._connection.execute(
            "UPDATE research_runs SET is_primary_run = COALESCE(is_primary_run, 1)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_research_runs_primary_created_at ON research_runs (is_primary_run, created_at)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_research_runs_session_id ON research_runs (research_session_id)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_research_runs_parent_run_id ON research_runs (parent_research_run_id)"
        )

def migration_v17_research_run_root_and_batch_rebind(store: Any) -> None:
    """修复 research root_run_id 空值，并把历史 batch 任务重挂到 batch 主记录下。"""
    if not store._table_exists("research_runs"):
        return
    store._connection.execute(
        "UPDATE research_runs SET root_research_run_id = research_run_id WHERE COALESCE(root_research_run_id, '') = ''"
    )
    rows = store.query(
        "SELECT research_run_id, research_session_id, result_json FROM research_runs WHERE artifact_type = 'experiment_batch_summary'"
    )
    for row in rows:
        batch_run_id = row["research_run_id"]
        batch_session_id = row.get("research_session_id") or f"research_batch_session::{batch_run_id}"
        store._connection.execute(
            "UPDATE research_runs SET research_session_id = ?, root_research_run_id = ?, step_name = COALESCE(NULLIF(step_name, ''), artifact_type), is_primary_run = 1 WHERE research_run_id = ?",
            (batch_session_id, batch_run_id, batch_run_id),
        )
        try:
            payload = json.loads(row.get("result_json") or "{}")
        except json.JSONDecodeError:
            payload = {}
        aggregate = payload.get("aggregate") if isinstance(payload, dict) else {}
        experiment_run_ids = aggregate.get("generated_research_run_ids") if isinstance(aggregate, dict) else []
        if not isinstance(experiment_run_ids, list):
            continue
        for experiment_run_id in experiment_run_ids:
            experiment_row = store.query(
                "SELECT result_json FROM research_runs WHERE research_run_id = ?",
                (experiment_run_id,),
            )
            store._connection.execute(
                "UPDATE research_runs SET research_session_id = ?, parent_research_run_id = ?, root_research_run_id = ?, is_primary_run = 0 WHERE research_run_id = ?",
                (batch_session_id, batch_run_id, batch_run_id, experiment_run_id),
            )
            if not experiment_row:
                continue
            try:
                experiment_payload = json.loads(experiment_row[0].get("result_json") or "{}")
            except json.JSONDecodeError:
                experiment_payload = {}
            lineage = ((experiment_payload.get("experiment") or {}).get("artifact_lineage") or {}) if isinstance(experiment_payload, dict) else {}
            child_ids = [
                lineage.get("dataset_summary_run_id"),
                lineage.get("feature_snapshot_run_id"),
                lineage.get("signal_snapshot_run_id"),
            ]
            for child_run_id in child_ids:
                if not child_run_id:
                    continue
                store._connection.execute(
                    "UPDATE research_runs SET research_session_id = ?, parent_research_run_id = ?, root_research_run_id = ?, is_primary_run = 0 WHERE research_run_id = ?",
                    (batch_session_id, experiment_run_id, batch_run_id, child_run_id),
                )

def migration_v8_run_manifest_contract(store: Any) -> None:
    """为回测运行补充正式 manifest 字段。"""
    store._ensure_column("backtest_runs", "run_manifest_json", "TEXT NOT NULL DEFAULT '{}'" )
    if not store._table_exists("backtest_runs"):
        return
    rows = store.query(
        "SELECT run_id, entrypoint, strategy_version, runtime_mode, report_artifacts_json, config_snapshot_json, run_manifest_json FROM backtest_runs"
    )
    for row in rows:
        existing_payload = row["run_manifest_json"]
        if existing_payload not in (None, "", "{}"):
            continue
        try:
            config_snapshot = json.loads(row["config_snapshot_json"] or "{}")
        except json.JSONDecodeError:
            config_snapshot = {}
        payload = {
            "schema_version": 2,
            "entrypoint": row["entrypoint"],
            "strategy_version": row["strategy_version"],
            "runtime_mode": row["runtime_mode"],
            "benchmark_initial_value": config_snapshot.get("backtest", {}).get("initial_cash"),
            "report_paths": json.loads(row["report_artifacts_json"] or "[]"),
            "event_log_path": None,
            "run_event_summary": {},
        }
        store._connection.execute(
            "UPDATE backtest_runs SET run_manifest_json = ? WHERE run_id = ?",
            (json.dumps(payload, ensure_ascii=False), row["run_id"]),
        )

def migration_v7_dataset_version_fingerprint(store: Any) -> None:
    if not store._table_exists("dataset_versions"):
        store._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS dataset_versions (
                dataset_version_id TEXT PRIMARY KEY,
                version_fingerprint TEXT NOT NULL UNIQUE,
                dataset_digest TEXT NOT NULL,
                data_source TEXT NOT NULL,
                data_start_date TEXT,
                data_end_date TEXT,
                scope_json TEXT NOT NULL DEFAULT '{}',
                import_run_ids_json TEXT NOT NULL DEFAULT '[]',
                degradation_flags_json TEXT NOT NULL DEFAULT '[]',
                warnings_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_digest ON dataset_versions (dataset_digest);
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_last_used_at ON dataset_versions (last_used_at);
            """
        )
        return
    columns = store._table_columns("dataset_versions")
    if "version_fingerprint" in columns:
        store._connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_digest ON dataset_versions (dataset_digest);
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_last_used_at ON dataset_versions (last_used_at);
            """
        )
        return
    legacy_rows = store.query(
        """
        SELECT dataset_version_id, dataset_digest, data_source, data_start_date, data_end_date,
               scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at
        FROM dataset_versions
        """
    )
    store._connection.execute("ALTER TABLE dataset_versions RENAME TO dataset_versions__legacy")
    store._connection.executescript(
        """
        DROP INDEX IF EXISTS idx_dataset_versions_dataset_digest;
        DROP INDEX IF EXISTS idx_dataset_versions_last_used_at;
        CREATE TABLE dataset_versions (
            dataset_version_id TEXT PRIMARY KEY,
            version_fingerprint TEXT NOT NULL UNIQUE,
            dataset_digest TEXT NOT NULL,
            data_source TEXT NOT NULL,
            data_start_date TEXT,
            data_end_date TEXT,
            scope_json TEXT NOT NULL DEFAULT '{}',
            import_run_ids_json TEXT NOT NULL DEFAULT '[]',
            degradation_flags_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL
        );
        CREATE INDEX idx_dataset_versions_dataset_digest ON dataset_versions (dataset_digest);
        CREATE INDEX idx_dataset_versions_last_used_at ON dataset_versions (last_used_at);
        """
    )
    migrated_rows: list[tuple[str, str, str, str, str | None, str | None, str, str, str, str, str, str]] = []
    for row in legacy_rows:
        version_fingerprint = build_dataset_version_fingerprint(
            dataset_digest=row["dataset_digest"],
            data_source=row["data_source"],
            data_start_date=row["data_start_date"],
            data_end_date=row["data_end_date"],
            scope=row["scope_json"],
            import_run_ids=json.loads(row["import_run_ids_json"] or "[]"),
            degradation_flags=json.loads(row["degradation_flags_json"] or "[]"),
            warnings=json.loads(row["warnings_json"] or "[]"),
        )
        migrated_rows.append((
            row["dataset_version_id"],
            version_fingerprint,
            row["dataset_digest"],
            row["data_source"],
            row["data_start_date"],
            row["data_end_date"],
            row["scope_json"],
            row["import_run_ids_json"],
            row["degradation_flags_json"],
            row["warnings_json"],
            row["created_at"],
            row["last_used_at"],
        ))
    if migrated_rows:
        store._connection.executemany(
            """
            INSERT INTO dataset_versions
            (dataset_version_id, version_fingerprint, dataset_digest, data_source, data_start_date, data_end_date,
             scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            migrated_rows,
        )
    store._connection.execute("DROP TABLE dataset_versions__legacy")

def migration_v11_order_execution_fields(store: Any) -> None:
    store._ensure_column("orders", "broker_order_id", "TEXT")
    store._ensure_column("orders", "order_type", "TEXT NOT NULL DEFAULT 'MARKET'")
    store._ensure_column("orders", "time_in_force", "TEXT NOT NULL DEFAULT 'DAY'")
    store._ensure_column("orders", "filled_quantity", "INTEGER NOT NULL DEFAULT 0")
    store._ensure_column("orders", "avg_fill_price", "REAL")
    store._ensure_column("orders", "last_error", "TEXT")

def migration_v15_fill_broker_order_linkage(store: Any) -> None:
    """为 fills 表补充 broker_order_id，避免领域 ID 与外部 broker ID 混用。"""
    store._ensure_column("fills", "broker_order_id", "TEXT")

def migration_v18_operator_account_scope(store: Any) -> None:
    """为 operator trade 链补齐账户维度与同步元信息。"""
    store._ensure_column("orders", "account_id", "TEXT")
    store._ensure_column("fills", "account_id", "TEXT")
    store._ensure_column("trade_sessions", "account_id", "TEXT")
    store._ensure_column("trade_sessions", "broker_event_cursor", "TEXT")
    store._ensure_column("trade_sessions", "last_synced_at", "TEXT")
    if store._table_exists("trade_sessions"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_sessions_account_id ON trade_sessions (account_id)"
        )
    if store._table_exists("orders"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_execution_session_account_id ON orders (execution_session_id, account_id)"
        )
    if store._table_exists("fills"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_execution_session_account_id ON fills (execution_session_id, account_id)"
        )

def migration_v19_research_cache_entries(store: Any) -> None:
    """创建 research 持久化缓存表。"""
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_cache_entries (
            cache_namespace TEXT NOT NULL,
            cache_key TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            dataset_version_id TEXT,
            dataset_digest TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            hit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            PRIMARY KEY (cache_namespace, cache_key)
        );
        CREATE INDEX IF NOT EXISTS idx_research_cache_last_used_at ON research_cache_entries (cache_namespace, last_used_at DESC);
        """
    )

def migration_v20_operator_supervisor(store: Any) -> None:
    """为 operator supervisor 补齐租约与订阅元信息。"""
    store._ensure_column("trade_sessions", "supervisor_owner", "TEXT")
    store._ensure_column("trade_sessions", "supervisor_lease_expires_at", "TEXT")
    store._ensure_column("trade_sessions", "supervisor_mode", "TEXT")
    store._ensure_column("trade_sessions", "last_supervised_at", "TEXT")
    if store._table_exists("trade_sessions"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_sessions_supervisor_lease ON trade_sessions (status, supervisor_lease_expires_at)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_sessions_supervisor_owner ON trade_sessions (supervisor_owner)"
        )

def migration_v21_account_snapshot_scope(store: Any) -> None:
    """为账户/持仓快照补齐 account_id 维度。"""
    store._ensure_column("account_snapshots", "account_id", "TEXT")
    store._ensure_column("position_snapshots", "account_id", "TEXT")
    if store._table_exists("account_snapshots"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_snapshots_run_account_date ON account_snapshots (run_id, account_id, trade_date)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_snapshots_account_date ON account_snapshots (account_id, trade_date)"
        )
    if store._table_exists("position_snapshots"):
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_snapshots_run_account_date ON position_snapshots (run_id, account_id, trade_date)"
        )
        store._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_snapshots_account_date ON position_snapshots (account_id, trade_date)"
        )

def migration_v22_account_snapshot_unique_scope(store: Any) -> None:
    """将账户/持仓快照的唯一约束升级为 account_id 维度。"""
    if store._table_exists("account_snapshots"):
        store._connection.execute("DROP INDEX IF EXISTS uq_account_snapshots_run_date")
        store._connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_account_snapshots_run_account_date ON account_snapshots (run_id, COALESCE(account_id, ''), trade_date)"
        )
    if store._table_exists("position_snapshots"):
        store._connection.execute("DROP INDEX IF EXISTS uq_position_snapshots_run_date_code")
        store._connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_position_snapshots_run_account_date_code ON position_snapshots (run_id, COALESCE(account_id, ''), trade_date, ts_code)"
        )

def migration_v23_operator_account_snapshot_store(store: Any) -> None:
    """创建 operator 账户/持仓快照存储。"""
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS operator_account_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            capture_id TEXT NOT NULL,
            session_id TEXT,
            trade_date TEXT NOT NULL,
            account_id TEXT,
            source TEXT NOT NULL,
            cash REAL NOT NULL,
            available_cash REAL NOT NULL,
            market_value REAL NOT NULL,
            total_assets REAL NOT NULL,
            pnl REAL NOT NULL,
            cum_pnl REAL,
            daily_pnl REAL,
            drawdown REAL NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES trade_sessions(session_id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS operator_position_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            capture_id TEXT NOT NULL,
            session_id TEXT,
            trade_date TEXT NOT NULL,
            account_id TEXT,
            source TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            available_quantity INTEGER NOT NULL,
            avg_cost REAL NOT NULL,
            market_value REAL NOT NULL,
            unrealized_pnl REAL NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES trade_sessions(session_id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_operator_account_snapshots_account_created ON operator_account_snapshots (COALESCE(account_id, ''), created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_operator_account_snapshots_session_created ON operator_account_snapshots (session_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_operator_position_snapshots_capture ON operator_position_snapshots (capture_id);
        CREATE INDEX IF NOT EXISTS idx_operator_position_snapshots_account_created ON operator_position_snapshots (COALESCE(account_id, ''), created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_account_snapshots_capture_account ON operator_account_snapshots (capture_id, COALESCE(account_id, ''));
        CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_position_snapshots_capture_account_code ON operator_position_snapshots (capture_id, COALESCE(account_id, ''), ts_code);
        """
    )

def migration_v24_runtime_events(store: Any) -> None:
    """创建统一 runtime 事件流持久化。"""
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runtime_events (
            event_id TEXT PRIMARY KEY,
            source_domain TEXT NOT NULL,
            stream_scope TEXT NOT NULL,
            stream_id TEXT,
            event_type TEXT NOT NULL,
            level TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            occurred_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_runtime_events_occurred_at ON runtime_events (occurred_at DESC);
        CREATE INDEX IF NOT EXISTS idx_runtime_events_stream_scope ON runtime_events (stream_scope, stream_id, occurred_at DESC);
        CREATE INDEX IF NOT EXISTS idx_runtime_events_domain ON runtime_events (source_domain, occurred_at DESC);
        """
    )

def migration_v25_research_run_edges(store: Any) -> None:
    """创建 research run 正式边表。"""
    store._connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_run_edges (
            edge_id TEXT PRIMARY KEY,
            src_research_run_id TEXT NOT NULL,
            dst_research_run_id TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            UNIQUE (src_research_run_id, dst_research_run_id, edge_kind),
            FOREIGN KEY (src_research_run_id) REFERENCES research_runs(research_run_id) ON DELETE CASCADE,
            FOREIGN KEY (dst_research_run_id) REFERENCES research_runs(research_run_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_research_run_edges_src ON research_run_edges (src_research_run_id, edge_kind, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_research_run_edges_dst ON research_run_edges (dst_research_run_id, edge_kind, created_at DESC);
        """
    )
