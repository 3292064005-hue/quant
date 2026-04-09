CREATE TABLE IF NOT EXISTS schema_version (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    version INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS securities (
    ts_code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    board TEXT NOT NULL,
    is_st INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'L',
    list_date TEXT,
    delist_date TEXT,
    source_import_run_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trading_calendar (
    exchange TEXT NOT NULL,
    cal_date TEXT NOT NULL,
    is_open INTEGER NOT NULL,
    pretrade_date TEXT,
    source_import_run_id TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (exchange, cal_date)
);

CREATE TABLE IF NOT EXISTS bars_daily (
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
    source_import_run_id TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS strategies (
    strategy_id TEXT PRIMARY KEY,
    strategy_type TEXT NOT NULL,
    class_path TEXT NOT NULL DEFAULT '',
    params_json TEXT NOT NULL,
    version TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    component_manifest_json TEXT NOT NULL DEFAULT '{}',
    strategy_blueprint_json TEXT NOT NULL DEFAULT '{}',
    capability_tags_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

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


CREATE TABLE IF NOT EXISTS research_runs (
    research_run_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    dataset_version_id TEXT,
    dataset_digest TEXT,
    request_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    research_session_id TEXT,
    parent_research_run_id TEXT,
    root_research_run_id TEXT,
    step_name TEXT,
    is_primary_run INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);


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

CREATE TABLE IF NOT EXISTS backtest_runs (
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

CREATE TABLE IF NOT EXISTS orders (
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
);

CREATE TABLE IF NOT EXISTS fills (
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
);


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
    supervisor_owner TEXT,
    supervisor_lease_expires_at TEXT,
    supervisor_mode TEXT,
    last_supervised_at TEXT,
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

CREATE TABLE IF NOT EXISTS position_snapshots (
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
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS account_snapshots (
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
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
);

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

CREATE TABLE IF NOT EXISTS audit_logs (
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
);

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

CREATE INDEX IF NOT EXISTS idx_bars_daily_trade_date ON bars_daily (trade_date);
CREATE INDEX IF NOT EXISTS idx_bars_daily_source_import_run_id ON bars_daily (source_import_run_id);
CREATE INDEX IF NOT EXISTS idx_trading_calendar_cal_date ON trading_calendar (cal_date);
CREATE INDEX IF NOT EXISTS idx_trading_calendar_source_import_run_id ON trading_calendar (source_import_run_id);
CREATE INDEX IF NOT EXISTS idx_securities_source_import_run_id ON securities (source_import_run_id);
CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders (run_id);
CREATE INDEX IF NOT EXISTS idx_orders_trade_date ON orders (trade_date);
CREATE INDEX IF NOT EXISTS idx_orders_execution_session_id ON orders (execution_session_id);
CREATE INDEX IF NOT EXISTS idx_fills_run_id ON fills (run_id);
CREATE INDEX IF NOT EXISTS idx_fills_execution_session_id ON fills (execution_session_id);
CREATE INDEX IF NOT EXISTS idx_position_snapshots_run_id ON position_snapshots (run_id);
CREATE INDEX IF NOT EXISTS idx_account_snapshots_run_id ON account_snapshots (run_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id ON audit_logs (run_id);
CREATE INDEX IF NOT EXISTS idx_data_import_runs_started_at ON data_import_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_data_import_quality_events_run_id ON data_import_quality_events (import_run_id);
CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_digest ON dataset_versions (dataset_digest);
CREATE INDEX IF NOT EXISTS idx_dataset_versions_last_used_at ON dataset_versions (last_used_at);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_dataset_version_id ON backtest_runs (dataset_version_id);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_import_run_id ON backtest_runs (import_run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_dataset_digest ON backtest_runs (dataset_digest);
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_snapshots_run_account_date ON account_snapshots (run_id, COALESCE(account_id, ''), trade_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_snapshots_run_account_date_code ON position_snapshots (run_id, COALESCE(account_id, ''), trade_date, ts_code);

CREATE INDEX IF NOT EXISTS idx_research_runs_created_at ON research_runs (created_at);
CREATE INDEX IF NOT EXISTS idx_research_runs_dataset_version_id ON research_runs (dataset_version_id);
CREATE INDEX IF NOT EXISTS idx_research_runs_primary_created_at ON research_runs (is_primary_run, created_at);
CREATE INDEX IF NOT EXISTS idx_research_runs_session_id ON research_runs (research_session_id);
CREATE INDEX IF NOT EXISTS idx_research_runs_parent_run_id ON research_runs (parent_research_run_id);
CREATE INDEX IF NOT EXISTS idx_trade_sessions_created_at ON trade_sessions (created_at);
CREATE INDEX IF NOT EXISTS idx_trade_sessions_status ON trade_sessions (status);
CREATE INDEX IF NOT EXISTS idx_trade_command_events_session_id ON trade_command_events (session_id);

CREATE INDEX IF NOT EXISTS idx_trade_sessions_account_id ON trade_sessions (account_id);
CREATE INDEX IF NOT EXISTS idx_orders_execution_session_account_id ON orders (execution_session_id, account_id);
CREATE INDEX IF NOT EXISTS idx_fills_execution_session_account_id ON fills (execution_session_id, account_id);
CREATE INDEX IF NOT EXISTS idx_research_cache_last_used_at ON research_cache_entries (cache_namespace, last_used_at DESC);

CREATE INDEX IF NOT EXISTS idx_operator_account_snapshots_account_created ON operator_account_snapshots (COALESCE(account_id, ''), created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operator_account_snapshots_session_created ON operator_account_snapshots (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operator_position_snapshots_capture ON operator_position_snapshots (capture_id);
CREATE INDEX IF NOT EXISTS idx_operator_position_snapshots_account_created ON operator_position_snapshots (COALESCE(account_id, ''), created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_account_snapshots_capture_account ON operator_account_snapshots (capture_id, COALESCE(account_id, ''));
CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_position_snapshots_capture_account_code ON operator_position_snapshots (capture_id, COALESCE(account_id, ''), ts_code);
CREATE INDEX IF NOT EXISTS idx_runtime_events_occurred_at ON runtime_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_events_stream_scope ON runtime_events (stream_scope, stream_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_events_domain ON runtime_events (source_domain, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_run_edges_src ON research_run_edges (src_research_run_id, edge_kind, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_run_edges_dst ON research_run_edges (dst_research_run_id, edge_kind, created_at DESC);
