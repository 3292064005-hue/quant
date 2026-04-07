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
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trading_calendar (
    exchange TEXT NOT NULL,
    cal_date TEXT NOT NULL,
    is_open INTEGER NOT NULL,
    pretrade_date TEXT,
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
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
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
    report_artifacts_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS orders (
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
);

CREATE TABLE IF NOT EXISTS fills (
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
);

CREATE TABLE IF NOT EXISTS position_snapshots (
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
);

CREATE TABLE IF NOT EXISTS account_snapshots (
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
CREATE INDEX IF NOT EXISTS idx_trading_calendar_cal_date ON trading_calendar (cal_date);
CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders (run_id);
CREATE INDEX IF NOT EXISTS idx_orders_trade_date ON orders (trade_date);
CREATE INDEX IF NOT EXISTS idx_fills_run_id ON fills (run_id);
CREATE INDEX IF NOT EXISTS idx_position_snapshots_run_id ON position_snapshots (run_id);
CREATE INDEX IF NOT EXISTS idx_account_snapshots_run_id ON account_snapshots (run_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id ON audit_logs (run_id);
CREATE INDEX IF NOT EXISTS idx_data_import_runs_started_at ON data_import_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_data_import_quality_events_run_id ON data_import_quality_events (import_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_snapshots_run_date ON account_snapshots (run_id, trade_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_snapshots_run_date_code ON position_snapshots (run_id, trade_date, ts_code);
