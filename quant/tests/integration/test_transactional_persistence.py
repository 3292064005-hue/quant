from pathlib import Path

import pytest

from a_share_quant.app.bootstrap import bootstrap


def test_backtest_day_persistence_rolls_back_when_audit_write_fails(temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = Path(__file__).resolve().parents[2]
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    data_service = context.require_data_service()
    strategy_service = context.require_strategy_service()
    data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
    strategy = strategy_service.build_default()

    original_write = context.audit_repository.write
    state = {"calls": 0}

    def flaky_write(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            raise RuntimeError("forced audit failure")
        return original_write(*args, **kwargs)

    monkeypatch.setattr(context.audit_repository, "write", flaky_write)
    with pytest.raises(RuntimeError):
        context.require_backtest_service().run(strategy)

    run_rows = context.backtest_run_repository.store.query("SELECT status FROM backtest_runs")
    assert len(run_rows) == 1
    assert run_rows[0]["status"] == "FAILED"
    for table_name in ("orders", "fills", "account_snapshots", "position_snapshots", "audit_logs"):
        rows = context.store.query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        assert rows[0]["cnt"] == 0, table_name
    context.close()



def test_schema_enforces_foreign_keys(temp_config_dir: Path) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        for table_name in ("orders", "fills", "account_snapshots", "position_snapshots", "audit_logs"):
            rows = context.store.query(f"PRAGMA foreign_key_list({table_name})")
            assert rows, table_name
    finally:
        context.close()



def test_market_import_rolls_back_atomically_and_persists_failed_audit(temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    project_root = Path(__file__).resolve().parents[2]
    def flaky_upsert_bars(*args, **kwargs):
        raise RuntimeError("forced bars failure")

    monkeypatch.setattr(context.market_repository, "upsert_bars", flaky_upsert_bars)
    with pytest.raises(RuntimeError):
        data_service = context.require_data_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")

    assert data_service.last_import_run_id is not None
    counts = {
        table: context.store.query(f"SELECT COUNT(*) AS cnt FROM {table}")[0]["cnt"]
        for table in ("securities", "trading_calendar", "bars_daily")
    }
    assert counts == {"securities": 0, "trading_calendar": 0, "bars_daily": 0}
    run_row = context.store.query(
        "SELECT status, error_message FROM data_import_runs WHERE import_run_id = ?",
        (data_service.last_import_run_id,),
    )[0]
    assert run_row["status"] == "FAILED"
    assert "forced bars failure" in run_row["error_message"]
    events = context.data_import_repository.list_quality_events(data_service.last_import_run_id)
    assert any(event["event_type"] == "import_failed" for event in events)
    context.close()



def test_market_import_persists_quality_events_on_success(temp_config_dir: Path) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        project_root = Path(__file__).resolve().parents[2]
        data_service = context.require_data_service()
        data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        run_id = data_service.last_import_run_id
        assert run_id is not None
        run_row = context.store.query(
            "SELECT status, bars_count FROM data_import_runs WHERE import_run_id = ?",
            (run_id,),
        )[0]
        assert run_row["status"] == "COMPLETED"
        assert run_row["bars_count"] > 0
        events = context.data_import_repository.list_quality_events(run_id)
        assert any(event["event_type"] == "row_count_summary" for event in events)
    finally:
        context.close()
