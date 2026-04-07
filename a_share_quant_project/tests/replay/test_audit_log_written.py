from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap


def test_audit_log_written(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        bars_by_symbol, securities = context.data_service.load_market_data()
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, bars_by_symbol, securities)
        rows = context.audit_repository.store.query("SELECT COUNT(*) AS cnt FROM audit_logs WHERE run_id = ?", (result.run_id,))
        assert rows[0]["cnt"] > 0
    finally:
        context.close()
