from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap


def test_backtest_flow(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config_path = temp_config_dir / "app.yaml"
    context = bootstrap(str(config_path))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        bars_by_symbol, securities = context.data_service.load_market_data()
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, bars_by_symbol, securities)
        assert result.order_count > 0
        assert result.fill_count > 0
        assert len(result.equity_curve) > 1
        assert result.report_path is not None
        report_path = Path(result.report_path)
        assert report_path.exists()
        run_rows = context.backtest_run_repository.store.query("SELECT status FROM backtest_runs WHERE run_id = ?", (result.run_id,))
        assert run_rows[0]["status"] == "COMPLETED"
    finally:
        context.close()



def test_context_close_releases_store(temp_config_dir: Path) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    context.close()
    try:
        context.market_repository.load_securities()
        assert False, "expected RuntimeError after store closed"
    except RuntimeError:
        pass
