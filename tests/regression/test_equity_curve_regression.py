from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap


def test_equity_curve_regression(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        bars_by_symbol, securities = context.data_service.load_market_data()
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, bars_by_symbol, securities)
        assert round(result.equity_curve[-1], 2) >= 1000000.00
        assert round(result.metrics["max_drawdown"], 4) <= 0.0
    finally:
        context.close()
