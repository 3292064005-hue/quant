from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap


def test_equity_curve_regression(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy)
        assert round(result.equity_curve[-1], 2) >= 1000000.00
        assert round(result.metrics["max_drawdown"], 4) <= 0.0
        rows = context.store.query(
            "SELECT trade_date, daily_pnl, total_assets FROM account_snapshots WHERE run_id = ? ORDER BY trade_date",
            (result.run_id,),
        )
        assert len(rows) >= 2
        assert rows[0]["daily_pnl"] == 0.0
        assert rows[1]["daily_pnl"] == rows[1]["total_assets"] - rows[0]["total_assets"]
    finally:
        context.close()
