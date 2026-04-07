from datetime import date

from pathlib import Path

import pytest

from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.domain.models import Security, TargetPosition, TradingCalendarEntry


class _HistoryBoundedStrategy:
    strategy_id = "history_bounded"

    def __init__(self, required_bars: int) -> None:
        self._required_bars = required_bars
        self.max_seen_history = 0

    def required_history_bars(self) -> int:
        return self._required_bars

    def should_rebalance(self, eligible_trade_index: int) -> bool:
        return True

    def generate_targets(self, history_by_symbol: dict[str, list], current_date: date, securities: dict[str, Security]) -> list[TargetPosition]:
        for bars in history_by_symbol.values():
            self.max_seen_history = max(self.max_seen_history, len(bars))
        return []


def test_backtest_flow(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config_path = temp_config_dir / "app.yaml"
    context = bootstrap(str(config_path))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy)
        assert result.order_count > 0
        assert result.fill_count > 0
        assert len(result.equity_curve) > 1
        assert result.report_path is not None
        report_path = Path(result.report_path)
        assert report_path.exists()
        rebuilt_report = context.backtest_service.report_service.rebuild_backtest_report(result.run_id)
        assert rebuilt_report.exists()
        run_rows = context.backtest_run_repository.store.query("SELECT status FROM backtest_runs WHERE run_id = ?", (result.run_id,))
        assert run_rows[0]["status"] == "COMPLETED"
    finally:
        context.close()



def test_context_close_releases_store_and_broker(temp_config_dir: Path) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    context.close()
    try:
        context.market_repository.load_securities()
        assert False, "expected RuntimeError after store closed"
    except RuntimeError:
        pass
    try:
        context.broker.get_account({})
        assert False, "expected RuntimeError after broker closed"
    except RuntimeError:
        pass



def test_backtest_raises_when_no_bars_available(temp_config_dir: Path) -> None:
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    strategy = context.strategy_service.build_default()
    try:
        with pytest.raises(ValueError, match="没有任何可用行情 bar"):
            context.backtest_service.run(
                strategy,
                bars_by_symbol={},
                securities={},
                trade_calendar=[TradingCalendarEntry(exchange="SSE", cal_date=date(2024, 1, 2), is_open=True)],
            )
    finally:
        context.close()


def test_history_window_is_bounded_in_preload_and_stream_modes(temp_config_dir: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    context = bootstrap(str(temp_config_dir / "app.yaml"))
    try:
        context.data_service.import_csv(project_root / "sample_data" / "daily_bars.csv")
        preload_bundle = context.data_service.load_market_data_bundle()
        preload_strategy = _HistoryBoundedStrategy(required_bars=3)
        preload_result = context.backtest_service.engine.run(
            preload_strategy,
            preload_bundle.bars_by_symbol,
            preload_bundle.securities,
            trade_calendar=preload_bundle.trade_calendar,
        )
        assert preload_result.run_id
        assert preload_strategy.max_seen_history <= 3

        day_batches, securities, trade_dates = context.data_service.stream_market_data()
        stream_strategy = _HistoryBoundedStrategy(required_bars=4)
        stream_result = context.backtest_service.engine.run_streaming(
            stream_strategy,
            day_batches=day_batches,
            trade_dates=trade_dates,
            securities=securities,
        )
        assert stream_result.run_id
        assert stream_strategy.max_seen_history <= 4
    finally:
        context.close()
