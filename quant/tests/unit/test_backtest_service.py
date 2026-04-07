from __future__ import annotations

from pathlib import Path

from a_share_quant.config.models import AppConfig
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus
from a_share_quant.services.backtest_service import BacktestService


class _EngineStub:
    def run(self, strategy, bars_by_symbol, securities, config_snapshot, benchmark_symbol, trade_calendar=None, **kwargs):
        return BacktestResult(strategy_id="demo_strategy", run_id="run_demo")


class _ReportServiceStub:
    def __init__(self, report_path: Path) -> None:
        self.report_path = report_path

    def write_backtest_report(self, result: BacktestResult):
        self.report_path.write_text("{}", encoding="utf-8")
        latest = self.report_path.parent / "latest.json"
        latest.write_text("{}", encoding="utf-8")
        return [self.report_path, latest]


class _RunRepositoryStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, BacktestRunStatus, str | None, str | None, list[str] | None]] = []

    def finish_run(self, run_id: str, status: BacktestRunStatus, error_message: str | None = None, report_path: str | None = None, report_artifacts: list[str] | None = None) -> None:
        self.calls.append((run_id, status, error_message, report_path, report_artifacts))


class _StrategyStub:
    strategy_id = "demo_strategy"


def test_backtest_service_marks_success_once(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    run_repository = _RunRepositoryStub()
    service = BacktestService(AppConfig(), _EngineStub(), _ReportServiceStub(report_path), run_repository)
    result = service.run(_StrategyStub(), bars_by_symbol={}, securities={})
    assert result.report_path == str(report_path)
    assert run_repository.calls == [
        ("run_demo", BacktestRunStatus.COMPLETED, None, str(report_path), [str(report_path), str(report_path.parent / "latest.json")])
    ]
