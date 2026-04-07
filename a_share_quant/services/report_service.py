"""报表服务。"""
from __future__ import annotations

from pathlib import Path

from a_share_quant.core.reporting import ReportWriter
from a_share_quant.domain.models import BacktestResult


class ReportService:
    """输出回测报告。"""

    def __init__(self, reports_dir: str, report_name_template: str) -> None:
        self.reports_dir = Path(reports_dir)
        self.report_name_template = report_name_template
        self.writer = ReportWriter()

    def write_backtest_report(self, result: BacktestResult) -> Path:
        payload = {
            "strategy_id": result.strategy_id,
            "run_id": result.run_id,
            "benchmark_symbol": result.benchmark_symbol,
            "trade_dates": [item.isoformat() for item in result.trade_dates],
            "equity_curve": result.equity_curve,
            "order_count": result.order_count,
            "fill_count": result.fill_count,
            "metrics": result.metrics,
        }
        report_name = self.report_name_template.format(strategy_id=result.strategy_id, run_id=result.run_id)
        primary_path = self.writer.write_json(self.reports_dir / report_name, payload)
        self.writer.write_json(self.reports_dir / f"{result.strategy_id}_backtest.json", payload)
        return primary_path
