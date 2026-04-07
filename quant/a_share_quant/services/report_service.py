"""报表服务。"""
from __future__ import annotations

import json
from pathlib import Path

from a_share_quant.core.metrics import compute_metrics, compute_relative_metrics
from a_share_quant.core.reporting import ReportWriter
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage, RunArtifacts
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository


class ReportService:
    """输出与重建回测报告。"""

    def __init__(
        self,
        reports_dir: str,
        report_name_template: str,
        *,
        account_repository: AccountRepository | None = None,
        order_repository: OrderRepository | None = None,
        run_repository: BacktestRunRepository | None = None,
        market_repository: MarketRepository | None = None,
        annual_trading_days: int = 252,
        risk_free_rate: float = 0.0,
    ) -> None:
        self.reports_dir = Path(reports_dir)
        self.report_name_template = report_name_template
        self.writer = ReportWriter()
        self.account_repository = account_repository
        self.order_repository = order_repository
        self.run_repository = run_repository
        self.market_repository = market_repository
        self.annual_trading_days = annual_trading_days
        self.risk_free_rate = risk_free_rate

    def write_backtest_report(self, result: BacktestResult) -> list[Path]:
        """写出回测报告，并返回所有产物路径。

        Boundary Behavior:
            - 首次写出的报告文件必须与 ``backtest_runs.report_artifacts_json`` 保持一致；
            - 因此会先解析目标路径，再把真实 ``report_paths`` 写入 payload 与 ``result.artifacts``。
        """
        report_name = self.report_name_template.format(strategy_id=result.strategy_id, run_id=result.run_id)
        primary_path = self.reports_dir / report_name
        latest_path = self.reports_dir / f"{result.strategy_id}_backtest.json"
        resolved_report_paths = [str(primary_path), str(latest_path)]
        result.artifacts.report_paths = resolved_report_paths
        payload = {
            "strategy_id": result.strategy_id,
            "run_id": result.run_id,
            "benchmark_symbol": result.benchmark_symbol,
            "trade_dates": [item.isoformat() for item in result.trade_dates],
            "equity_curve": result.equity_curve,
            "benchmark_curve": result.benchmark_curve,
            "order_count": result.order_count,
            "fill_count": result.fill_count,
            "metrics": result.metrics,
            "data_lineage": {
                "import_run_id": result.data_lineage.import_run_id,
                "data_source": result.data_lineage.data_source,
                "data_start_date": result.data_lineage.data_start_date,
                "data_end_date": result.data_lineage.data_end_date,
                "dataset_digest": result.data_lineage.dataset_digest,
                "degradation_flags": result.data_lineage.degradation_flags,
                "warnings": result.data_lineage.warnings,
            },
            "artifacts": {
                "entrypoint": result.artifacts.entrypoint,
                "strategy_version": result.artifacts.strategy_version,
                "runtime_mode": result.artifacts.runtime_mode,
                "report_paths": resolved_report_paths,
            },
        }
        self.writer.write_json(primary_path, payload)
        self.writer.write_json(latest_path, payload)
        return [primary_path, latest_path]

    def rebuild_backtest_report(self, run_id: str | None = None) -> Path:
        """基于数据库中的回测结果重建报表。"""
        if self.run_repository is None or self.account_repository is None or self.order_repository is None:
            raise RuntimeError("ReportService 未注入重建报表所需的 repository")
        run = self.run_repository.get_run(run_id) if run_id is not None else self.run_repository.get_latest_run(BacktestRunStatus.COMPLETED)
        if run is None:
            if run_id is None:
                raise ValueError("数据库中不存在已完成的回测运行")
            raise ValueError(f"找不到指定 run_id 的回测运行: {run_id}")
        trade_dates, equity_curve = self.account_repository.load_equity_curve(run.run_id)
        config_snapshot = json.loads(run.config_snapshot_json)
        benchmark_symbol = config_snapshot.get("backtest", {}).get("benchmark_symbol")
        benchmark_curve = self._rebuild_benchmark_curve(trade_dates, benchmark_symbol)
        metrics_payload = self._build_metrics_payload(equity_curve, benchmark_curve)
        result = BacktestResult(
            strategy_id=run.strategy_id,
            run_id=run.run_id,
            benchmark_symbol=benchmark_symbol,
            trade_dates=trade_dates,
            equity_curve=equity_curve,
            benchmark_curve=benchmark_curve,
            order_count=self.order_repository.count_orders(run.run_id),
            fill_count=self.order_repository.count_fills(run.run_id),
            metrics=metrics_payload,
            data_lineage=DataLineage(
                import_run_id=run.import_run_id,
                data_source=run.data_source or "database_snapshot",
                data_start_date=run.data_start_date,
                data_end_date=run.data_end_date,
                dataset_digest=run.dataset_digest,
                degradation_flags=json.loads(run.degradation_flags_json or "[]"),
                warnings=json.loads(run.warnings_json or "[]"),
            ),
            artifacts=RunArtifacts(
                entrypoint=run.entrypoint,
                strategy_version=run.strategy_version,
                runtime_mode=run.runtime_mode,
                report_paths=json.loads(run.report_artifacts_json or "[]"),
            ),
        )
        return self.write_backtest_report(result)[0]

    def _build_metrics_payload(self, equity_curve: list[float], benchmark_curve: list[float]) -> dict[str, float]:
        if len(equity_curve) >= 2:
            metrics = compute_metrics(equity_curve, annual_days=self.annual_trading_days, risk_free_rate=self.risk_free_rate)
            payload = {
                "total_return": metrics.total_return,
                "annual_return": metrics.annual_return,
                "max_drawdown": metrics.max_drawdown,
                "sharpe": metrics.sharpe,
                "volatility": metrics.volatility,
            }
        else:
            payload = {"total_return": 0.0, "annual_return": 0.0, "max_drawdown": 0.0, "sharpe": 0.0, "volatility": 0.0}
        if len(benchmark_curve) == len(equity_curve) and len(benchmark_curve) >= 2:
            relative = compute_relative_metrics(
                equity_curve,
                benchmark_curve,
                annual_days=self.annual_trading_days,
                risk_free_rate=self.risk_free_rate,
            )
            payload.update(
                {
                    "benchmark_total_return": relative.benchmark_total_return,
                    "benchmark_annual_return": relative.benchmark_annual_return,
                    "excess_total_return": relative.excess_total_return,
                    "tracking_error": relative.tracking_error,
                    "information_ratio": relative.information_ratio,
                    "beta": relative.beta,
                    "alpha": relative.alpha,
                }
            )
        return payload

    def _rebuild_benchmark_curve(self, trade_dates, benchmark_symbol: str | None) -> list[float]:
        if self.market_repository is None or not benchmark_symbol or not trade_dates:
            return []
        bars_by_symbol = self.market_repository.load_bars_grouped(
            start_date=trade_dates[0],
            end_date=trade_dates[-1],
            ts_codes=[benchmark_symbol],
        )
        bars = bars_by_symbol.get(benchmark_symbol, [])
        if len(bars) < 1:
            return []
        by_date = {bar.trade_date: bar.close for bar in bars if bar.close > 0}
        first_price = next((by_date[item] for item in trade_dates if item in by_date), None)
        if first_price is None:
            return []
        curve: list[float] = []
        last_value = 1_000_000.0
        for trade_date in trade_dates:
            price = by_date.get(trade_date)
            if price is not None:
                last_value = 1_000_000.0 * (price / first_price)
            curve.append(last_value)
        return curve
