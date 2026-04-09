"""报表服务。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from a_share_quant.core.metrics import compute_metrics, compute_relative_metrics
from a_share_quant.core.reporting import ReportWriter
from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage, RunArtifacts
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
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
        data_import_repository: DataImportRepository | None = None,
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
        self.data_import_repository = data_import_repository
        self.annual_trading_days = annual_trading_days
        self.risk_free_rate = risk_free_rate

    def write_backtest_report(self, result: BacktestResult) -> list[Path]:
        """写出回测报告，并返回所有报告主产物路径。

        Args:
            result: 已完成回测的结果对象；其中 ``artifacts`` / ``run_events`` / ``data_quality_events``
                会被纳入报告契约。

        Returns:
            仅返回主报告路径列表，不包含独立事件日志路径。

        Raises:
            ValueError: 当 ``result`` 缺少 ``strategy_id`` 或 ``run_id`` 时抛出。

        Boundary Behavior:
            - 首次写出的报告文件必须与 ``backtest_runs.report_artifacts_json`` 保持一致；
            - manifest 中的 ``report_paths`` / ``event_log_path`` 以相对 ``reports_dir`` 的可迁移路径为优先表达；
            - 若存在 ``run_events``，会额外写出 sidecar 事件日志 JSON，同时完整事件明细仍由数据库字段兜底；
            - 本方法只负责产物写出，不直接决定数据库 run.status，状态收口由调用方控制。
        """
        if not result.strategy_id or not result.run_id:
            raise ValueError("write_backtest_report 需要有效的 strategy_id 与 run_id")
        report_name = self.report_name_template.format(strategy_id=result.strategy_id, run_id=result.run_id)
        primary_path = self.reports_dir / report_name
        latest_path = self.reports_dir / f"{result.strategy_id}_backtest.json"
        resolved_report_paths = [primary_path, latest_path]
        result.artifacts.report_paths = [self._to_manifest_path(path) for path in resolved_report_paths]
        run_event_summary = self._resolve_run_event_summary(result.run_events, result.artifacts)
        result.artifacts.run_event_summary = run_event_summary
        if result.run_events:
            event_log_path = self.reports_dir / f"{result.strategy_id}_{result.run_id}_events.json"
            self.writer.write_json(
                event_log_path,
                {
                    "run_id": result.run_id,
                    "strategy_id": result.strategy_id,
                    "event_count": len(result.run_events),
                    "events": result.run_events,
                },
            )
            result.artifacts.event_log_path = self._to_manifest_path(event_log_path)
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
                "dataset_version_id": result.data_lineage.dataset_version_id,
                "import_run_id": result.data_lineage.import_run_id,
                "import_run_ids": result.data_lineage.import_run_ids,
                "data_source": result.data_lineage.data_source,
                "data_start_date": result.data_lineage.data_start_date,
                "data_end_date": result.data_lineage.data_end_date,
                "dataset_digest": result.data_lineage.dataset_digest,
                "degradation_flags": result.data_lineage.degradation_flags,
                "warnings": result.data_lineage.warnings,
            },
            "data_quality_summary": self._build_data_quality_summary(result.data_quality_events),
            "run_event_summary": run_event_summary,
            "artifacts": self._serialize_artifacts(result.artifacts),
        }
        self.writer.write_json(primary_path, payload)
        self.writer.write_json(latest_path, payload)
        return resolved_report_paths

    def rebuild_backtest_report(self, run_id: str | None = None) -> Path:
        """基于数据库中的回测结果重建报表。

        Args:
            run_id: 指定回测运行 ID；为空时重建最近一次可重建运行。

        Returns:
            主报告路径。

        Raises:
            RuntimeError: 当缺少重建必需仓储依赖时抛出。
            ValueError: 当数据库中不存在目标运行或该运行不可重建时抛出。

        Boundary Behavior:
            - 优先使用 ``run_manifest_json`` 还原 manifest；
            - 若旧运行尚未携带该字段，则回退到历史列与配置快照；
            - benchmark 曲线重建必须优先使用 manifest 中的 ``benchmark_initial_value``，
              若缺失再回退到配置快照中的 ``backtest.initial_cash``；
            - ``ENGINE_COMPLETED`` / ``ARTIFACT_EXPORT_FAILED`` 运行会在重建成功后提升为 ``COMPLETED``。
        """
        if self.run_repository is None or self.account_repository is None or self.order_repository is None:
            raise RuntimeError("ReportService 未注入重建报表所需的 repository")
        rebuildable_statuses = [
            BacktestRunStatus.COMPLETED,
            BacktestRunStatus.ENGINE_COMPLETED,
            BacktestRunStatus.ARTIFACT_EXPORT_FAILED,
        ]
        run = (
            self.run_repository.get_run(run_id)
            if run_id is not None
            else self.run_repository.get_latest_run_by_statuses(rebuildable_statuses)
        )
        if run is None:
            if run_id is None:
                raise ValueError("数据库中不存在可重建的回测运行")
            raise ValueError(f"找不到指定 run_id 的回测运行: {run_id}")
        if not run.status.rebuildable:
            raise ValueError(f"run_id={run.run_id} 当前状态={run.status.value}，不可重建报告")
        trade_dates, equity_curve = self.account_repository.load_equity_curve(run.run_id)
        config_snapshot = json.loads(run.config_snapshot_json)
        benchmark_symbol = config_snapshot.get("backtest", {}).get("benchmark_symbol")
        manifest = self._load_run_manifest(run)
        benchmark_initial_value = manifest.benchmark_initial_value
        if benchmark_initial_value is None:
            benchmark_initial_value = self._coerce_float(config_snapshot.get("backtest", {}).get("initial_cash"))
        benchmark_curve = self._rebuild_benchmark_curve(trade_dates, benchmark_symbol, benchmark_initial_value)
        metrics_payload = self._build_metrics_payload(equity_curve, benchmark_curve)
        quality_events = self._load_quality_events(run.import_run_id)
        run_events = self._load_run_events(run, manifest)
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
                dataset_version_id=run.dataset_version_id,
                import_run_id=run.import_run_id,
                import_run_ids=json.loads(run.import_run_ids_json or "[]"),
                data_source=run.data_source or "database_snapshot",
                data_start_date=run.data_start_date,
                data_end_date=run.data_end_date,
                dataset_digest=run.dataset_digest,
                degradation_flags=json.loads(run.degradation_flags_json or "[]"),
                warnings=json.loads(run.warnings_json or "[]"),
            ),
            artifacts=manifest,
            run_events=run_events,
            data_quality_events=quality_events,
        )
        result.artifacts.artifact_status = "GENERATED"
        result.artifacts.artifact_errors = []
        result.artifacts.artifact_completed_at = now_iso()
        try:
            report_paths = self.write_backtest_report(result)
        except Exception as exc:
            result.artifacts.artifact_status = "FAILED"
            result.artifacts.artifact_errors = [str(exc)]
            result.artifacts.artifact_completed_at = now_iso()
            self.run_repository.finish_run(
                run.run_id,
                BacktestRunStatus.ARTIFACT_EXPORT_FAILED,
                error_message=str(exc),
                run_manifest=result.artifacts,
                run_events=result.run_events,
                overwrite_error_message=True,
            )
            raise
        result.report_path = str(report_paths[0])
        self.run_repository.finish_run(
            run.run_id,
            BacktestRunStatus.COMPLETED,
            error_message=None,
            report_path=str(report_paths[0]),
            report_artifacts=result.artifacts.report_paths,
            run_manifest=result.artifacts,
            run_events=result.run_events,
            overwrite_error_message=True,
        )
        return report_paths[0]

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

    def _rebuild_benchmark_curve(
        self,
        trade_dates: list,
        benchmark_symbol: str | None,
        benchmark_initial_value: float | None,
    ) -> list[float]:
        """按给定基准资产重建 benchmark 曲线。"""
        if self.market_repository is None or not benchmark_symbol or not trade_dates:
            return []
        initial_value = self._coerce_float(benchmark_initial_value)
        if initial_value is None or initial_value <= 0:
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
        last_value = initial_value
        for trade_date in trade_dates:
            price = by_date.get(trade_date)
            if price is not None:
                last_value = initial_value * (price / first_price)
            curve.append(last_value)
        return curve

    def _load_run_manifest(self, run) -> RunArtifacts:
        payload: dict[str, Any] = {}
        if getattr(run, "run_manifest_json", None):
            try:
                parsed = json.loads(run.run_manifest_json)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                payload = parsed
        if not payload:
            payload = {
                "entrypoint": run.entrypoint,
                "strategy_version": run.strategy_version,
                "runtime_mode": run.runtime_mode,
                "report_paths": json.loads(run.report_artifacts_json or "[]"),
            }
        return RunArtifacts(
            schema_version=int(payload.get("schema_version", 1) or 1),
            entrypoint=payload.get("entrypoint") or run.entrypoint,
            strategy_version=payload.get("strategy_version") or run.strategy_version,
            runtime_mode=payload.get("runtime_mode") or run.runtime_mode,
            benchmark_initial_value=self._coerce_float(payload.get("benchmark_initial_value")),
            report_paths=list(payload.get("report_paths") or json.loads(run.report_artifacts_json or "[]")),
            event_log_path=payload.get("event_log_path"),
            run_event_summary=dict(payload.get("run_event_summary") or {}),
            artifact_status=str(payload.get("artifact_status") or ("GENERATED" if run.status == BacktestRunStatus.COMPLETED else "PENDING")),
            artifact_errors=list(payload.get("artifact_errors") or ([] if run.error_message is None else [run.error_message])),
            engine_completed_at=payload.get("engine_completed_at") or run.finished_at,
            artifact_completed_at=payload.get("artifact_completed_at") or run.finished_at,
            component_manifest=dict(payload.get("component_manifest") or {}),
            promotion_package=dict(payload.get("promotion_package") or {}),
            signal_source_run_id=payload.get("signal_source_run_id"),
            signal_source_artifact_type=payload.get("signal_source_artifact_type"),
        )

    def _load_quality_events(self, import_run_id: str | None) -> list[dict[str, Any]]:
        if self.data_import_repository is None or not import_run_id:
            return []
        rows = self.data_import_repository.list_quality_events(import_run_id)
        events: list[dict[str, Any]] = []
        for item in rows:
            payload = item.get("payload_json")
            try:
                parsed_payload = json.loads(payload) if isinstance(payload, str) else payload
            except json.JSONDecodeError:
                parsed_payload = {"raw_payload": payload}
            events.append(
                {
                    "event_type": item.get("event_type"),
                    "level": item.get("level"),
                    "created_at": item.get("created_at"),
                    "payload": parsed_payload,
                }
            )
        return events

    def _load_run_events(self, run, manifest: RunArtifacts) -> list[dict[str, Any]]:
        """恢复运行事件完整明细。

        Args:
            run: ``BacktestRun`` 记录；要求至少包含 ``run_events_json`` 字段。
            manifest: 当前运行对应的产物 manifest。

        Returns:
            运行事件明细列表。

        Boundary Behavior:
            - 优先返回数据库中的 ``run_events_json``，确保完整事件明细为数据库内生资产；
            - 若历史 run 尚未持久化该字段，则回退读取 sidecar 事件日志；
            - 任一路径解析失败时返回空列表，而不是抛出重建异常。
        """
        stored_events = getattr(run, "run_events_json", None)
        if isinstance(stored_events, str) and stored_events.strip():
            try:
                parsed = json.loads(stored_events)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list) and parsed:
                return list(parsed)
        if not manifest.event_log_path:
            return []
        path = self._resolve_manifest_path(manifest.event_log_path)
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        events = payload.get("events", [])
        return list(events) if isinstance(events, list) else []


    def _to_manifest_path(self, path: str | Path) -> str:
        """将绝对运行时路径转换为相对 reports_dir 的可迁移路径。

        Args:
            path: 实际写入磁盘的路径。

        Returns:
            若目标位于 ``reports_dir`` 下，则返回相对路径；否则返回绝对路径字符串。
        """
        candidate = Path(path)
        try:
            return str(candidate.resolve().relative_to(self.reports_dir.resolve()))
        except ValueError:
            return str(candidate)

    def _resolve_manifest_path(self, path_value: str) -> Path:
        """将 manifest 中记录的相对/绝对路径解析为可读取路径。

        Args:
            path_value: manifest 中的路径字符串。

        Returns:
            可直接访问的 ``Path`` 对象。
        """
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.reports_dir / candidate

    @classmethod
    def _resolve_run_event_summary(cls, run_events: list[dict[str, Any]], artifacts: RunArtifacts) -> dict[str, Any]:
        """解析运行事件摘要。

        优先使用当前 ``run_events`` 计算；当重建场景缺少 sidecar 事件日志时，
        回退到 manifest 中已持久化的摘要，避免静默丢失事件统计。
        """
        computed = cls._build_run_event_summary(run_events)
        if computed.get("event_count", 0) > 0:
            return computed
        if artifacts.run_event_summary:
            return dict(artifacts.run_event_summary)
        return computed

    @staticmethod
    def _build_run_event_summary(run_events: list[dict[str, Any]]) -> dict[str, Any]:
        summary: dict[str, Any] = {"event_count": len(run_events), "by_type": {}}
        for event in run_events:
            event_type = str(event.get("type") or "UNKNOWN")
            summary["by_type"][event_type] = summary["by_type"].get(event_type, 0) + 1
        return summary

    @staticmethod
    def _build_data_quality_summary(data_quality_events: list[dict[str, Any]]) -> dict[str, Any]:
        summary: dict[str, Any] = {"event_count": len(data_quality_events), "by_level": {}, "by_type": {}}
        for event in data_quality_events:
            level = str(event.get("level") or "UNKNOWN")
            event_type = str(event.get("event_type") or "UNKNOWN")
            summary["by_level"][level] = summary["by_level"].get(level, 0) + 1
            summary["by_type"][event_type] = summary["by_type"].get(event_type, 0) + 1
        return summary

    @staticmethod
    def _serialize_artifacts(artifacts: RunArtifacts) -> dict[str, Any]:
        return {
            "schema_version": artifacts.schema_version,
            "entrypoint": artifacts.entrypoint,
            "strategy_version": artifacts.strategy_version,
            "runtime_mode": artifacts.runtime_mode,
            "benchmark_initial_value": artifacts.benchmark_initial_value,
            "report_paths": artifacts.report_paths,
            "event_log_path": artifacts.event_log_path,
            "run_event_summary": artifacts.run_event_summary,
            "artifact_status": artifacts.artifact_status,
            "artifact_errors": artifacts.artifact_errors,
            "engine_completed_at": artifacts.engine_completed_at,
            "artifact_completed_at": artifacts.artifact_completed_at,
            "component_manifest": artifacts.component_manifest,
            "promotion_package": artifacts.promotion_package,
            "signal_source_run_id": artifacts.signal_source_run_id,
            "signal_source_artifact_type": artifacts.signal_source_artifact_type,
        }

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
