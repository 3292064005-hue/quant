"""报表服务。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from a_share_quant.core.metrics import compute_metrics, compute_relative_metrics
from a_share_quant.core.reporting import ReportWriter
from a_share_quant.contracts.versioned_contracts import parse_run_manifest_contract
from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import BacktestResult, BacktestRunStatus, DataLineage, RunArtifacts
from a_share_quant.execution.order_lifecycle_service import OrderLifecycleEventService
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.services.report_rebuild_service import ReportRebuildService
from a_share_quant.services.report_write_service import ReportWriteService


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
        self.lifecycle_service = OrderLifecycleEventService()
        self.write_service = ReportWriteService(self)
        self.rebuild_service = ReportRebuildService(self)

    def write_backtest_report(self, result: BacktestResult) -> list[Path]:
        """写出回测报告，并返回所有报告主产物路径。"""
        return self.write_service.write_backtest_report(result)

    def rebuild_backtest_report(self, run_id: str | None = None) -> Path:
        """基于数据库中的回测结果重建报表。"""
        return self.rebuild_service.rebuild_backtest_report(run_id)

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
                "schema_version": 6,
                "entrypoint": run.entrypoint,
                "strategy_version": run.strategy_version,
                "runtime_mode": run.runtime_mode,
                "report_paths": json.loads(run.report_artifacts_json or "[]"),
            }
        payload.setdefault("schema_version", 6)
        payload.setdefault("entrypoint", run.entrypoint)
        payload.setdefault("strategy_version", run.strategy_version)
        payload.setdefault("runtime_mode", run.runtime_mode)
        payload.setdefault("report_paths", json.loads(run.report_artifacts_json or "[]"))
        payload.setdefault("artifact_status", "GENERATED" if run.status == BacktestRunStatus.COMPLETED else "PENDING")
        payload.setdefault("artifact_errors", [] if run.error_message is None else [run.error_message])
        payload.setdefault("engine_completed_at", run.finished_at)
        payload.setdefault("artifact_completed_at", run.finished_at)
        contract = parse_run_manifest_contract(payload)
        return RunArtifacts(
            schema_version=contract.schema_version,
            entrypoint=contract.entrypoint,
            strategy_version=contract.strategy_version,
            runtime_mode=contract.runtime_mode,
            benchmark_initial_value=self._coerce_float(contract.benchmark_initial_value),
            report_paths=list(contract.report_paths),
            report_artifacts=[item.model_dump(mode="python") for item in contract.report_artifacts],
            event_log_path=contract.event_log_path,
            run_event_summary=contract.run_event_summary.model_dump(mode="python"),
            artifact_status=str(contract.artifact_status),
            artifact_errors=list(contract.artifact_errors),
            engine_completed_at=contract.engine_completed_at,
            artifact_completed_at=contract.artifact_completed_at,
            component_manifest=contract.component_manifest.model_dump(mode="python"),
            promotion_package=(contract.promotion_package.model_dump(mode="python") if contract.promotion_package is not None else {}),
            signal_source_run_id=contract.signal_source_run_id,
            signal_source_artifact_type=contract.signal_source_artifact_type,
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
        contract = parse_run_manifest_contract(
            {
                "schema_version": int(artifacts.schema_version or 6),
                "entrypoint": artifacts.entrypoint,
                "strategy_version": artifacts.strategy_version,
                "runtime_mode": artifacts.runtime_mode,
                "benchmark_initial_value": artifacts.benchmark_initial_value,
                "report_paths": list(artifacts.report_paths),
                "report_artifacts": list(artifacts.report_artifacts),
                "event_log_path": artifacts.event_log_path,
                "run_event_summary": dict(artifacts.run_event_summary),
                "artifact_status": artifacts.artifact_status,
                "artifact_errors": list(artifacts.artifact_errors),
                "engine_completed_at": artifacts.engine_completed_at,
                "artifact_completed_at": artifacts.artifact_completed_at,
                "component_manifest": dict(artifacts.component_manifest),
                "promotion_package": dict(artifacts.promotion_package) or None,
                "signal_source_run_id": artifacts.signal_source_run_id,
                "signal_source_artifact_type": artifacts.signal_source_artifact_type,
            }
        )
        return contract.model_dump(mode="python")

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
