"""运行查询服务。"""
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.contracts.versioned_contracts import parse_run_manifest_contract
from a_share_quant.domain.models import BacktestRun, DataImportRun
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.runtime_event_repository import RuntimeEventRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.execution.order_lifecycle_service import OrderLifecycleEventService
from a_share_quant.services.run_query_operator_snapshot_service import OperatorSnapshotService
from a_share_quant.services.run_query_snapshot_service import LatestRunSnapshotService
from a_share_quant.services.ui_read_models import build_recent_research_run_projection


class RunQueryService:
    """统一聚合运行期只读查询模型。

    该服务用于 CLI snapshot、桌面 UI、报告/回放摘要等读路径，避免上层再以脚本方式
    拼接分散仓储查询结果。
    """

    def __init__(
        self,
        *,
        backtest_run_repository: BacktestRunRepository,
        order_repository: OrderRepository,
        audit_repository: AuditRepository,
        data_import_repository: DataImportRepository,
        research_run_repository: ResearchRunRepository,
        execution_session_repository: ExecutionSessionRepository | None = None,
        account_repository: AccountRepository | None = None,
        runtime_event_repository: RuntimeEventRepository | None = None,
    ) -> None:
        self.backtest_run_repository = backtest_run_repository
        self.order_repository = order_repository
        self.audit_repository = audit_repository
        self.data_import_repository = data_import_repository
        self.research_run_repository = research_run_repository
        self.execution_session_repository = execution_session_repository
        self.account_repository = account_repository
        if runtime_event_repository is not None:
            self.runtime_event_repository = runtime_event_repository
        elif execution_session_repository is not None and hasattr(execution_session_repository, "runtime_event_repository"):
            self.runtime_event_repository = getattr(execution_session_repository, "runtime_event_repository")
        else:
            self.runtime_event_repository = runtime_event_repository
        self.snapshot_service = LatestRunSnapshotService(self)
        self.operator_snapshot_service = OperatorSnapshotService(self)

    def build_latest_snapshot(self) -> dict[str, Any]:
        """构建最近一次导入/研究/回测的统一只读快照。"""
        return self.snapshot_service.build_latest_snapshot()

    def build_operator_snapshot(
        self,
        *,
        broker: LiveBrokerPort,
        runtime_mode: str,
        broker_provider: str,
        default_account_id: str | None,
        allowed_account_ids: list[str],
        event_source_mode: str,
        supervisor_config: dict[str, Any],
        runtime_checks: list[dict[str, Any]],
        capability_summary: dict[str, Any],
    ) -> dict[str, Any]:
        """构建统一 operator 只读快照。"""
        return self.operator_snapshot_service.build_operator_snapshot(
            broker=broker,
            runtime_mode=runtime_mode,
            broker_provider=broker_provider,
            default_account_id=default_account_id,
            allowed_account_ids=allowed_account_ids,
            event_source_mode=event_source_mode,
            supervisor_config=supervisor_config,
            runtime_checks=runtime_checks,
            capability_summary=capability_summary,
        )

    @staticmethod
    def _get_account_snapshot(broker: LiveBrokerPort, account_id: str | None):
        getter = getattr(broker, "get_account_snapshot", None)
        if callable(getter):
            return getter(account_id=account_id, last_prices=None)
        return broker.get_account(last_prices=None)

    @staticmethod
    def _get_position_snapshots(broker: LiveBrokerPort, account_id: str | None):
        getter = getattr(broker, "get_position_snapshots", None)
        if callable(getter):
            return list(getter(account_id=account_id, last_prices=None))
        return list(broker.get_positions(last_prices=None))

    @staticmethod
    def _query_orders_scoped(broker: LiveBrokerPort, account_id: str | None):
        getter = getattr(broker, "query_orders_scoped", None)
        if callable(getter):
            return list(getter(account_id=account_id))
        orders = list(broker.query_orders())
        if account_id is None:
            return orders
        return [item for item in orders if getattr(item, "account_id", None) in {None, "", account_id}]

    @staticmethod
    def _query_trades_scoped(broker: LiveBrokerPort, account_id: str | None):
        getter = getattr(broker, "query_trades_scoped", None)
        if callable(getter):
            return list(getter(account_id=account_id))
        fills = list(broker.query_trades())
        if account_id is None:
            return fills
        return [item for item in fills if getattr(item, "account_id", None) in {None, "", account_id}]

    def _serialize_backtest_run(self, run: BacktestRun) -> dict[str, Any]:
        manifest = self._load_manifest(run)
        return {
            "run_id": run.run_id,
            "strategy_id": run.strategy_id,
            "status": run.status.value,
            "status_breakdown": {
                "business_status": ("COMPLETED" if run.status.business_complete else run.status.value),
                "artifact_status": manifest.get("artifact_status") or ("GENERATED" if run.status == run.status.COMPLETED else "PENDING"),
                "rebuildable": run.status.rebuildable,
            },
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "entrypoint": run.entrypoint,
            "runtime_mode": run.runtime_mode,
            "dataset_version_id": run.dataset_version_id,
            "import_run_id": run.import_run_id,
            "dataset_digest": run.dataset_digest,
            "report_artifacts": manifest.get("report_artifacts") or json.loads(run.report_artifacts_json or "[]"),
            "run_manifest": manifest,
        }

    def _serialize_import_run(self, import_run: DataImportRun) -> dict[str, Any]:
        return {
            "import_run_id": import_run.import_run_id,
            "source": import_run.source,
            "status": import_run.status,
            "started_at": import_run.started_at,
            "finished_at": import_run.finished_at,
            "degradation_flags": json.loads(import_run.degradation_flags_json or "[]"),
            "warnings": json.loads(import_run.warnings_json or "[]"),
            "securities_count": import_run.securities_count,
            "calendar_count": import_run.calendar_count,
            "bars_count": import_run.bars_count,
        }

    def _load_quality_events(self, import_run_id: str | None) -> list[dict[str, Any]]:
        if not import_run_id:
            return []
        rows = self.data_import_repository.list_quality_events(import_run_id)
        events: list[dict[str, Any]] = []
        for row in rows:
            payload: dict[str, Any] = dict(row)
            try:
                payload["payload"] = json.loads(payload.pop("payload_json") or "{}")
            except json.JSONDecodeError:
                raw_payload = payload.pop("payload_json", "")
                payload["payload"] = {"raw": raw_payload}
            events.append(payload)
        return events

    def _build_execution_summary(self, run_id: str) -> dict[str, Any]:
        orders = self.order_repository.list_orders(run_id, limit=50)
        fills = self.order_repository.list_fills(run_id, limit=50)
        order_status_counts: dict[str, int] = {}
        for row in orders:
            order_status_counts[row["status"]] = order_status_counts.get(row["status"], 0) + 1
        fill_notional = sum(float(row["fill_price"]) * int(row["fill_quantity"]) for row in fills)
        return {
            "run_id": run_id,
            "order_count": self.order_repository.count_orders(run_id),
            "fill_count": self.order_repository.count_fills(run_id),
            "order_status_counts": order_status_counts,
            "recent_orders": orders,
            "recent_fills": fills,
            "fill_notional": fill_notional,
        }

    def _build_risk_summary(self, run_id: str, import_run_id: str | None) -> dict[str, Any]:
        logs = self.audit_repository.list_logs(run_id, limit=100, modules=("risk_engine", "execution", "backtest"))
        normalized_logs: list[dict[str, Any]] = []
        module_counts: dict[str, int] = {}
        level_counts: dict[str, int] = {}
        for row in logs:
            item = dict(row)
            module_counts[item["module"]] = module_counts.get(item["module"], 0) + 1
            level_counts[item["level"]] = level_counts.get(item["level"], 0) + 1
            try:
                item["payload"] = json.loads(item.pop("payload_json") or "{}")
            except json.JSONDecodeError:
                item["payload"] = {"raw": item.pop("payload_json", "")}
            normalized_logs.append(item)
        import_events = self._load_quality_events(import_run_id)
        return {
            "run_id": run_id,
            "import_run_id": import_run_id,
            "audit_log_count": len(normalized_logs),
            "audit_module_counts": module_counts,
            "audit_level_counts": level_counts,
            "risk_audit_logs": normalized_logs,
            "import_quality_events": import_events,
        }

    def _build_report_replay_summary(self, run: BacktestRun) -> dict[str, Any]:
        manifest = self._load_manifest(run)
        bound_research_run_id = manifest.get("signal_source_run_id")
        bound_research_run = self.research_run_repository.get(bound_research_run_id) if bound_research_run_id else None
        causal_research_runs: list[dict[str, Any]] = []
        if bound_research_run is not None:
            causal_research_runs.append(self._build_lineage_reference(bound_research_run, binding_mode="signal_source"))
        seen_research_run_ids = {item["research_run_id"] for item in causal_research_runs}
        related_source_rows = []
        if bound_research_run is not None:
            related_source_rows = self.research_run_repository.list_related_via_edges(
                bound_research_run["research_run_id"],
                edge_kinds=["related_by_dataset", "promoted_from", "references"],
                limit=100,
            )
            related_source_rows = [item for item in related_source_rows if item["research_run_id"] not in seen_research_run_ids]
        if not related_source_rows:
            related_source_rows = self.research_run_repository.list_related_by_dataset(
                dataset_version_id=run.dataset_version_id,
                dataset_digest=run.dataset_digest,
                exclude_research_run_ids=list(seen_research_run_ids),
                limit=100,
                primary_only=True,
            )
        related_research_runs = [
            self._build_lineage_reference(item, binding_mode="related_by_dataset")
            for item in related_source_rows
            if item["research_run_id"] not in seen_research_run_ids
        ]
        lineage_graph = {
            "import_run_id": run.import_run_id,
            "dataset_version_id": run.dataset_version_id,
            "dataset_digest": run.dataset_digest,
            "signal_source_run_id": bound_research_run_id,
            "signal_source_artifact_type": manifest.get("signal_source_artifact_type"),
            "research_runs": causal_research_runs,
            "causal_research_runs": causal_research_runs,
            "related_research_runs": related_research_runs,
            "backtest_run_id": run.run_id,
            "report_artifacts": manifest.get("report_artifacts") or json.loads(run.report_artifacts_json or "[]"),
        }
        return {
            "run_id": run.run_id,
            "report_path": run.report_path,
            "report_artifacts": manifest.get("report_artifacts") or json.loads(run.report_artifacts_json or "[]"),
            "run_event_summary": manifest.get("run_event_summary", {}),
            "event_log_path": manifest.get("event_log_path"),
            "artifact_status": manifest.get("artifact_status"),
            "artifact_errors": list(manifest.get("artifact_errors") or []),
            "component_manifest": dict(manifest.get("component_manifest") or {}),
            "promotion_package": dict(manifest.get("promotion_package") or {}),
            "signal_source_run_id": manifest.get("signal_source_run_id"),
            "signal_source_artifact_type": manifest.get("signal_source_artifact_type"),
            "related_research_run_summaries": build_recent_research_run_projection(related_research_runs),
            "lineage_graph": lineage_graph,
        }

    @staticmethod
    def _build_lineage_reference(row: dict[str, Any], *, binding_mode: str) -> dict[str, Any]:
        return {
            "research_run_id": row["research_run_id"],
            "artifact_type": row["artifact_type"],
            "dataset_version_id": row.get("dataset_version_id"),
            "dataset_digest": row.get("dataset_digest"),
            "research_session_id": row.get("research_session_id"),
            "step_name": row.get("step_name"),
            "binding_mode": binding_mode,
            "request": row.get("request") or {},
            "result": row.get("result") or {},
            "created_at": row.get("created_at"),
            "is_primary_run": row.get("is_primary_run", True),
        }

    def _load_manifest(self, run: BacktestRun) -> dict[str, Any]:
        try:
            payload = json.loads(run.run_manifest_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        if is_dataclass(payload) and not isinstance(payload, type):
            payload = asdict(payload)
        if not isinstance(payload, dict):
            payload = {}
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
        payload.setdefault("report_artifacts", [
            {"role": "primary" if index == 0 else f"report_copy_{index}", "path": path, "kind": "report", "format": "json", "primary": index == 0}
            for index, path in enumerate(payload.get("report_paths") or [])
        ])
        return parse_run_manifest_contract(payload).model_dump(mode="python")

    def _build_latest_operator_session(self) -> dict[str, Any] | None:
        if self.execution_session_repository is None:
            return None
        session = self.execution_session_repository.get_latest()
        if session is None:
            return None
        lifecycle_service = OrderLifecycleEventService()
        session_events = self.execution_session_repository.list_events(session.session_id, limit=200)
        normalized_events = [lifecycle_service.normalize_trade_command_event(item) for item in session_events]
        lifecycle_snapshot = lifecycle_service.replay_lifecycle_events(session_events)
        observability = self._build_operator_observability_summary(session, runtime_events=normalized_events)
        return {
            "session_id": session.session_id,
            "runtime_mode": session.runtime_mode,
            "broker_provider": session.broker_provider,
            "command_type": session.command_type,
            "command_source": session.command_source,
            "requested_by": session.requested_by,
            "account_id": session.account_id,
            "status": session.status.value,
            "idempotency_key": session.idempotency_key,
            "requested_trade_date": session.requested_trade_date,
            "risk_summary": session.risk_summary,
            "order_count": session.order_count,
            "submitted_count": session.submitted_count,
            "rejected_count": session.rejected_count,
            "error_message": session.error_message,
            "broker_event_cursor": session.broker_event_cursor,
            "last_synced_at": session.last_synced_at,
            "supervisor_owner": session.supervisor_owner,
            "supervisor_lease_expires_at": session.supervisor_lease_expires_at,
            "supervisor_mode": session.supervisor_mode,
            "last_supervised_at": session.last_supervised_at,
            "recent_orders": self.order_repository.list_orders(execution_session_id=session.session_id, limit=20),
            "recent_fills": self.order_repository.list_fills(execution_session_id=session.session_id, limit=20),
            "events": normalized_events,
            "lifecycle_snapshot": asdict(lifecycle_snapshot) if lifecycle_snapshot is not None else None,
            "observability": observability,
        }

    def _build_operator_observability_summary(self, session: Any, *, runtime_events: list[dict[str, Any]]) -> dict[str, Any]:
        """汇总 operator 会话降级、supervisor 与 reconcile 可观测性指标。"""
        normalized_events = []
        lifecycle_service = OrderLifecycleEventService()
        for event in runtime_events:
            if "payload" in event and "event_type" in event:
                normalized_events.append(lifecycle_service.normalize_runtime_event(event))
        event_type_counts: dict[str, int] = {}
        level_counts: dict[str, int] = {}
        degraded_events: list[dict[str, Any]] = []
        supervisor_event_count = 0
        reconcile_event_count = 0
        audit_write_failures = 0
        recovery_retry_failures = 0
        for event in normalized_events:
            event_type = str(event.get("event_type") or "UNKNOWN")
            level = str(event.get("level") or "INFO")
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            level_counts[level] = level_counts.get(level, 0) + 1
            if event_type.startswith("SUPERVISOR_"):
                supervisor_event_count += 1
            if event_type.startswith("RECOVERY_") or event_type in {"SESSION_SYNC_COMPLETED", "RECOVERY_REQUIRED"}:
                reconcile_event_count += 1
            is_degraded = level in {"ERROR", "WARN"} or event_type.endswith("FAILED") or event_type in {"AUDIT_WRITE_FAILED", "SUPERVISOR_ERROR", "RECOVERY_RETRY_FAILED"}
            if event_type == "AUDIT_WRITE_FAILED":
                audit_write_failures += 1
            if event_type == "RECOVERY_RETRY_FAILED":
                recovery_retry_failures += 1
            if is_degraded:
                degraded_events.append({"event_id": event.get("event_id"), "event_type": event_type, "level": level, "created_at": event.get("created_at") or event.get("occurred_at"), "payload": dict(event.get("payload") or {})})
        degraded_events.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return {
            "session_id": getattr(session, "session_id", None) if session is not None and not isinstance(session, dict) else (session or {}).get("session_id"),
            "total_event_count": len(normalized_events),
            "event_type_counts": event_type_counts,
            "level_counts": level_counts,
            "degraded_event_count": len(degraded_events),
            "audit_write_failure_count": audit_write_failures,
            "recovery_retry_failure_count": recovery_retry_failures,
            "supervisor_event_count": supervisor_event_count,
            "reconcile_event_count": reconcile_event_count,
            "recent_degraded_events": degraded_events[:10],
        }

    @staticmethod
    def _empty_execution_summary() -> dict[str, Any]:
        return {
            "run_id": None,
            "order_count": 0,
            "fill_count": 0,
            "order_status_counts": {},
            "recent_orders": [],
            "recent_fills": [],
            "fill_notional": 0.0,
        }

    @staticmethod
    def _empty_risk_summary(import_run_id: str | None) -> dict[str, Any]:
        return {
            "run_id": None,
            "import_run_id": import_run_id,
            "audit_log_count": 0,
            "audit_module_counts": {},
            "audit_level_counts": {},
            "risk_audit_logs": [],
            "import_quality_events": [],
        }
