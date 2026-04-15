"""operator snapshot read-model use-case。"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.services.read_model_source_of_truth import build_snapshot_source_of_truth


class OperatorSnapshotService:
    def __init__(self, owner) -> None:
        self.owner = owner

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
        latest_runs = self.owner.snapshot_service.build_latest_snapshot()
        session_account_id = None
        latest_session = latest_runs.get("latest_operator_session") or {}
        if isinstance(latest_session, dict):
            session_account_id = latest_session.get("account_id")
        configured_accounts: list[str | None] = []
        explicit_accounts: list[str] = []
        for candidate in [default_account_id, session_account_id, *(allowed_account_ids or [])]:
            normalized = str(candidate).strip() if candidate is not None else None
            if not normalized:
                continue
            if normalized not in explicit_accounts:
                explicit_accounts.append(normalized)
        if explicit_accounts:
            configured_accounts.extend(explicit_accounts)
        else:
            configured_accounts.append(None)
        account_views: list[dict[str, Any]] = []
        for scoped_account_id in configured_accounts:
            persisted_account = self.owner.account_repository.load_latest_operator_account_snapshot(account_id=scoped_account_id) if self.owner.account_repository is not None else None
            persisted_positions = self.owner.account_repository.load_latest_operator_position_snapshots(account_id=scoped_account_id, capture_id=str(persisted_account["capture_id"]) if persisted_account is not None else None) if self.owner.account_repository is not None else []
            account_snapshot = self.owner._get_account_snapshot(broker, scoped_account_id)
            positions = self.owner._get_position_snapshots(broker, scoped_account_id)
            orders = self.owner._query_orders_scoped(broker, scoped_account_id)
            fills = self.owner._query_trades_scoped(broker, scoped_account_id)
            account_views.append(
                {
                    "account_id": scoped_account_id,
                    "account": asdict(account_snapshot),
                    "positions": [asdict(item) for item in positions],
                    "orders": [asdict(item) for item in orders],
                    "fills": [asdict(item) for item in fills],
                    "persisted_account": persisted_account,
                    "persisted_positions": persisted_positions,
                }
            )
        primary_view = next((item for item in account_views if item["account_id"] == default_account_id), account_views[0] if account_views else None)
        operator_runtime_events = self.owner.runtime_event_repository.list_recent(source_domain="operator", limit=50) if self.owner.runtime_event_repository is not None else []
        observability = self.owner._build_operator_observability_summary(latest_runs.get("latest_operator_session"), runtime_events=operator_runtime_events)
        return {
            "runtime_mode": runtime_mode,
            "broker_provider": broker_provider,
            "default_account_id": default_account_id,
            "allowed_account_ids": allowed_account_ids,
            "event_source_mode": event_source_mode,
            "supervisor": supervisor_config,
            "runtime_checks": runtime_checks,
            "capability_summary": capability_summary,
            "account": primary_view.get("account") if primary_view else None,
            "positions": primary_view.get("positions") if primary_view else [],
            "orders": primary_view.get("orders") if primary_view else [],
            "fills": primary_view.get("fills") if primary_view else [],
            "account_views": account_views,
            "latest_runs": latest_runs,
            "recent_runtime_events": operator_runtime_events,
            "observability": observability,
            "source_of_truth": build_snapshot_source_of_truth(operator_mode=True),
        }
