"""latest snapshot read-model use-case。"""
from __future__ import annotations

from typing import Any

from a_share_quant.services.read_model_source_of_truth import build_snapshot_source_of_truth
from a_share_quant.services.ui_read_models import build_recent_research_run_projection


class LatestRunSnapshotService:
    def __init__(self, owner) -> None:
        self.owner = owner

    def build_latest_snapshot(self) -> dict[str, Any]:
        latest_import = self.owner.data_import_repository.get_latest_run()
        latest_run = self.owner.backtest_run_repository.get_latest_run()
        latest_run_import_run_id = latest_run.import_run_id if latest_run is not None else None
        recent_research_runs = self.owner.research_run_repository.list_recent(limit=10)
        payload: dict[str, Any] = {
            "latest_import_run": self.owner._serialize_import_run(latest_import) if latest_import is not None else None,
            "latest_import_quality_events": self.owner._load_quality_events(latest_import.import_run_id) if latest_import is not None else [],
            "latest_backtest_run": self.owner._serialize_backtest_run(latest_run) if latest_run is not None else None,
            "latest_execution_summary": self.owner._build_execution_summary(latest_run.run_id) if latest_run is not None else self.owner._empty_execution_summary(),
            "latest_risk_alerts": self.owner._build_risk_summary(latest_run.run_id, latest_run_import_run_id) if latest_run is not None else self.owner._empty_risk_summary(latest_import.import_run_id if latest_import else None),
            "recent_research_runs": recent_research_runs,
            "recent_research_run_summaries": build_recent_research_run_projection(recent_research_runs),
            "latest_operator_session": self.owner._build_latest_operator_session(),
            "recent_runtime_events": self.owner.runtime_event_repository.list_recent(limit=50) if self.owner.runtime_event_repository is not None else [],
        }
        payload["latest_report_replay_summary"] = self.owner._build_report_replay_summary(latest_run) if latest_run is not None else None
        payload["source_of_truth"] = build_snapshot_source_of_truth()
        return payload
