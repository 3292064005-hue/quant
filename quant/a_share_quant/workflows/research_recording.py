"""research workflow 持久化/谱系辅助组件。"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from a_share_quant.core.utils import new_id


class ResearchRecordingService:
    """抽离 experiment/batch 记录落库与谱系重绑逻辑。"""

    def __init__(self, research_run_repository, persist_artifact: Callable[..., dict[str, Any]]) -> None:
        self.research_run_repository = research_run_repository
        self.persist_artifact = persist_artifact

    def persist_experiment_bundle(
        self,
        *,
        request: dict[str, Any],
        summary,
        dataset_request: dict[str, Any],
        dataset_result: dict[str, Any],
        feature_snapshot,
        signal_snapshot,
    ) -> dict[str, Any]:
        """持久化 experiment 主记录及 dataset/feature/signal 子步骤。"""
        from a_share_quant.workflows.research_models import ResearchPersistSpec

        research_session_id = str((summary.experiment or {}).get("research_session_id") or new_id("research_session"))
        experiment_payload = self.persist_artifact(
            artifact_type="experiment_summary",
            request=request,
            result=summary.to_dict(),
            persist_spec=ResearchPersistSpec(
                research_session_id=research_session_id,
                step_name="experiment_summary",
                is_primary_run=True,
            ),
        )
        experiment_run_id = experiment_payload["research_run_id"]
        child_spec = ResearchPersistSpec(
            research_session_id=research_session_id,
            parent_research_run_id=experiment_run_id,
            root_research_run_id=experiment_run_id,
            is_primary_run=False,
        )
        dataset_payload = self.persist_artifact(
            artifact_type="dataset_summary",
            request=dataset_request,
            result=dataset_result,
            persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "dataset_summary"}),
        )
        feature_payload = self.persist_artifact(
            artifact_type="feature_snapshot",
            request=feature_snapshot.request,
            result=feature_snapshot.result,
            persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "feature_snapshot"}),
        )
        signal_payload = self.persist_artifact(
            artifact_type="signal_snapshot",
            request=signal_snapshot.request,
            result=signal_snapshot.result,
            persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "signal_snapshot"}),
        )
        experiment_section = dict(experiment_payload.get("experiment") or {})
        experiment_section["artifact_lineage"] = {
            "dataset_summary_run_id": dataset_payload["research_run_id"],
            "feature_snapshot_run_id": feature_payload["research_run_id"],
            "signal_snapshot_run_id": signal_payload["research_run_id"],
        }
        experiment_payload["experiment"] = experiment_section
        return experiment_payload

    def rebind_batch_lineage(
        self,
        *,
        batch_run_id: str,
        batch_session_id: str,
        task_name: str,
        experiment_result: dict[str, Any],
    ) -> dict[str, Any]:
        """把单个 experiment 重挂到 batch 主记录下。"""
        experiment_run_id = str(experiment_result["research_run_id"])
        artifact_lineage = ((experiment_result.get("experiment") or {}).get("artifact_lineage") or {})
        self.research_run_repository.update_lineage(
            experiment_run_id,
            research_session_id=batch_session_id,
            parent_research_run_id=batch_run_id,
            root_research_run_id=batch_run_id,
            step_name=f"experiment_summary::{task_name}",
            is_primary_run=False,
        )
        for step_name, run_id in (
            ("dataset_summary", artifact_lineage.get("dataset_summary_run_id")),
            ("feature_snapshot", artifact_lineage.get("feature_snapshot_run_id")),
            ("signal_snapshot", artifact_lineage.get("signal_snapshot_run_id")),
        ):
            if not run_id:
                continue
            self.research_run_repository.update_lineage(
                str(run_id),
                research_session_id=batch_session_id,
                parent_research_run_id=experiment_run_id,
                root_research_run_id=batch_run_id,
                step_name=step_name,
                is_primary_run=False,
            )
        return {
            "task_name": task_name,
            "research_run_id": experiment_run_id,
            "research_session_id": batch_session_id,
            "dataset_digest": experiment_result["dataset"]["dataset_digest"],
            "feature_name": experiment_result["experiment"]["feature_name"],
            "lookback": experiment_result["experiment"]["lookback"],
            "top_n": experiment_result["experiment"]["top_n"],
            "top_symbols": experiment_result["feature"]["top_symbols"],
            "dataset_summary_run_id": artifact_lineage.get("dataset_summary_run_id"),
            "feature_snapshot_run_id": artifact_lineage.get("feature_snapshot_run_id"),
            "signal_snapshot_run_id": artifact_lineage.get("signal_snapshot_run_id"),
        }

    def finalize_batch_summary(
        self,
        *,
        request: dict[str, Any],
        result: dict[str, Any],
        task_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """持久化 batch 主记录并统一重绑 experiment 谱系。"""
        from a_share_quant.workflows.research_models import ResearchPersistSpec

        batch_session_id = new_id("research_session")
        batch_payload = self.persist_artifact(
            artifact_type="experiment_batch_summary",
            request=request,
            result=result,
            persist_spec=ResearchPersistSpec(
                research_session_id=batch_session_id,
                step_name="experiment_batch_summary",
                is_primary_run=True,
            ),
        )
        batch_run_id = str(batch_payload["research_run_id"])
        rebound_tasks = [
            self.rebind_batch_lineage(
                batch_run_id=batch_run_id,
                batch_session_id=batch_session_id,
                task_name=str(item["task_name"]),
                experiment_result=item["experiment_result"],
            )
            for item in task_results
        ]
        batch_payload["tasks"] = rebound_tasks
        batch_payload["aggregate"] = {
            **(batch_payload.get("aggregate") or {}),
            "generated_research_run_ids": [item["research_run_id"] for item in rebound_tasks],
            "signal_snapshot_run_ids": [item["signal_snapshot_run_id"] for item in rebound_tasks if item.get("signal_snapshot_run_id")],
        }
        return batch_payload
