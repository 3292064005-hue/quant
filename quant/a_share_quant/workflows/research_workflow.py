"""研究工作流。"""
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from a_share_quant.core.utils import new_id
from a_share_quant.providers.dataset_provider import DatasetProvider
from a_share_quant.providers.feature_provider import FeatureProvider
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.workflows.research_batch_service import ResearchBatchWorkflowService
from a_share_quant.workflows.research_models import ResearchArtifactSummary, ResearchPersistSpec, ResearchTaskSpec, load_research_task_specs
from a_share_quant.workflows.research_recording import ResearchRecordingService
from a_share_quant.workflows.research_snapshot_service import ResearchSnapshotService

_ResultT = TypeVar("_ResultT")


class ResearchWorkflow:
    """封装 research snapshot/experiment/batch 的正式编排边界。"""

    def __init__(self, dataset_provider: DatasetProvider, feature_provider: FeatureProvider, research_run_repository: ResearchRunRepository, context, *, plugin_manager=None) -> None:
        self.dataset_provider = dataset_provider
        self.feature_provider = feature_provider
        self.research_run_repository = research_run_repository
        self.context = context
        self.plugin_manager = plugin_manager
        self.snapshot_service = ResearchSnapshotService(dataset_provider=dataset_provider, feature_provider=feature_provider, context=context)
        self.recording_service = ResearchRecordingService(research_run_repository, self._persist_artifact)
        self.batch_service = ResearchBatchWorkflowService()

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    def _persist_artifact(self, *, artifact_type: str, request: dict[str, object], result: dict[str, object], persist_spec: ResearchPersistSpec | None = None) -> dict[str, object]:
        spec = persist_spec or ResearchPersistSpec(step_name=artifact_type)
        dataset_summary = result.get("dataset_summary") or result.get("dataset") or {}
        research_run_id = self.research_run_repository.create_run(
            workflow_name="workflow.research",
            artifact_type=artifact_type,
            dataset_version_id=dataset_summary.get("dataset_version_id"),
            dataset_digest=dataset_summary.get("dataset_digest"),
            request=request,
            result=result,
            research_session_id=spec.research_session_id,
            parent_research_run_id=spec.parent_research_run_id,
            root_research_run_id=spec.root_research_run_id,
            step_name=spec.step_name or artifact_type,
            is_primary_run=spec.is_primary_run,
        )
        payload = dict(result)
        payload["research_run_id"] = research_run_id
        payload["research_session_id"] = spec.research_session_id
        payload["parent_research_run_id"] = spec.parent_research_run_id
        payload["root_research_run_id"] = spec.root_research_run_id or research_run_id
        payload["step_name"] = spec.step_name or artifact_type
        payload["is_primary_run"] = spec.is_primary_run
        payload["recorded"] = True
        payload["recording_mode"] = "research_run"
        return payload

    def _should_record_query_artifact(self, record: bool | None) -> bool:
        if record is not None:
            return bool(record)
        return bool(getattr(self.context.config.research, "record_query_runs", False))

    @staticmethod
    def _materialize_query_payload(*, artifact_type: str, result: dict[str, object]) -> dict[str, object]:
        payload = dict(result)
        payload.setdefault("research_run_id", None)
        payload.setdefault("research_session_id", None)
        payload.setdefault("parent_research_run_id", None)
        payload.setdefault("root_research_run_id", None)
        payload.setdefault("step_name", artifact_type)
        payload.setdefault("is_primary_run", False)
        payload["recorded"] = False
        payload["recording_mode"] = "cache_only"
        return payload

    def _run_with_plugin_hooks(self, artifact_type: str, payload: dict[str, object], executor: Callable[[], _ResultT]) -> _ResultT:
        hook_payload = {"artifact_type": artifact_type, **payload}
        if self.plugin_manager is not None:
            self.plugin_manager.emit_before_workflow_run(self.context, "workflow.research", hook_payload)
        result: _ResultT | None = None
        error: Exception | None = None
        try:
            result = executor()
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            if self.plugin_manager is not None:
                self.plugin_manager.emit_after_workflow_run(self.context, "workflow.research", hook_payload, result=result, error=error)

    def list_recent_runs(self, limit: int = 20) -> list[dict[str, object]]:
        return self.research_run_repository.list_recent(limit=limit)

    def load_snapshot_summary(self, *, start_date=None, end_date=None, ts_codes=None, record: bool | None = None) -> dict[str, object]:
        request = self.snapshot_service.build_dataset_request(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        def _execute() -> dict[str, object]:
            _, result = self.snapshot_service.compute_dataset_snapshot_payload(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            return self._persist_artifact(artifact_type="dataset_summary", request=request, result=result) if self._should_record_query_artifact(record) else self._materialize_query_payload(artifact_type="dataset_summary", result=result)
        return self._run_with_plugin_hooks("dataset_summary", request, _execute)

    def run_feature_snapshot(self, *, feature_name: str, lookback: int, start_date=None, end_date=None, ts_codes=None, record: bool | None = None) -> dict[str, object]:
        request = {"feature_name": feature_name, "lookback": lookback, "start_date": start_date, "end_date": end_date, "ts_codes": ts_codes or []}
        def _execute() -> dict[str, object]:
            computed = self.snapshot_service.compute_feature_snapshot_payload(feature_name=feature_name, lookback=lookback, start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            return self._persist_artifact(artifact_type="feature_snapshot", request=request, result=computed.result) if self._should_record_query_artifact(record) else self._materialize_query_payload(artifact_type="feature_snapshot", result=computed.result)
        return self._run_with_plugin_hooks("feature_snapshot", request, _execute)

    def run_signal_snapshot(self, *, feature_name: str, lookback: int, top_n: int, start_date=None, end_date=None, ts_codes=None, record: bool | None = None) -> dict[str, object]:
        request = {"feature_name": feature_name, "lookback": lookback, "top_n": top_n, "start_date": start_date, "end_date": end_date, "ts_codes": ts_codes or []}
        def _execute() -> dict[str, object]:
            computed = self.snapshot_service.compute_signal_snapshot_payload(feature_name=feature_name, lookback=lookback, top_n=top_n, start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            return self._persist_artifact(artifact_type="signal_snapshot", request=request, result=computed.result) if self._should_record_query_artifact(record) else self._materialize_query_payload(artifact_type="signal_snapshot", result=computed.result)
        return self._run_with_plugin_hooks("signal_snapshot", request, _execute)

    def summarize_experiment(self, *, feature_name: str, lookback: int, top_n: int, start_date=None, end_date=None, ts_codes=None) -> dict[str, object]:
        request = {"feature_name": feature_name, "lookback": lookback, "top_n": top_n, "start_date": start_date, "end_date": end_date, "ts_codes": ts_codes or []}
        def _execute() -> dict[str, object]:
            research_session_id = new_id("research_session")
            dataset_request, dataset_result = self.snapshot_service.compute_dataset_snapshot_payload(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            feature_snapshot = self.snapshot_service.compute_feature_snapshot_payload(feature_name=feature_name, lookback=lookback, start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            signal_snapshot = self.snapshot_service.compute_signal_snapshot_payload(feature_name=feature_name, lookback=lookback, top_n=top_n, start_date=start_date, end_date=end_date, ts_codes=ts_codes, feature_snapshot=feature_snapshot)
            summary = ResearchArtifactSummary(
                dataset=dataset_result,
                feature={"name": feature_snapshot.result["feature_spec"]["name"], "value_count": feature_snapshot.result["value_count"], "top_symbols": feature_snapshot.result["top_symbols"], "cache_meta": feature_snapshot.result.get("cache_meta") or {}},
                signal={"signal_type": signal_snapshot.result["signal_type"], "selected_count": len(signal_snapshot.result["selected_symbols"]), "cache_meta": signal_snapshot.result.get("cache_meta") or {}},
                experiment={"workflow": "workflow.research", "feature_name": feature_name, "lookback": lookback, "top_n": top_n, "research_session_id": research_session_id, "cache_meta": {"dataset": dataset_result.get("cache_meta") or {}, "feature": feature_snapshot.result.get("cache_meta") or {}, "signal": signal_snapshot.result.get("cache_meta") or {}}},
            )
            return self.recording_service.persist_experiment_bundle(request=request, summary=summary, dataset_request=dataset_request, dataset_result=dataset_result, feature_snapshot=feature_snapshot, signal_snapshot=signal_snapshot)
        return self._run_with_plugin_hooks("experiment_summary", request, _execute)

    def _finalize_batch_summary(self, *, request: dict[str, object], result: dict[str, object], task_results: list[dict[str, object]]) -> dict[str, object]:
        return self.recording_service.finalize_batch_summary(request=request, result=result, task_results=task_results)

    def summarize_experiment_batch(self, task_specs: list[ResearchTaskSpec]) -> dict[str, object]:
        request = self.batch_service.build_batch_request(task_specs)
        def _execute() -> dict[str, object]:
            return self.batch_service.run_batch(task_specs=task_specs, summarize_experiment=self.summarize_experiment, finalize_batch_summary=self._finalize_batch_summary, request=request)
        return self._run_with_plugin_hooks("experiment_batch_summary", request, _execute)


__all__ = ["ResearchArtifactSummary", "ResearchPersistSpec", "ResearchTaskSpec", "ResearchWorkflow", "load_research_task_specs"]
