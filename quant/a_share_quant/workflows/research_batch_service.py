"""research batch 聚合服务。"""
from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from a_share_quant.workflows.research_models import ResearchTaskSpec


class ResearchBatchWorkflowService:
    """抽离 batch task 聚合与汇总逻辑，收紧 ResearchWorkflow 边界。"""

    def build_batch_request(self, task_specs: list["ResearchTaskSpec"]) -> dict[str, Any]:
        return {
            "task_count": len(task_specs),
            "tasks": [asdict(item) for item in task_specs],
        }

    def run_batch(
        self,
        *,
        task_specs: list["ResearchTaskSpec"],
        summarize_experiment: Callable[..., dict[str, Any]],
        finalize_batch_summary: Callable[..., dict[str, Any]],
        request: dict[str, Any],
    ) -> dict[str, Any]:
        """执行 batch task 并生成聚合结果。"""
        task_results: list[dict[str, Any]] = []
        selected_symbol_union: set[str] = set()
        batch_dataset_summary: dict[str, Any] = {}
        cache_hits = {"dataset": 0, "feature": 0, "signal": 0}
        for spec in task_specs:
            result = summarize_experiment(**spec.to_request_kwargs())
            if not batch_dataset_summary:
                batch_dataset_summary = dict(result["dataset"])
            task_results.append({"task_name": spec.task_name, "experiment_result": result})
            selected_symbol_union.update(result["feature"]["top_symbols"])
            experiment_cache = ((result.get("experiment") or {}).get("cache_meta") or {})
            for key in cache_hits:
                if bool((experiment_cache.get(key) or {}).get("cache_hit")):
                    cache_hits[key] += 1
        aggregate = {
            "task_count": len(task_results),
            "generated_research_run_ids": [item["experiment_result"]["research_run_id"] for item in task_results],
            "feature_names": sorted({item["experiment_result"]["experiment"]["feature_name"] for item in task_results}),
            "selected_symbol_union": sorted(selected_symbol_union),
            "cache_hit_counts": cache_hits,
        }
        result = {"tasks": [], "aggregate": aggregate, "dataset_summary": batch_dataset_summary}
        return finalize_batch_summary(request=request, result=result, task_results=task_results)
