"""研究工作流。"""
from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, TypeVar

from a_share_quant.core.utils import new_id
from a_share_quant.providers.dataset_provider import DatasetProvider
from a_share_quant.providers.feature_provider import FeatureProvider
from a_share_quant.repositories.research_cache_repository import ResearchCacheRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.services.research_promotion import build_signal_promotion_package

_ResultT = TypeVar("_ResultT")


@dataclass(frozen=True, slots=True)
class ResearchArtifactSummary:
    """研究产物摘要。"""

    dataset: dict[str, Any]
    feature: dict[str, Any] | None = None
    signal: dict[str, Any] | None = None
    experiment: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ResearchTaskSpec:
    """单个 research 实验任务描述。"""

    task_name: str
    feature_name: str = "momentum"
    lookback: int = 3
    top_n: int = 2
    start_date: date | None = None
    end_date: date | None = None
    ts_codes: tuple[str, ...] = ()

    @classmethod
    def from_payload(cls, index: int, payload: dict[str, Any]) -> ResearchTaskSpec:
        def _parse_date(raw: Any) -> date | None:
            return date.fromisoformat(str(raw)) if raw else None

        raw_symbols = payload.get("ts_codes") or payload.get("symbols") or []
        if isinstance(raw_symbols, str):
            symbols = tuple(item.strip() for item in raw_symbols.split(",") if item.strip())
        elif isinstance(raw_symbols, list):
            symbols = tuple(str(item).strip() for item in raw_symbols if str(item).strip())
        else:
            raise ValueError(f"research task[{index}] 的 ts_codes/symbols 必须是字符串或列表")
        return cls(
            task_name=str(payload.get("task_name") or payload.get("name") or f"task_{index + 1}"),
            feature_name=str(payload.get("feature_name") or "momentum"),
            lookback=int(payload.get("lookback", 3)),
            top_n=int(payload.get("top_n", 2)),
            start_date=_parse_date(payload.get("start_date")),
            end_date=_parse_date(payload.get("end_date")),
            ts_codes=symbols,
        )

    def to_request_kwargs(self) -> dict[str, Any]:
        return {
            "feature_name": self.feature_name,
            "lookback": self.lookback,
            "top_n": self.top_n,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ts_codes": list(self.ts_codes) or None,
        }


@dataclass(frozen=True, slots=True)
class ResearchPersistSpec:
    """研究产物持久化规格。"""

    research_session_id: str | None = None
    parent_research_run_id: str | None = None
    root_research_run_id: str | None = None
    step_name: str | None = None
    is_primary_run: bool = True


@dataclass(frozen=True, slots=True)
class ComputedFeatureSnapshot:
    """内存态特征快照。"""

    request: dict[str, Any]
    result: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ComputedSignalSnapshot:
    """内存态信号快照。"""

    request: dict[str, Any]
    result: dict[str, Any]
    feature_snapshot: ComputedFeatureSnapshot


def load_research_task_specs(path: str | Path) -> list[ResearchTaskSpec]:
    """从 JSON 文件加载 batch research 任务。"""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_tasks = payload.get("tasks")
    elif isinstance(payload, list):
        raw_tasks = payload
    else:
        raise ValueError("batch spec 根节点必须为对象或数组")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError("batch spec.tasks 必须是非空数组")
    return [ResearchTaskSpec.from_payload(index, item) for index, item in enumerate(raw_tasks) if isinstance(item, dict)]


class ResearchWorkflow:
    """封装研究数据快照、特征批量计算与实验摘要。

    Boundary Behavior:
        - 当前只覆盖 research_backtest 范围内的确定性研究流程；
        - 不承担模型训练或 live orchestration；
        - 对于历史不足的证券，特征批量输出会显式跳过，不制造伪样本；
        - experiment 会把用户主记录与内部 dataset/feature/signal 子步骤分离持久化，避免 recent-runs 被内部步骤污染；
        - dataset/feature/signal 结果会进入持久化 research cache，以 dataset digest + request 作为稳定缓存键。
    """

    def __init__(
        self,
        dataset_provider: DatasetProvider,
        feature_provider: FeatureProvider,
        research_run_repository: ResearchRunRepository,
        context,
        *,
        plugin_manager=None,
    ) -> None:
        self.dataset_provider = dataset_provider
        self.feature_provider = feature_provider
        self.research_run_repository = research_run_repository
        self.context = context
        self.plugin_manager = plugin_manager
        self.research_cache_repository = ResearchCacheRepository(context.store)
        self._cache_enabled = bool(getattr(context.config.research, "enable_cache", True))
        self._cache_namespace = str(getattr(context.config.research, "cache_namespace", "default") or "default")
        self._cache_schema_version = str(getattr(context.config.research, "cache_schema_version", "v2") or "v2")
        self._dataset_scope_cache_invalidation = bool(getattr(context.config.research, "dataset_scope_cache_invalidation", True))
        self._cache_max_entries = int(getattr(context.config.research, "max_cached_entries", 500) or 500)

    def bind_plugin_manager(self, plugin_manager) -> None:
        self.plugin_manager = plugin_manager

    @staticmethod
    def _json_default(value: Any) -> str:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _cache_request_digest(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, ensure_ascii=False, default=self._json_default, separators=(",", ":"))

    def _latest_data_revision_token(self) -> str:
        latest_import = self.context.data_import_repository.get_latest_run()
        if latest_import is not None and latest_import.import_run_id:
            return latest_import.import_run_id
        latest_dataset = self.context.dataset_version_repository.get_latest_version()
        if latest_dataset is not None and latest_dataset.dataset_version_id:
            return latest_dataset.dataset_version_id
        return "no_data_revision"

    @staticmethod
    def _provider_signature(provider: object) -> str:
        explicit_version = getattr(provider, "implementation_version", None) or getattr(provider, "__cache_version__", None) or getattr(provider, "version", None)
        type_name = f"{provider.__class__.__module__}.{provider.__class__.__qualname__}"
        if explicit_version:
            return f"{type_name}@{explicit_version}"
        source_fingerprint = None
        try:
            source = inspect.getsource(provider.__class__)
            source_fingerprint = hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
        except Exception:
            source_fingerprint = None
        return f"{type_name}@{source_fingerprint or 'unknown'}"

    def _cache_revision_token(
        self,
        *,
        dataset_version_id: str | None = None,
        dataset_digest: str | None = None,
        request: dict[str, Any] | None = None,
    ) -> str:
        if dataset_version_id:
            return dataset_version_id
        if dataset_digest:
            return dataset_digest
        if self._dataset_scope_cache_invalidation and request is not None:
            return self._cache_request_digest({"request_scope": request})
        return self._latest_data_revision_token()

    def _build_cache_seed(
        self,
        *,
        artifact_type: str,
        request: dict[str, Any],
        dataset_version_id: str | None = None,
        dataset_digest: str | None = None,
        include_dataset_digest_in_key: bool = True,
    ) -> dict[str, Any]:
        return {
            "artifact_type": artifact_type,
            "request": request,
            "revision": self._cache_revision_token(
                dataset_version_id=dataset_version_id,
                dataset_digest=dataset_digest if include_dataset_digest_in_key else None,
                request=request,
            ),
            "dataset_digest": dataset_digest if include_dataset_digest_in_key else None,
            "cache_schema_version": self._cache_schema_version,
            "dataset_provider_signature": self._provider_signature(self.dataset_provider),
            "feature_provider_signature": self._provider_signature(self.feature_provider),
            "strategy_version": getattr(getattr(self.context.config, "strategy", object()), "version", None),
        }

    def _get_cached_payload(
        self,
        *,
        artifact_type: str,
        request: dict[str, Any],
        dataset_version_id: str | None = None,
        dataset_digest: str | None = None,
        include_dataset_digest_in_key: bool = True,
    ) -> dict[str, Any] | None:
        if not self._cache_enabled:
            return None
        cache_seed = self._build_cache_seed(
            artifact_type=artifact_type,
            request=request,
            dataset_version_id=dataset_version_id,
            dataset_digest=dataset_digest,
            include_dataset_digest_in_key=include_dataset_digest_in_key,
        )
        cache_key = self._cache_request_digest(cache_seed)
        row = self.research_cache_repository.get(cache_namespace=self._cache_namespace, cache_key=cache_key)
        if row is None:
            return None
        payload = dict(row["payload"])
        payload.setdefault("cache_meta", {})
        payload["cache_meta"] = {
            **dict(payload.get("cache_meta") or {}),
            "artifact_type": artifact_type,
            "cache_hit": True,
            "cache_key": cache_key,
            "cache_namespace": self._cache_namespace,
            "request_digest": row.get("request_digest"),
        }
        return payload

    def _store_cached_payload(
        self,
        *,
        artifact_type: str,
        request: dict[str, Any],
        dataset_version_id: str | None,
        dataset_digest: str | None,
        payload: dict[str, Any],
        include_dataset_digest_in_key: bool = True,
    ) -> dict[str, Any]:
        """写入 research 持久化缓存并回写缓存元信息。

        Args:
            artifact_type: 产物类型。
            request: 参与缓存键构造的稳定请求。
            dataset_version_id: 结果所属的数据集版本。
            dataset_digest: 结果关联的数据摘要，会写入缓存行元数据。
            payload: 待缓存的结果载荷。
            include_dataset_digest_in_key: 是否把 ``dataset_digest`` 纳入缓存键。
                ``dataset_summary`` 在读取缓存前尚不知道真实 digest，因此该类型必须关闭该开关，
                保持读/写两侧的键构造一致，避免永不命中。

        Returns:
            回写 ``cache_meta`` 后的标准化结果。
        """
        cache_seed = self._build_cache_seed(
            artifact_type=artifact_type,
            request=request,
            dataset_version_id=dataset_version_id,
            dataset_digest=dataset_digest,
            include_dataset_digest_in_key=include_dataset_digest_in_key,
        )
        cache_key = self._cache_request_digest(cache_seed)
        request_digest = self._cache_request_digest(request)
        normalized_payload = dict(payload)
        normalized_payload.setdefault("cache_meta", {})
        normalized_payload["cache_meta"] = {
            **dict(normalized_payload.get("cache_meta") or {}),
            "artifact_type": artifact_type,
            "cache_hit": False,
            "cache_key": cache_key,
            "cache_namespace": self._cache_namespace,
            "request_digest": request_digest,
        }
        if self._cache_enabled:
            self.research_cache_repository.put(
                cache_namespace=self._cache_namespace,
                cache_key=cache_key,
                artifact_type=artifact_type,
                request_digest=request_digest,
                dataset_version_id=dataset_version_id,
                dataset_digest=dataset_digest,
                payload=normalized_payload,
            )
            self.research_cache_repository.prune(cache_namespace=self._cache_namespace, max_entries=self._cache_max_entries)
        return normalized_payload

    def _persist_artifact(
        self,
        *,
        artifact_type: str,
        request: dict[str, Any],
        result: dict[str, Any],
        persist_spec: ResearchPersistSpec | None = None,
    ) -> dict[str, Any]:
        """持久化 research 产物，并把持久化元信息回写到返回载荷。"""
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
        return payload

    def _run_with_plugin_hooks(
        self,
        artifact_type: str,
        payload: dict[str, Any],
        executor: Callable[[], _ResultT],
    ) -> _ResultT:
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
                self.plugin_manager.emit_after_workflow_run(
                    self.context,
                    "workflow.research",
                    hook_payload,
                    result=result,
                    error=error,
                )

    def list_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """列出最近研究运行摘要。"""
        return self.research_run_repository.list_recent(limit=limit)

    def _build_dataset_request(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        return self.dataset_provider.build_request(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=None,
            active_only=False,
            access_mode="preload",
        ).to_dict()

    def _compute_dataset_snapshot_payload(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """构造数据快照请求与结果，不执行持久化。"""
        request = self._build_dataset_request(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        cached = self._get_cached_payload(
            artifact_type="dataset_summary",
            request=request,
            include_dataset_digest_in_key=False,
        )
        if cached is not None:
            return request, cached
        snapshot = self.dataset_provider.load_snapshot(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        total_bar_count = sum(len(bars) for bars in snapshot.bars_by_symbol.values())
        payload = {
            "request": request,
            "symbol_count": len(snapshot.securities),
            "calendar_count": len(snapshot.trade_calendar),
            "bar_symbol_count": len(snapshot.bars_by_symbol),
            "total_bar_count": total_bar_count,
            "dataset_version_id": snapshot.data_lineage.dataset_version_id,
            "dataset_digest": snapshot.data_lineage.dataset_digest or "",
            "import_run_ids": list(snapshot.data_lineage.import_run_ids),
            "provider_name": "provider.dataset",
            "data_lineage": asdict(snapshot.data_lineage),
        }
        payload = self._store_cached_payload(
            artifact_type="dataset_summary",
            request=request,
            dataset_version_id=None,
            dataset_digest=payload.get("dataset_digest"),
            payload=payload,
            include_dataset_digest_in_key=False,
        )
        return request, payload

    def _compute_feature_snapshot_payload(
        self,
        *,
        feature_name: str,
        lookback: int,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> ComputedFeatureSnapshot:
        """构造特征快照请求与结果，不执行持久化。"""
        dataset_request, dataset_summary = self._compute_dataset_snapshot_payload(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
        )
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
            "dataset_request": dataset_request,
        }
        cached = self._get_cached_payload(
            artifact_type="feature_snapshot",
            request=request,
            dataset_version_id=dataset_summary.get("dataset_version_id"),
            dataset_digest=dataset_summary.get("dataset_digest"),
        )
        if cached is not None:
            return ComputedFeatureSnapshot(request=request, result=cached)
        snapshot = self.dataset_provider.load_snapshot(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        values = self.feature_provider.compute_feature_batch(
            feature_name,
            snapshot.bars_by_symbol,
            lookback=lookback,
        )
        ordered = sorted(values.items(), key=lambda item: item[1], reverse=True)
        result = {
            "feature_spec": self.feature_provider.momentum_spec(lookback).to_dict()
            if feature_name == "momentum"
            else {
                "name": feature_name,
                "params": {"lookback": lookback},
            },
            "dataset_summary": dataset_summary,
            "value_count": len(values),
            "values": {symbol: value for symbol, value in ordered},
            "top_symbols": [symbol for symbol, _ in ordered[: min(10, len(ordered))]],
        }
        result = self._store_cached_payload(
            artifact_type="feature_snapshot",
            request=request,
            dataset_version_id=dataset_summary.get("dataset_version_id"),
            dataset_digest=dataset_summary.get("dataset_digest"),
            payload=result,
        )
        return ComputedFeatureSnapshot(request=request, result=result)

    def _compute_signal_snapshot_payload(
        self,
        *,
        feature_name: str,
        lookback: int,
        top_n: int,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        feature_snapshot: ComputedFeatureSnapshot | None = None,
    ) -> ComputedSignalSnapshot:
        """构造信号快照请求与结果，不执行持久化。"""
        resolved_feature_snapshot = feature_snapshot or self._compute_feature_snapshot_payload(
            feature_name=feature_name,
            lookback=lookback,
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
        )
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "top_n": top_n,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
            "feature_request": resolved_feature_snapshot.request,
        }
        dataset_summary = resolved_feature_snapshot.result["dataset_summary"]
        cached = self._get_cached_payload(
            artifact_type="signal_snapshot",
            request=request,
            dataset_version_id=dataset_summary.get("dataset_version_id"),
            dataset_digest=dataset_summary.get("dataset_digest"),
        )
        if cached is not None:
            return ComputedSignalSnapshot(
                request=request,
                result=cached,
                feature_snapshot=resolved_feature_snapshot,
            )
        ordered = list(resolved_feature_snapshot.result["values"].items())
        selected = ordered[: max(0, top_n)]
        weight = (1.0 / len(selected)) if selected else 0.0
        result = {
            "signal_type": "top_n_equal_weight",
            "top_n": top_n,
            "selected_symbols": [
                {"ts_code": ts_code, "score": score, "target_weight": weight}
                for ts_code, score in selected
            ],
            "dataset_summary": dataset_summary,
            "feature_spec": resolved_feature_snapshot.result["feature_spec"],
            "promotion_package": build_signal_promotion_package(
                dataset_summary=dataset_summary,
                feature_spec=resolved_feature_snapshot.result["feature_spec"],
                top_n=top_n,
            ),
        }
        result = self._store_cached_payload(
            artifact_type="signal_snapshot",
            request=request,
            dataset_version_id=dataset_summary.get("dataset_version_id"),
            dataset_digest=dataset_summary.get("dataset_digest"),
            payload=result,
        )
        return ComputedSignalSnapshot(
            request=request,
            result=result,
            feature_snapshot=resolved_feature_snapshot,
        )

    def load_snapshot_summary(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        """返回 research 数据快照摘要。"""
        request = self._build_dataset_request(start_date=start_date, end_date=end_date, ts_codes=ts_codes)

        def _execute() -> dict[str, Any]:
            _, result = self._compute_dataset_snapshot_payload(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
            return self._persist_artifact(artifact_type="dataset_summary", request=request, result=result)

        return self._run_with_plugin_hooks("dataset_summary", request, _execute)

    def run_feature_snapshot(
        self,
        *,
        feature_name: str,
        lookback: int,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        """计算特征横截面快照。"""
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
        }

        def _execute() -> dict[str, Any]:
            computed = self._compute_feature_snapshot_payload(
                feature_name=feature_name,
                lookback=lookback,
                start_date=start_date,
                end_date=end_date,
                ts_codes=ts_codes,
            )
            return self._persist_artifact(artifact_type="feature_snapshot", request=request, result=computed.result)

        return self._run_with_plugin_hooks("feature_snapshot", request, _execute)

    def run_signal_snapshot(
        self,
        *,
        feature_name: str,
        lookback: int,
        top_n: int,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        """基于特征横截面生成简单信号快照。"""
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "top_n": top_n,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
        }

        def _execute() -> dict[str, Any]:
            computed = self._compute_signal_snapshot_payload(
                feature_name=feature_name,
                lookback=lookback,
                top_n=top_n,
                start_date=start_date,
                end_date=end_date,
                ts_codes=ts_codes,
            )
            return self._persist_artifact(artifact_type="signal_snapshot", request=request, result=computed.result)

        return self._run_with_plugin_hooks("signal_snapshot", request, _execute)

    def summarize_experiment(
        self,
        *,
        feature_name: str,
        lookback: int,
        top_n: int,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        """输出研究实验摘要，不执行真实训练。"""
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "top_n": top_n,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
        }

        def _execute() -> dict[str, Any]:
            research_session_id = new_id("research_session")
            dataset_request, dataset_result = self._compute_dataset_snapshot_payload(
                start_date=start_date,
                end_date=end_date,
                ts_codes=ts_codes,
            )
            feature_snapshot = self._compute_feature_snapshot_payload(
                feature_name=feature_name,
                lookback=lookback,
                start_date=start_date,
                end_date=end_date,
                ts_codes=ts_codes,
            )
            signal_snapshot = self._compute_signal_snapshot_payload(
                feature_name=feature_name,
                lookback=lookback,
                top_n=top_n,
                start_date=start_date,
                end_date=end_date,
                ts_codes=ts_codes,
                feature_snapshot=feature_snapshot,
            )
            summary = ResearchArtifactSummary(
                dataset=dataset_result,
                feature={
                    "name": feature_snapshot.result["feature_spec"]["name"],
                    "value_count": feature_snapshot.result["value_count"],
                    "top_symbols": feature_snapshot.result["top_symbols"],
                    "cache_meta": feature_snapshot.result.get("cache_meta") or {},
                },
                signal={
                    "signal_type": signal_snapshot.result["signal_type"],
                    "selected_count": len(signal_snapshot.result["selected_symbols"]),
                    "cache_meta": signal_snapshot.result.get("cache_meta") or {},
                },
                experiment={
                    "workflow": "workflow.research",
                    "feature_name": feature_name,
                    "lookback": lookback,
                    "top_n": top_n,
                    "research_session_id": research_session_id,
                    "cache_meta": {
                        "dataset": dataset_result.get("cache_meta") or {},
                        "feature": feature_snapshot.result.get("cache_meta") or {},
                        "signal": signal_snapshot.result.get("cache_meta") or {},
                    },
                },
            )
            experiment_payload = self._persist_artifact(
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
            dataset_payload = self._persist_artifact(
                artifact_type="dataset_summary",
                request=dataset_request,
                result=dataset_result,
                persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "dataset_summary"}),
            )
            feature_payload = self._persist_artifact(
                artifact_type="feature_snapshot",
                request=feature_snapshot.request,
                result=feature_snapshot.result,
                persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "feature_snapshot"}),
            )
            signal_payload = self._persist_artifact(
                artifact_type="signal_snapshot",
                request=signal_snapshot.request,
                result=signal_snapshot.result,
                persist_spec=ResearchPersistSpec(**{**asdict(child_spec), "step_name": "signal_snapshot"}),
            )
            experiment_section = dict(experiment_payload["experiment"] or {})
            experiment_section["artifact_lineage"] = {
                "dataset_summary_run_id": dataset_payload["research_run_id"],
                "feature_snapshot_run_id": feature_payload["research_run_id"],
                "signal_snapshot_run_id": signal_payload["research_run_id"],
            }
            experiment_payload["experiment"] = experiment_section
            return experiment_payload

        return self._run_with_plugin_hooks("experiment_summary", request, _execute)

    def _rebind_batch_lineage(
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

    def _finalize_batch_summary(
        self,
        *,
        request: dict[str, Any],
        result: dict[str, Any],
        task_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """持久化 batch 主记录并把子 experiment 统一挂到 batch 谱系下。"""
        batch_session_id = new_id("research_session")
        batch_payload = self._persist_artifact(
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
            self._rebind_batch_lineage(
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

    def summarize_experiment_batch(self, task_specs: list[ResearchTaskSpec]) -> dict[str, Any]:
        """批量执行 experiment task spec，并聚合输出。"""
        request = {
            "task_count": len(task_specs),
            "tasks": [asdict(item) for item in task_specs],
        }

        def _execute() -> dict[str, Any]:
            task_results: list[dict[str, Any]] = []
            selected_symbol_union: set[str] = set()
            batch_dataset_summary: dict[str, Any] = {}
            cache_hits = {"dataset": 0, "feature": 0, "signal": 0}
            for spec in task_specs:
                result = self.summarize_experiment(**spec.to_request_kwargs())
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
            return self._finalize_batch_summary(request=request, result=result, task_results=task_results)

        return self._run_with_plugin_hooks("experiment_batch_summary", request, _execute)
