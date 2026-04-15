"""research snapshot 计算与缓存服务。"""
from __future__ import annotations

import hashlib
import inspect
import json
from datetime import date
from typing import Any

from a_share_quant.providers.dataset_provider import DatasetProvider
from a_share_quant.providers.feature_provider import FeatureProvider
from a_share_quant.repositories.research_cache_repository import ResearchCacheRepository
from a_share_quant.services.research_promotion import build_signal_promotion_package
from a_share_quant.workflows.research_models import ComputedFeatureSnapshot, ComputedSignalSnapshot


class ResearchSnapshotService:
    def __init__(self, *, dataset_provider: DatasetProvider, feature_provider: FeatureProvider, context) -> None:
        self.dataset_provider = dataset_provider
        self.feature_provider = feature_provider
        self.context = context
        self.research_cache_repository = ResearchCacheRepository(context.store)
        self._cache_enabled = bool(getattr(context.config.research, "enable_cache", True))
        self._cache_namespace = str(getattr(context.config.research, "cache_namespace", "default") or "default")
        self._cache_schema_version = str(getattr(context.config.research, "cache_schema_version", "v2") or "v2")
        self._dataset_scope_cache_invalidation = bool(getattr(context.config.research, "dataset_scope_cache_invalidation", True))
        self._cache_max_entries = int(getattr(context.config.research, "max_cached_entries", 500) or 500)

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

    def _cache_revision_token(self, *, dataset_version_id: str | None = None, dataset_digest: str | None = None, request: dict[str, Any] | None = None) -> str:
        if dataset_version_id:
            return dataset_version_id
        if dataset_digest:
            return dataset_digest
        if self._dataset_scope_cache_invalidation and request is not None:
            return self._cache_request_digest({"request_scope": request})
        return self._latest_data_revision_token()

    def _build_cache_seed(self, *, artifact_type: str, request: dict[str, Any], dataset_version_id: str | None = None, dataset_digest: str | None = None, include_dataset_digest_in_key: bool = True) -> dict[str, Any]:
        return {
            "artifact_type": artifact_type,
            "request": request,
            "revision": self._cache_revision_token(dataset_version_id=dataset_version_id, dataset_digest=dataset_digest if include_dataset_digest_in_key else None, request=request),
            "dataset_digest": dataset_digest if include_dataset_digest_in_key else None,
            "cache_schema_version": self._cache_schema_version,
            "dataset_provider_signature": self._provider_signature(self.dataset_provider),
            "feature_provider_signature": self._provider_signature(self.feature_provider),
            "strategy_version": getattr(getattr(self.context.config, "strategy", object()), "version", None),
        }

    def _get_cached_payload(self, *, artifact_type: str, request: dict[str, Any], dataset_version_id: str | None = None, dataset_digest: str | None = None, include_dataset_digest_in_key: bool = True) -> dict[str, Any] | None:
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

    def _store_cached_payload(self, *, artifact_type: str, request: dict[str, Any], dataset_version_id: str | None, dataset_digest: str | None, payload: dict[str, Any], include_dataset_digest_in_key: bool = True) -> dict[str, Any]:
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

    def build_dataset_request(self, *, start_date: date | None = None, end_date: date | None = None, ts_codes: list[str] | None = None) -> dict[str, Any]:
        return self.dataset_provider.build_request(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=None,
            active_only=False,
            access_mode="preload",
        ).to_dict()

    def compute_dataset_snapshot_payload(self, *, start_date: date | None = None, end_date: date | None = None, ts_codes: list[str] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        request = self.build_dataset_request(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        cached = self._get_cached_payload(artifact_type="dataset_summary", request=request, include_dataset_digest_in_key=False)
        if cached is not None:
            return request, cached
        result = self.dataset_provider.summarize_snapshot(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=None,
            active_only=False,
        ).to_dict()
        result = self._store_cached_payload(
            artifact_type="dataset_summary",
            request=request,
            dataset_version_id=None,
            dataset_digest=None,
            payload=result,
            include_dataset_digest_in_key=False,
        )
        return request, result

    def compute_feature_snapshot_payload(self, *, feature_name: str, lookback: int, start_date: date | None = None, end_date: date | None = None, ts_codes: list[str] | None = None) -> ComputedFeatureSnapshot:
        dataset_request, dataset_summary = self.compute_dataset_snapshot_payload(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
        request = {
            "feature_name": feature_name,
            "lookback": lookback,
            "start_date": start_date,
            "end_date": end_date,
            "ts_codes": ts_codes or [],
            "dataset_request": dataset_request,
        }
        cached = self._get_cached_payload(artifact_type="feature_snapshot", request=request, dataset_version_id=dataset_summary.get("dataset_version_id"), dataset_digest=dataset_summary.get("dataset_digest"))
        if cached is not None:
            return ComputedFeatureSnapshot(request=request, result=cached)
        snapshot = self.dataset_provider.load_snapshot(start_date=start_date, end_date=end_date, ts_codes=ts_codes, as_of_date=None, active_only=False)
        values = self.feature_provider.compute_feature_batch(feature_name, snapshot.bars_by_symbol, lookback=lookback)
        sorted_values = sorted(values.items(), key=lambda item: item[1], reverse=True)
        result = {
            "feature_spec": {"name": feature_name, "lookback": lookback},
            "value_count": len(sorted_values),
            "top_symbols": [ts_code for ts_code, _score in sorted_values[:10]],
            "values": dict(sorted_values),
            "dataset_summary": dataset_summary,
        }
        result = self._store_cached_payload(artifact_type="feature_snapshot", request=request, dataset_version_id=dataset_summary.get("dataset_version_id"), dataset_digest=dataset_summary.get("dataset_digest"), payload=result)
        return ComputedFeatureSnapshot(request=request, result=result)

    def compute_signal_snapshot_payload(self, *, feature_name: str, lookback: int, top_n: int, start_date: date | None = None, end_date: date | None = None, ts_codes: list[str] | None = None, feature_snapshot: ComputedFeatureSnapshot | None = None) -> ComputedSignalSnapshot:
        resolved_feature_snapshot = feature_snapshot or self.compute_feature_snapshot_payload(feature_name=feature_name, lookback=lookback, start_date=start_date, end_date=end_date, ts_codes=ts_codes)
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
        cached = self._get_cached_payload(artifact_type="signal_snapshot", request=request, dataset_version_id=dataset_summary.get("dataset_version_id"), dataset_digest=dataset_summary.get("dataset_digest"))
        if cached is not None:
            return ComputedSignalSnapshot(request=request, result=cached, feature_snapshot=resolved_feature_snapshot)
        ordered = list(resolved_feature_snapshot.result["values"].items())
        selected = ordered[: max(0, top_n)]
        weight = (1.0 / len(selected)) if selected else 0.0
        result = {
            "signal_type": "top_n_equal_weight",
            "top_n": top_n,
            "selected_symbols": [{"ts_code": ts_code, "score": score, "target_weight": weight} for ts_code, score in selected],
            "dataset_summary": dataset_summary,
            "feature_spec": resolved_feature_snapshot.result["feature_spec"],
            "promotion_package": build_signal_promotion_package(dataset_summary=dataset_summary, feature_spec=resolved_feature_snapshot.result["feature_spec"], top_n=top_n),
        }
        result = self._store_cached_payload(artifact_type="signal_snapshot", request=request, dataset_version_id=dataset_summary.get("dataset_version_id"), dataset_digest=dataset_summary.get("dataset_digest"), payload=result)
        return ComputedSignalSnapshot(request=request, result=result, feature_snapshot=resolved_feature_snapshot)
