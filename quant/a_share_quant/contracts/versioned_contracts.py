"""research/backtest/operator 共享的版本化合同。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContractBaseModel(BaseModel):
    """统一开启 forbid/assignment 的合同基类。"""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SignalSchemaV1(ContractBaseModel):
    kind: str
    fields: list[str]
    top_n: int


class StrategyBlueprintV1(ContractBaseModel):
    signal: str
    factor: str
    portfolio_construction: str


class SignalPromotionPackageV1(ContractBaseModel):
    artifact_contract_version: int = 1
    promotion_stage: str
    artifact_type: str
    compatible_runtime_lanes: list[str]
    compatible_signal_component: str
    compatible_execution_contract: str
    target_intent_contract: str
    compatible_risk_gate: list[str]
    dataset_version_id: str | None = None
    dataset_digest: str | None = None
    signal_schema: SignalSchemaV1
    feature_spec: dict[str, Any] = Field(default_factory=dict)
    strategy_blueprint: StrategyBlueprintV1

    @model_validator(mode="after")
    def _validate_contract(self) -> "SignalPromotionPackageV1":
        if self.artifact_contract_version != 1:
            raise ValueError("仅支持 signal promotion contract v1")
        if self.artifact_type != "signal_snapshot":
            raise ValueError("signal promotion contract artifact_type 必须为 signal_snapshot")
        return self


class SignalSelectionV1(ContractBaseModel):
    ts_code: str
    score: float | int | None = None
    target_weight: float | int | None = None


class DatasetSummaryV1(ContractBaseModel):
    dataset_version_id: str | None = None
    dataset_digest: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    request: dict[str, Any] = Field(default_factory=dict)
    symbol_count: int | None = None
    calendar_count: int | None = None
    bar_symbol_count: int | None = None
    total_bar_count: int | None = None
    import_run_ids: list[str] = Field(default_factory=list)
    provider_name: str | None = None
    cache_meta: dict[str, Any] = Field(default_factory=dict)


class SignalSnapshotPayloadV1(ContractBaseModel):
    artifact_schema_version: int = 1
    research_run_id: str
    artifact_type: str = "signal_snapshot"
    signal_type: str | None = None
    dataset_version_id: str | None = None
    dataset_digest: str | None = None
    root_research_run_id: str | None = None
    research_session_id: str | None = None
    dataset_summary: DatasetSummaryV1 = Field(default_factory=DatasetSummaryV1)
    selected_symbols: list[SignalSelectionV1] = Field(default_factory=list)
    top_n: int | None = None
    feature_spec: dict[str, Any] = Field(default_factory=dict)
    cache_meta: dict[str, Any] = Field(default_factory=dict)
    promotion_package: SignalPromotionPackageV1

    @model_validator(mode="after")
    def _validate_contract(self) -> "SignalSnapshotPayloadV1":
        if self.artifact_schema_version != 1:
            raise ValueError("仅支持 signal snapshot contract v1")
        if self.artifact_type != "signal_snapshot":
            raise ValueError("signal snapshot contract artifact_type 必须为 signal_snapshot")
        if not self.selected_symbols:
            raise ValueError("signal snapshot contract selected_symbols 不能为空")
        return self


class ReportArtifactRecordV1(ContractBaseModel):
    role: str
    path: str
    kind: str = "report"
    format: str = "json"
    primary: bool = False


class RunEventSummaryV1(ContractBaseModel):
    event_count: int = 0
    by_type: dict[str, int] = Field(default_factory=dict)
    lifecycle_summary: dict[str, Any] = Field(default_factory=dict)


class ComponentManifestV1(ContractBaseModel):
    strategy_id: str | None = None
    strategy_class: str | None = None
    signal_component: str | None = None
    feature_component: str | None = None
    portfolio_component: str | None = None
    execution_component: str | None = None
    extras: dict[str, Any] = Field(default_factory=dict)


class RunManifestContractV6(ContractBaseModel):
    schema_version: int = 6
    entrypoint: str | None = None
    strategy_version: str | None = None
    runtime_mode: str | None = None
    benchmark_initial_value: float | None = None
    report_paths: list[str] = Field(default_factory=list)
    report_artifacts: list[ReportArtifactRecordV1] = Field(default_factory=list)
    event_log_path: str | None = None
    run_event_summary: RunEventSummaryV1 = Field(default_factory=RunEventSummaryV1)
    artifact_status: str = "PENDING"
    artifact_errors: list[str] = Field(default_factory=list)
    engine_completed_at: str | None = None
    artifact_completed_at: str | None = None
    component_manifest: ComponentManifestV1 = Field(default_factory=ComponentManifestV1)
    promotion_package: SignalPromotionPackageV1 | None = None
    signal_source_run_id: str | None = None
    signal_source_artifact_type: str | None = None

    @model_validator(mode="after")
    def _validate_contract(self) -> "RunManifestContractV6":
        if self.schema_version != 6:
            raise ValueError("仅支持 run manifest schema v6")
        return self


class ExecutionIntentMetadataV1(ContractBaseModel):
    signal_type: str | None = None
    dataset_version_id: str | None = None
    dataset_digest: str | None = None
    root_research_run_id: str | None = None
    research_session_id: str | None = None


class ExecutionIntentEnvelopeV1(ContractBaseModel):
    intent_contract_version: int = 1
    intent_type: str
    strategy_id: str
    trade_date: str
    runtime_mode: str
    source_run_id: str | None = None
    account_id: str | None = None
    promotion_package: SignalPromotionPackageV1 | None = None
    metadata: ExecutionIntentMetadataV1 = Field(default_factory=ExecutionIntentMetadataV1)


def parse_signal_promotion_package(payload: dict[str, Any]) -> SignalPromotionPackageV1:
    return SignalPromotionPackageV1.model_validate(payload)


def parse_signal_snapshot_payload(payload: dict[str, Any]) -> SignalSnapshotPayloadV1:
    return SignalSnapshotPayloadV1.model_validate(payload)


def _normalize_component_manifest(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(payload or {})
    known_keys = {
        "strategy_id",
        "strategy_class",
        "signal_component",
        "feature_component",
        "portfolio_component",
        "execution_component",
    }
    normalized = {key: raw.get(key) for key in known_keys if raw.get(key) is not None}
    normalized["extras"] = {key: value for key, value in raw.items() if key not in known_keys}
    return normalized


def _normalize_run_event_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(payload or {})
    return {
        "event_count": int(raw.get("event_count") or 0),
        "by_type": dict(raw.get("by_type") or {}),
        "lifecycle_summary": dict(raw.get("lifecycle_summary") or {}),
    }


def _normalize_report_artifacts(report_paths: list[str] | None, event_log_path: str | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, path in enumerate(report_paths or []):
        normalized.append(
            {
                "role": "primary" if index == 0 else f"report_copy_{index}",
                "path": path,
                "kind": "report",
                "format": "json",
                "primary": index == 0,
            }
        )
    if event_log_path:
        normalized.append({"role": "event_log", "path": event_log_path, "kind": "event_log", "format": "json", "primary": False})
    return normalized


def _normalize_run_manifest_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = dict(payload or {})
    schema_version = int(raw.get("schema_version") or 5)
    report_paths = list(raw.get("report_paths") or [])
    event_log_path = raw.get("event_log_path")

    def _normalize_promotion_package(value: Any) -> dict[str, Any] | None:
        if not value:
            return None
        if isinstance(value, BaseModel):
            data = value.model_dump(mode="python")
        else:
            data = dict(value)
        return data or None

    if schema_version == 6:
        normalized = dict(raw)
        normalized.setdefault("report_artifacts", _normalize_report_artifacts(report_paths, event_log_path))
        normalized["run_event_summary"] = _normalize_run_event_summary(normalized.get("run_event_summary"))
        normalized["component_manifest"] = _normalize_component_manifest(normalized.get("component_manifest"))
        normalized["promotion_package"] = _normalize_promotion_package(normalized.get("promotion_package"))
        return normalized
    promotion_package = _normalize_promotion_package(raw.get("promotion_package"))
    return {
        "schema_version": 6,
        "entrypoint": raw.get("entrypoint"),
        "strategy_version": raw.get("strategy_version"),
        "runtime_mode": raw.get("runtime_mode"),
        "benchmark_initial_value": raw.get("benchmark_initial_value"),
        "report_paths": report_paths,
        "report_artifacts": _normalize_report_artifacts(report_paths, event_log_path),
        "event_log_path": event_log_path,
        "run_event_summary": _normalize_run_event_summary(raw.get("run_event_summary")),
        "artifact_status": raw.get("artifact_status") or "PENDING",
        "artifact_errors": list(raw.get("artifact_errors") or []),
        "engine_completed_at": raw.get("engine_completed_at"),
        "artifact_completed_at": raw.get("artifact_completed_at"),
        "component_manifest": _normalize_component_manifest(raw.get("component_manifest")),
        "promotion_package": promotion_package,
        "signal_source_run_id": raw.get("signal_source_run_id"),
        "signal_source_artifact_type": raw.get("signal_source_artifact_type"),
    }


def parse_run_manifest_contract(payload: dict[str, Any]) -> RunManifestContractV6:
    return RunManifestContractV6.model_validate(_normalize_run_manifest_payload(payload))


def parse_execution_intent_envelope(payload: dict[str, Any]) -> ExecutionIntentEnvelopeV1:
    return ExecutionIntentEnvelopeV1.model_validate(payload)
