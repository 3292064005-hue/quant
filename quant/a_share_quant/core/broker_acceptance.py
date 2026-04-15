"""Broker readiness 与 acceptance 证据模型。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from pathlib import Path
from typing import Any


class BrokerReadinessLevel(IntEnum):
    """按强度递增表达 broker 运行可用性。"""

    CONFIG_VALIDATED = 10
    BOUNDARY_VALIDATED = 20
    CLIENT_CONTRACT_VALIDATED = 30
    OPERABLE = 40
    STAGING_ACCEPTED = 50
    PRODUCTION_ACCEPTED = 60


_READINESS_ALIASES = {
    "config": BrokerReadinessLevel.CONFIG_VALIDATED,
    "config_validated": BrokerReadinessLevel.CONFIG_VALIDATED,
    "boundary": BrokerReadinessLevel.BOUNDARY_VALIDATED,
    "boundary_validated": BrokerReadinessLevel.BOUNDARY_VALIDATED,
    "client_contract": BrokerReadinessLevel.CLIENT_CONTRACT_VALIDATED,
    "client_contract_validated": BrokerReadinessLevel.CLIENT_CONTRACT_VALIDATED,
    "operable": BrokerReadinessLevel.OPERABLE,
    "staging": BrokerReadinessLevel.STAGING_ACCEPTED,
    "staging_accepted": BrokerReadinessLevel.STAGING_ACCEPTED,
    "production": BrokerReadinessLevel.PRODUCTION_ACCEPTED,
    "production_accepted": BrokerReadinessLevel.PRODUCTION_ACCEPTED,
}


@dataclass(slots=True)
class BrokerAcceptanceEvidence:
    """外部券商验收证据。"""

    provider: str
    readiness_level: str
    verified_at: str
    scenario_checks: list[str] = field(default_factory=list)
    suite_name: str | None = None
    environment: str | None = None
    expires_at: str | None = None
    capabilities: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def has_recovery_coverage(self) -> bool:
        normalized = {str(item).strip().lower() for item in self.scenario_checks}
        expected = {"submit", "sync", "reconcile", "restart_recovery"}
        return expected.issubset(normalized)

    def is_expired(self, *, now: datetime | None = None) -> bool:
        if not self.expires_at:
            return False
        current = now or datetime.now(timezone.utc)
        try:
            expires = _parse_timestamp(self.expires_at)
        except ValueError:
            return True
        return expires <= current


class BrokerAcceptanceError(ValueError):
    """Broker acceptance 证据不合法。"""


class BrokerAcceptanceMissingError(BrokerAcceptanceError):
    """缺少 acceptance 证据。"""


class BrokerAcceptanceStaleError(BrokerAcceptanceError):
    """Acceptance 证据过期。"""


class BrokerAcceptanceMismatchError(BrokerAcceptanceError):
    """Acceptance 证据与当前 provider/runtime 不匹配。"""


class BrokerAcceptanceInsufficientError(BrokerAcceptanceError):
    """Acceptance 证据强度不足。"""


def normalize_readiness_level(value: str | BrokerReadinessLevel | None) -> BrokerReadinessLevel:
    """把用户输入标准化为 readiness level。"""
    if isinstance(value, BrokerReadinessLevel):
        return value
    if value is None:
        return BrokerReadinessLevel.CONFIG_VALIDATED
    raw = str(value).strip().lower()
    if not raw:
        return BrokerReadinessLevel.CONFIG_VALIDATED
    try:
        return _READINESS_ALIASES[raw]
    except KeyError as exc:
        allowed = ", ".join(sorted(_READINESS_ALIASES))
        raise BrokerAcceptanceError(f"未知 broker readiness level={value}；允许值: {allowed}") from exc


def readiness_level_name(value: str | BrokerReadinessLevel | None) -> str:
    """返回标准化的 readiness level 名称。"""
    return normalize_readiness_level(value).name.lower()


def is_readiness_sufficient(current: str | BrokerReadinessLevel | None, required: str | BrokerReadinessLevel | None) -> bool:
    """判断当前 readiness 是否满足目标要求。"""
    return normalize_readiness_level(current) >= normalize_readiness_level(required)


def derive_required_readiness_level(
    *,
    runtime_mode: str | None,
    distribution_profile: str | None,
    explicit_requirement: str | None = None,
) -> BrokerReadinessLevel:
    """按 profile/runtime 派生最小 broker readiness 要求。"""
    if explicit_requirement:
        return normalize_readiness_level(explicit_requirement)
    normalized_mode = str(runtime_mode or "").strip().lower()
    normalized_profile = str(distribution_profile or "").strip().lower()
    if normalized_mode == "live_trade" and normalized_profile == "production":
        return BrokerReadinessLevel.PRODUCTION_ACCEPTED
    if normalized_mode == "paper_trade" and normalized_profile == "production":
        return BrokerReadinessLevel.STAGING_ACCEPTED
    if normalized_mode in {"paper_trade", "live_trade"}:
        return BrokerReadinessLevel.OPERABLE
    return BrokerReadinessLevel.CONFIG_VALIDATED


def compute_broker_readiness_level(
    *,
    config_ok: bool,
    boundary_ok: bool,
    client_contract_ok: bool,
    operable_ok: bool,
    evidence: BrokerAcceptanceEvidence | None = None,
) -> BrokerReadinessLevel:
    """根据分层布尔能力与证据计算 readiness level。"""
    if evidence is not None:
        return normalize_readiness_level(evidence.readiness_level)
    if operable_ok:
        return BrokerReadinessLevel.OPERABLE
    if client_contract_ok:
        return BrokerReadinessLevel.CLIENT_CONTRACT_VALIDATED
    if boundary_ok:
        return BrokerReadinessLevel.BOUNDARY_VALIDATED
    if config_ok:
        return BrokerReadinessLevel.CONFIG_VALIDATED
    return BrokerReadinessLevel.CONFIG_VALIDATED


def load_acceptance_evidence(
    manifest_path: str | Path,
    *,
    provider: str,
    runtime_mode: str | None = None,
) -> BrokerAcceptanceEvidence:
    """从 JSON manifest 读取 broker acceptance 证据。"""
    path = Path(manifest_path)
    if not path.exists():
        raise BrokerAcceptanceMissingError(f"broker acceptance manifest 不存在: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BrokerAcceptanceError(f"broker acceptance manifest 非法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise BrokerAcceptanceError("broker acceptance manifest 根节点必须是 JSON object")
    payload_provider = str(payload.get("provider") or "").strip().lower()
    expected_provider = str(provider).strip().lower()
    if payload_provider != expected_provider:
        raise BrokerAcceptanceMismatchError(
            f"broker acceptance manifest provider 不匹配: expected={expected_provider} actual={payload_provider or 'empty'}"
        )
    readiness_level = readiness_level_name(payload.get("readiness_level"))
    verified_at = str(payload.get("verified_at") or "").strip()
    if not verified_at:
        raise BrokerAcceptanceError("broker acceptance manifest 缺少 verified_at")
    if runtime_mode:
        allowed_modes = {str(item).strip().lower() for item in payload.get("runtime_modes") or []}
        if allowed_modes and str(runtime_mode).strip().lower() not in allowed_modes:
            raise BrokerAcceptanceMismatchError(
                f"broker acceptance manifest 不覆盖 runtime_mode={runtime_mode}；允许值={sorted(allowed_modes)}"
            )
    evidence = BrokerAcceptanceEvidence(
        provider=payload_provider,
        readiness_level=readiness_level,
        verified_at=verified_at,
        scenario_checks=[str(item).strip().lower() for item in payload.get("scenario_checks") or [] if str(item).strip()],
        suite_name=str(payload.get("suite_name") or "").strip() or None,
        environment=str(payload.get("environment") or "").strip() or None,
        expires_at=str(payload.get("expires_at") or "").strip() or None,
        capabilities=dict(payload.get("capabilities") or {}),
        metadata={
            key: value
            for key, value in payload.items()
            if key
            not in {
                "provider",
                "readiness_level",
                "verified_at",
                "scenario_checks",
                "suite_name",
                "environment",
                "expires_at",
                "capabilities",
                "runtime_modes",
            }
        },
    )
    if evidence.is_expired():
        raise BrokerAcceptanceStaleError(f"broker acceptance manifest 已过期: {path}")
    return evidence


def summarize_acceptance_evidence(evidence: BrokerAcceptanceEvidence | None) -> dict[str, Any]:
    """把 acceptance 证据归一化为可序列化摘要。"""
    if evidence is None:
        return {
            "present": False,
            "readiness_level": None,
            "recovery_covered": False,
        }
    return {
        "present": True,
        "provider": evidence.provider,
        "readiness_level": readiness_level_name(evidence.readiness_level),
        "suite_name": evidence.suite_name,
        "environment": evidence.environment,
        "verified_at": evidence.verified_at,
        "expires_at": evidence.expires_at,
        "scenario_checks": list(evidence.scenario_checks),
        "recovery_covered": evidence.has_recovery_coverage(),
        "capabilities": dict(evidence.capabilities),
        "metadata": dict(evidence.metadata),
    }


def validate_acceptance_requirement(
    evidence: BrokerAcceptanceEvidence | None,
    *,
    required_readiness_level: str | BrokerReadinessLevel,
) -> None:
    """检查 acceptance 证据是否满足目标 readiness。"""
    required = normalize_readiness_level(required_readiness_level)
    if evidence is None:
        raise BrokerAcceptanceMissingError(
            f"缺少 broker acceptance 证据；当前运行至少要求 {required.name.lower()}"
        )
    current = normalize_readiness_level(evidence.readiness_level)
    if current < required:
        raise BrokerAcceptanceInsufficientError(
            f"broker acceptance readiness 不足: current={current.name.lower()} required={required.name.lower()}"
        )


def build_acceptance_manifest(
    *,
    provider: str,
    readiness_level: str,
    scenario_checks: list[str],
    suite_name: str,
    environment: str,
    expires_in_days: int = 30,
    capabilities: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """为 demo/fixture 构造标准 acceptance manifest。"""
    verified_at = datetime.now(timezone.utc)
    expires_at = verified_at + timedelta(days=max(int(expires_in_days), 1))
    return {
        "provider": provider,
        "readiness_level": readiness_level_name(readiness_level),
        "verified_at": verified_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "suite_name": suite_name,
        "environment": environment,
        "scenario_checks": [str(item).strip().lower() for item in scenario_checks if str(item).strip()],
        "capabilities": dict(capabilities or {}),
    }


def _parse_timestamp(value: str) -> datetime:
    raw = str(value).strip()
    if not raw:
        raise ValueError("timestamp 为空")
    normalized = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
