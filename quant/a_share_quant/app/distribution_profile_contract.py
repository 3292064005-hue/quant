"""发布形态单一真相源。"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class DistributionProfileSpec:
    """单个 distribution profile 的正式契约。"""

    profile: str
    project_name: str
    description: str
    selected_requirements: str
    excluded_paths: tuple[str, ...]
    capabilities: dict[str, Any]


_PROFILE_SPECS: dict[str, DistributionProfileSpec] = {
    "core": DistributionProfileSpec(
        profile="core",
        project_name="a-share-quant-core",
        description="A-share quantitative core research release",
        selected_requirements="requirements-core.txt",
        excluded_paths=(
            "a_share_quant/ui",
            "docs/operator_manual.md",
            "sample_data",
            "scripts/launch_ui.py",
            "scripts/operator_snapshot.py",
            "scripts/operator_submit_order.py",
            "scripts/operator_sync_session.py",
            "scripts/operator_reconcile_session.py",
            "scripts/operator_run_supervisor.py",
            "configs/operator_paper_trade.yaml",
            "configs/operator_paper_trade_demo.yaml",
            "configs/broker/qmt.yaml",
            "configs/broker/ptrade.yaml",
            "a_share_quant/resources/configs/operator_paper_trade.yaml",
            "a_share_quant/resources/configs/operator_paper_trade_demo.yaml",
            "a_share_quant/resources/configs/broker/qmt.yaml",
            "a_share_quant/resources/configs/broker/ptrade.yaml",
        ),
        capabilities={
            "profile": "core",
            "supports_ui": False,
            "supports_headless_scripts": True,
            "supports_research_workflow": True,
            "supports_operator_runtime": False,
            "enforces_strict_market_contract": False,
        },
    ),
    "workstation": DistributionProfileSpec(
        profile="workstation",
        project_name="a-share-quant-workstation",
        description="A-share quantitative research and execution workstation",
        selected_requirements="requirements-workstation.txt",
        excluded_paths=(),
        capabilities={
            "profile": "workstation",
            "supports_ui": True,
            "supports_headless_scripts": True,
            "supports_research_workflow": True,
            "supports_operator_runtime": True,
            "enforces_strict_market_contract": False,
        },
    ),
    "production": DistributionProfileSpec(
        profile="production",
        project_name="a-share-quant-production",
        description="A-share quantitative production operator release",
        selected_requirements="requirements-production.txt",
        excluded_paths=(
            "a_share_quant/ui",
            "docs/strategy_spec.md",
            "sample_data",
            "scripts/launch_ui.py",
            "scripts/research.py",
            "configs/backtest.yaml",
            "configs/research_batch.json",
            "configs/operator_paper_trade_demo.yaml",
            "a_share_quant/resources/configs/backtest.yaml",
            "a_share_quant/resources/configs/research_batch.json",
            "a_share_quant/resources/configs/operator_paper_trade_demo.yaml",
        ),
        capabilities={
            "profile": "production",
            "supports_ui": False,
            "supports_headless_scripts": True,
            "supports_research_workflow": False,
            "supports_operator_runtime": True,
            "enforces_strict_market_contract": True,
        },
    ),
}


ALL_DISTRIBUTION_PROFILES: tuple[str, ...] = tuple(_PROFILE_SPECS)


def get_distribution_profile_spec(profile: str) -> DistributionProfileSpec:
    normalized = str(profile).strip().lower()
    try:
        return _PROFILE_SPECS[normalized]
    except KeyError as exc:
        raise ValueError(f"distribution_profile 不支持: {profile}") from exc


def iter_distribution_profile_specs() -> tuple[DistributionProfileSpec, ...]:
    return tuple(_PROFILE_SPECS.values())


def profile_requirement_filenames() -> tuple[str, ...]:
    return tuple(spec.selected_requirements for spec in _PROFILE_SPECS.values())


def ensure_profile_surface_exists(project_root: Path, profile: str) -> DistributionProfileSpec:
    spec = get_distribution_profile_spec(profile)
    requirement_path = project_root / spec.selected_requirements
    if not requirement_path.exists():
        raise FileNotFoundError(
            f"distribution profile={profile} 缺少 requirements surface: {spec.selected_requirements}"
        )
    return spec


def build_distribution_manifest(profile: str) -> dict[str, Any]:
    spec = get_distribution_profile_spec(profile)
    return {
        "distribution_profile": spec.profile,
        "project_name": spec.project_name,
        "description": spec.description,
        "selected_requirements": spec.selected_requirements,
        "excluded_paths": list(spec.excluded_paths),
        "capabilities": dict(spec.capabilities),
    }
