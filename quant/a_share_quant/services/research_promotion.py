"""research 产物晋级合同。"""
from __future__ import annotations

from typing import Any

from a_share_quant.config.models import AppConfig


def build_signal_promotion_package(*, dataset_summary: dict[str, Any], feature_spec: dict[str, Any], top_n: int) -> dict[str, Any]:
    """构建 signal_snapshot 的正式晋级合同。"""
    return {
        "artifact_contract_version": 1,
        "promotion_stage": "research_candidate",
        "artifact_type": "signal_snapshot",
        "compatible_runtime_lanes": ["research_backtest", "paper_trade", "live_trade"],
        "compatible_signal_component": "research.signal_snapshot",
        "compatible_execution_contract": "component_runtime",
        "compatible_risk_gate": ["builtin.pre_trade_risk", "operator_pre_trade"],
        "dataset_version_id": dataset_summary.get("dataset_version_id"),
        "dataset_digest": dataset_summary.get("dataset_digest"),
        "signal_schema": {
            "kind": "top_n_equal_weight",
            "fields": ["ts_code", "score", "target_weight"],
            "top_n": int(top_n),
        },
        "feature_spec": feature_spec,
        "strategy_blueprint": {
            "signal": "research.signal_snapshot",
            "factor": "builtin.none",
            "portfolio_construction": "builtin.bypassed_portfolio",
        },
    }


def validate_signal_promotion_package(package: dict[str, Any] | None, *, config: AppConfig) -> dict[str, Any]:
    """校验 research signal 晋级合同与当前运行时是否兼容。"""
    if not isinstance(package, dict):
        raise ValueError("research signal 缺少 promotion_package，不能进入正式策略绑定链")
    if package.get("artifact_type") != "signal_snapshot":
        raise ValueError(f"promotion_package.artifact_type 非 signal_snapshot: {package.get('artifact_type')}")
    lanes = package.get("compatible_runtime_lanes") or []
    if config.app.runtime_mode not in lanes:
        raise ValueError(
            f"research signal 不兼容当前 runtime_mode={config.app.runtime_mode}；允许值={lanes}"
        )
    if package.get("compatible_signal_component") != "research.signal_snapshot":
        raise ValueError("promotion_package.signal_component 与当前 research.signal_snapshot 合同不一致")
    if package.get("compatible_execution_contract") != "component_runtime":
        raise ValueError("promotion_package.execution_contract 非当前正式 component_runtime 合同")
    return package
