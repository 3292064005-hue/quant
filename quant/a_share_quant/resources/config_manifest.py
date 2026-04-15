"""配置清单真相源。"""
from __future__ import annotations

from pathlib import Path

EXPECTED_CONFIGS: tuple[Path, ...] = (
    Path("app.yaml"),
    Path("backtest.yaml"),
    Path("data.yaml"),
    Path("risk.yaml"),
    Path("operator_paper_trade.yaml"),
    Path("operator_paper_trade_demo.yaml"),
    Path("research_batch.json"),
    Path("broker/qmt.yaml"),
    Path("broker/ptrade.yaml"),
)
_PROFILE_EXCLUDED_CONFIGS: dict[str, tuple[Path, ...]] = {
    "core": (Path("operator_paper_trade.yaml"), Path("operator_paper_trade_demo.yaml"), Path("broker/qmt.yaml"), Path("broker/ptrade.yaml")),
    "workstation": (),
    "production": (Path("backtest.yaml"), Path("research_batch.json"), Path("operator_paper_trade_demo.yaml")),
}


def expected_configs_for_profile(profile: str = "workstation") -> tuple[Path, ...]:
    excluded = set(_PROFILE_EXCLUDED_CONFIGS.get(profile, ()))
    return tuple(path for path in EXPECTED_CONFIGS if path not in excluded)


def iter_expected_configs(profile: str = "workstation") -> tuple[Path, ...]:
    return expected_configs_for_profile(profile)
