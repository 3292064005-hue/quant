"""运行 lane 与装配能力定义。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RuntimeLane(str, Enum):
    """正式运行 lane。"""

    RESEARCH_BACKTEST = "research_backtest"
    PAPER_TRADE = "paper_trade"
    LIVE_TRADE = "live_trade"


@dataclass(slots=True, frozen=True)
class RuntimeLaneProfile:
    """描述某个运行 lane 的正式能力边界。"""

    lane: RuntimeLane
    assembly_name: str
    allow_mock_broker: bool
    supports_backtest: bool
    supports_report_rebuild: bool
    supports_operator_broker: bool
    supports_research_read: bool
    supports_strategy_promotion: bool
    supports_execution: bool
    supports_operator_commands: bool

    @property
    def supports_research_workflow(self) -> bool:
        """兼容旧调用方的 research 能力别名。"""
        return self.supports_research_read


_RUNTIME_PROFILES: dict[RuntimeLane, RuntimeLaneProfile] = {
    RuntimeLane.RESEARCH_BACKTEST: RuntimeLaneProfile(
        lane=RuntimeLane.RESEARCH_BACKTEST,
        assembly_name="research_backtest",
        allow_mock_broker=True,
        supports_backtest=True,
        supports_report_rebuild=True,
        supports_operator_broker=False,
        supports_research_read=True,
        supports_strategy_promotion=True,
        supports_execution=False,
        supports_operator_commands=False,
    ),
    RuntimeLane.PAPER_TRADE: RuntimeLaneProfile(
        lane=RuntimeLane.PAPER_TRADE,
        assembly_name="paper_trade",
        allow_mock_broker=False,
        supports_backtest=False,
        supports_report_rebuild=True,
        supports_operator_broker=True,
        supports_research_read=True,
        supports_strategy_promotion=True,
        supports_execution=True,
        supports_operator_commands=True,
    ),
    RuntimeLane.LIVE_TRADE: RuntimeLaneProfile(
        lane=RuntimeLane.LIVE_TRADE,
        assembly_name="live_trade",
        allow_mock_broker=False,
        supports_backtest=False,
        supports_report_rebuild=True,
        supports_operator_broker=True,
        supports_research_read=True,
        supports_strategy_promotion=True,
        supports_execution=True,
        supports_operator_commands=True,
    ),
}


def parse_runtime_lane(value: str) -> RuntimeLane:
    """把配置中的 ``app.runtime_mode`` 解析为正式 lane。"""
    normalized = str(value).strip().lower()
    try:
        return RuntimeLane(normalized)
    except ValueError as exc:  # pragma: no cover - 配置模型已兜底
        raise ValueError(f"未知运行 lane: {value}") from exc


def get_runtime_profile(value: str | RuntimeLane) -> RuntimeLaneProfile:
    """返回正式运行 lane 画像。"""
    lane = value if isinstance(value, RuntimeLane) else parse_runtime_lane(value)
    return _RUNTIME_PROFILES[lane]
