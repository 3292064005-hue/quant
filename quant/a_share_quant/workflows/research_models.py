"""research workflow 共享数据模型。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ResearchArtifactSummary:
    dataset: dict[str, Any]
    feature: dict[str, Any] | None = None
    signal: dict[str, Any] | None = None
    experiment: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ResearchTaskSpec:
    task_name: str
    feature_name: str = "momentum"
    lookback: int = 3
    top_n: int = 2
    start_date: date | None = None
    end_date: date | None = None
    ts_codes: tuple[str, ...] = ()

    @classmethod
    def from_payload(cls, index: int, payload: dict[str, Any]) -> "ResearchTaskSpec":
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
    research_session_id: str | None = None
    parent_research_run_id: str | None = None
    root_research_run_id: str | None = None
    step_name: str | None = None
    is_primary_run: bool = True


@dataclass(frozen=True, slots=True)
class ComputedFeatureSnapshot:
    request: dict[str, Any]
    result: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ComputedSignalSnapshot:
    request: dict[str, Any]
    result: dict[str, Any]
    feature_snapshot: ComputedFeatureSnapshot


def load_research_task_specs(path: str | Path) -> list[ResearchTaskSpec]:
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
