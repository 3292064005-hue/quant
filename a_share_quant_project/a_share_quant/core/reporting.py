"""报告输出。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from a_share_quant.core.utils import ensure_parent, json_dumps


class ReportWriter:
    """将结果写入 JSON/CSV。"""

    def write_json(self, path: str | Path, payload: dict[str, Any]) -> Path:
        """写入 JSON 文件。"""
        target = ensure_parent(path)
        target.write_text(json_dumps(payload), encoding="utf-8")
        return target
