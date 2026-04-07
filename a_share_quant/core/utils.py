"""通用工具函数。"""
from __future__ import annotations

import json
import math
import uuid
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any


def now_iso() -> str:
    """返回当前 UTC ISO 时间字符串。"""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def ensure_parent(path: str | Path) -> Path:
    """确保目标文件父目录存在。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def new_id(prefix: str) -> str:
    """生成带前缀的唯一标识。"""
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def json_dumps(value: Any) -> str:
    """安全序列化 JSON。"""
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        return value.model_dump()
    raise TypeError(f"对象不可序列化: {type(value)!r}")


def floor_to_lot(quantity: int | float, lot_size: int = 100) -> int:
    """按整数手向下取整。"""
    return int(math.floor(float(quantity) / lot_size) * lot_size)


def parse_yyyymmdd(value: str | None) -> date | None:
    """解析 YYYYMMDD 或 ISO 日期为 `date`。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan" or text.lower() == "none":
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)
