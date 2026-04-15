"""通用工具函数。"""
from __future__ import annotations

import hashlib
import json
import math
import threading
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


_NOW_ISO_LOCK = threading.Lock()
_LAST_NOW_ISO: datetime | None = None


def now_iso() -> str:
    """返回单调递增的 UTC ISO 时间字符串。

    Boundary Behavior:
        - 精度保留到微秒，避免 runtime event 在同秒内发生排序碰撞；
        - 若系统时钟回拨或同一微秒内被多次调用，会自动顺延 1 微秒，
          保证当前进程内时间戳严格单调递增；
        - 输出保持为 timezone-aware ISO-8601 文本，可直接参与字符串排序。
    """
    global _LAST_NOW_ISO
    with _NOW_ISO_LOCK:
        candidate = datetime.now(timezone.utc).astimezone(timezone.utc)
        if _LAST_NOW_ISO is not None and candidate <= _LAST_NOW_ISO:
            candidate = _LAST_NOW_ISO + timedelta(microseconds=1)
        _LAST_NOW_ISO = candidate
        return candidate.isoformat(timespec="microseconds")


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


def canonical_json_dumps(value: Any) -> str:
    """以稳定排序输出 JSON，适用于哈希与指纹计算。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def build_dataset_version_fingerprint(
    *,
    dataset_digest: str,
    data_source: str,
    data_start_date: str | None,
    data_end_date: str | None,
    scope: Any,
    import_run_ids: list[str],
    degradation_flags: list[str],
    warnings: list[str],
) -> str:
    """基于数据快照与 provenance 语义生成稳定指纹。"""
    payload = {
        "dataset_digest": dataset_digest,
        "data_source": data_source,
        "data_start_date": data_start_date,
        "data_end_date": data_end_date,
        "scope": scope,
        "import_run_ids": import_run_ids,
        "degradation_flags": degradation_flags,
        "warnings": warnings,
    }
    return hashlib.sha256(canonical_json_dumps(payload).encode("utf-8")).hexdigest()


def _json_default(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
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
    if not text or text.lower() in {"nan", "none"}:
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)
