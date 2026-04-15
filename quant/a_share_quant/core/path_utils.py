"""运行时路径解析工具。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

_ALLOWED_PATH_RESOLUTION_MODES = {"config_dir", "cwd"}
_RUNTIME_PATH_FIELDS: tuple[tuple[str, ...], ...] = (
    ("app", "logs_dir"),
    ("data", "storage_dir"),
    ("data", "reports_dir"),
    ("database", "path"),
    ("broker", "acceptance_manifest_path"),
)


def resolve_runtime_path(raw_path: str, *, config_path: Path, mode: str) -> str:
    """将运行时路径解析为绝对路径。

    Args:
        raw_path: 配置中声明的原始路径。可为绝对路径或相对路径。
        config_path: 当前主配置文件路径，用于 ``config_dir`` 模式解析。
        mode: 路径解析模式，仅允许 ``config_dir`` 或 ``cwd``。

    Returns:
        绝对路径字符串；若 ``raw_path`` 为空，则原样返回。

    Raises:
        ValueError: 当 ``mode`` 不受支持时抛出。

    Boundary Behavior:
        绝对路径不会被改写；相对路径在 ``config_dir`` 模式下相对配置文件目录解析，
        在 ``cwd`` 模式下相对当前工作目录解析。
    """
    if not raw_path:
        return raw_path
    if mode not in _ALLOWED_PATH_RESOLUTION_MODES:
        raise ValueError(f"不支持的路径解析模式: {mode}")
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return str(candidate)
    base_dir = config_path.parent if mode == "config_dir" else Path.cwd()
    return str((base_dir / candidate).resolve())



def normalize_runtime_paths(payload: dict[str, Any], *, config_path: Path) -> dict[str, Any]:
    """对配置载荷中的运行时路径字段执行标准化。

    Args:
        payload: 经过 companion 文件合并后的配置字典。
        config_path: 主配置文件路径。

    Returns:
        已将受支持路径字段解析为绝对路径的新字典。

    Raises:
        ValueError: 当 ``path_resolution_mode`` 非法时抛出。
    """
    normalized = dict(payload)
    app_section = dict(normalized.get("app", {}))
    mode = str(app_section.get("path_resolution_mode", "config_dir")).strip().lower() or "config_dir"
    if mode not in _ALLOWED_PATH_RESOLUTION_MODES:
        raise ValueError(f"app.path_resolution_mode 不支持: {mode}")
    app_section["path_resolution_mode"] = mode
    normalized["app"] = app_section

    for section_name, field_name in _RUNTIME_PATH_FIELDS:
        section = dict(normalized.get(section_name, {}))
        raw_value = section.get(field_name)
        if isinstance(raw_value, str) and raw_value:
            section[field_name] = resolve_runtime_path(raw_value, config_path=config_path, mode=mode)
        normalized[section_name] = section
    return normalized
