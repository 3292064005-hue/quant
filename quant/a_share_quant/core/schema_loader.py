"""Schema 资源加载工具。"""
from __future__ import annotations

from importlib.resources import files


SCHEMA_RESOURCE_NAME = "schema.sql"


def load_schema_sql() -> str:
    """读取内置 SQLite schema 资源。

    Returns:
        完整 schema SQL 文本。

    Raises:
        FileNotFoundError: 包内缺少 schema 资源时抛出。
    """
    return files("a_share_quant").joinpath(SCHEMA_RESOURCE_NAME).read_text(encoding="utf-8")
