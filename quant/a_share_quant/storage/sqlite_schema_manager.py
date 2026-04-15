"""SQLite schema 初始化与 migration 协调器。"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol


class _SQLiteSchemaHost(Protocol):
    CURRENT_SCHEMA_VERSION: int

    def _ensure_open(self) -> None: ...
    def _ensure_schema_version_table(self) -> None: ...
    def _has_existing_application_schema(self) -> bool: ...
    def _get_schema_version(self) -> int: ...
    def _set_schema_version(self, version: int) -> None: ...
    def transaction(self): ...


@dataclass(frozen=True, slots=True)
class SQLiteMigrationStep:
    """单个版本 migration 步骤。"""

    version: int
    apply: Callable[[], None]
    description: str = ""
    depends_on: tuple[int, ...] = field(default_factory=tuple)
    idempotent: bool = True


class SQLiteSchemaManager:
    """协调 SQLite schema 初始化与版本化迁移。"""

    def __init__(self, host: _SQLiteSchemaHost, *, migrations: list[SQLiteMigrationStep]) -> None:
        self.host = host
        self.migrations = list(migrations)

    def init_schema(self, schema_sql: str, *, execute_script: Callable[[str], None], commit: Callable[[], None]) -> None:
        """初始化数据库表结构并应用版本化迁移。"""
        self.host._ensure_open()
        self.host._ensure_schema_version_table()
        if self.host._has_existing_application_schema():
            self.apply_migrations()
            execute_script(schema_sql)
            if self.host._get_schema_version() < self.host.CURRENT_SCHEMA_VERSION:
                self.host._set_schema_version(self.host.CURRENT_SCHEMA_VERSION)
        else:
            execute_script(schema_sql)
            self.host._set_schema_version(self.host.CURRENT_SCHEMA_VERSION)
        commit()

    def validate_migrations(self) -> list[SQLiteMigrationStep]:
        """按 version 排序并校验 migration 元数据。"""
        ordered = sorted(self.migrations, key=lambda item: item.version)
        seen_versions: set[int] = set()
        known_versions = {step.version for step in ordered}
        for step in ordered:
            if step.version in seen_versions:
                raise ValueError(f"重复 migration version: {step.version}")
            seen_versions.add(step.version)
            invalid = [dep for dep in step.depends_on if dep >= step.version]
            if invalid:
                raise ValueError(f"migration version={step.version} depends_on 非法: {invalid}")
            missing = [dep for dep in step.depends_on if dep not in known_versions]
            if missing:
                raise ValueError(f"migration version={step.version} depends_on 缺失: {missing}")
        return ordered

    def apply_migrations(self) -> None:
        """按版本顺序执行所有待应用 migration。"""
        current_version = self.host._get_schema_version()
        applied_versions = {step.version for step in self.validate_migrations() if step.version <= current_version}
        for step in self.validate_migrations():
            if step.version <= current_version:
                continue
            unmet = [dep for dep in step.depends_on if dep not in applied_versions]
            if unmet:
                raise ValueError(
                    f"migration version={step.version} 依赖未满足: {unmet}; current_version={current_version}"
                )
            with self.host.transaction():
                step.apply()
                self.host._set_schema_version(step.version)
            applied_versions.add(step.version)
            current_version = step.version
