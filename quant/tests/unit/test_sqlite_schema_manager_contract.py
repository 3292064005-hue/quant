from __future__ import annotations

from a_share_quant.storage.sqlite_schema_manager import SQLiteMigrationStep, SQLiteSchemaManager


class _Host:
    CURRENT_SCHEMA_VERSION = 10

    def __init__(self, version: int = 0) -> None:
        self.version = version
        self.applied: list[int] = []

    def _ensure_open(self) -> None:
        return None

    def _ensure_schema_version_table(self) -> None:
        return None

    def _has_existing_application_schema(self) -> bool:
        return True

    def _get_schema_version(self) -> int:
        return self.version

    def _set_schema_version(self, version: int) -> None:
        self.version = version

    class _Txn:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    def transaction(self):
        return self._Txn()


def test_sqlite_schema_manager_sorts_and_applies_monotonic_migrations() -> None:
    host = _Host(version=1)
    manager = SQLiteSchemaManager(
        host,
        migrations=[
            SQLiteMigrationStep(1, lambda: host.applied.append(1)),
            SQLiteMigrationStep(3, lambda: host.applied.append(3), depends_on=(2,)),
            SQLiteMigrationStep(2, lambda: host.applied.append(2), depends_on=(1,)),
        ],
    )
    manager.apply_migrations()
    assert host.applied == [2, 3]
    assert host.version == 3


def test_sqlite_schema_manager_rejects_invalid_dependencies() -> None:
    host = _Host()
    manager = SQLiteSchemaManager(
        host,
        migrations=[SQLiteMigrationStep(2, lambda: None, depends_on=(3,))],
    )
    try:
        manager.validate_migrations()
    except ValueError as exc:
        assert "depends_on 非法" in str(exc) or "depends_on 缺失" in str(exc)
    else:
        raise AssertionError("应拒绝非法 migration dependency")
