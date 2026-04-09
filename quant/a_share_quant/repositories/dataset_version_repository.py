"""数据版本仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import build_dataset_version_fingerprint, json_dumps, new_id, now_iso
from a_share_quant.domain.models import DatasetVersion
from a_share_quant.storage.sqlite_store import SQLiteStore


class DatasetVersionRepository:
    """持久化回测可引用的数据版本快照摘要。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    @staticmethod
    def _normalize_values(values: list[str]) -> list[str]:
        """去重并排序集合型 provenance 字段，避免顺序漂移导致伪差异。"""
        return sorted({str(item) for item in values if str(item).strip()})

    def create_or_touch(
        self,
        *,
        dataset_digest: str,
        data_source: str,
        data_start_date: str | None,
        data_end_date: str | None,
        scope: dict,
        import_run_ids: list[str],
        degradation_flags: list[str],
        warnings: list[str],
    ) -> DatasetVersion:
        """按数据摘要创建或复用数据版本。

        Args:
            dataset_digest: 当前数据快照的确定性哈希。
            data_source: 数据来源摘要，例如 ``csv`` / ``mixed_snapshot``。
            data_start_date: 数据起始日期。
            data_end_date: 数据结束日期。
            scope: 快照作用域摘要，用于审计当前版本覆盖的 symbol / exchange / row count。
            import_run_ids: 当前快照实际涉及的导入运行集合。
            degradation_flags: 关联导入运行聚合后的降级标记。
            warnings: 关联导入运行聚合后的 warning 列表。

        Returns:
            已存在或新创建的 ``DatasetVersion``。

        Boundary Behavior:
            - 相同 ``version_fingerprint`` 视为同一数据版本，只刷新 ``last_used_at``；
            - 若数据库中尚无该 provenance 指纹，则创建新的 ``dataset_version_id``；
            - 不会伪造不存在的 ``import_run_ids``。
        """
        normalized_import_run_ids = self._normalize_values(import_run_ids)
        normalized_degradation_flags = self._normalize_values(degradation_flags)
        normalized_warnings = self._normalize_values(warnings)
        version_fingerprint = build_dataset_version_fingerprint(
            dataset_digest=dataset_digest,
            data_source=data_source,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            scope=scope,
            import_run_ids=normalized_import_run_ids,
            degradation_flags=normalized_degradation_flags,
            warnings=normalized_warnings,
        )
        existing = self.get_by_fingerprint(version_fingerprint)
        if existing is not None:
            self.store.execute(
                "UPDATE dataset_versions SET last_used_at = ? WHERE dataset_version_id = ?",
                (now_iso(), existing.dataset_version_id),
            )
            refreshed = self.get_by_id(existing.dataset_version_id)
            if refreshed is None:  # pragma: no cover - 防御性保护
                raise RuntimeError(f"刷新数据版本后读取失败 dataset_version_id={existing.dataset_version_id}")
            return refreshed
        dataset_version_id = new_id("dataset")
        now = now_iso()
        self.store.execute(
            """
            INSERT INTO dataset_versions
            (dataset_version_id, version_fingerprint, dataset_digest, data_source, data_start_date, data_end_date,
             scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dataset_version_id,
                version_fingerprint,
                dataset_digest,
                data_source,
                data_start_date,
                data_end_date,
                json_dumps(scope),
                json_dumps(normalized_import_run_ids),
                json_dumps(normalized_degradation_flags),
                json_dumps(normalized_warnings),
                now,
                now,
            ),
        )
        created = self.get_by_id(dataset_version_id)
        if created is None:  # pragma: no cover - 防御性保护
            raise RuntimeError(f"创建数据版本后读取失败 dataset_version_id={dataset_version_id}")
        return created

    def get_by_fingerprint(self, version_fingerprint: str) -> DatasetVersion | None:
        """按 ``version_fingerprint`` 读取数据版本。"""
        rows = self.store.query(
            """
            SELECT dataset_version_id, version_fingerprint, dataset_digest, data_source, data_start_date, data_end_date,
                   scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at
            FROM dataset_versions WHERE version_fingerprint = ?
            """,
            (version_fingerprint,),
        )
        if not rows:
            return None
        return DatasetVersion(**dict(rows[0]))

    def get_by_digest(self, dataset_digest: str) -> DatasetVersion | None:
        """按 ``dataset_digest`` 读取数据版本。"""
        rows = self.store.query(
            """
            SELECT dataset_version_id, version_fingerprint, dataset_digest, data_source, data_start_date, data_end_date,
                   scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at
            FROM dataset_versions WHERE dataset_digest = ?
            """,
            (dataset_digest,),
        )
        if not rows:
            return None
        return DatasetVersion(**dict(rows[0]))

    def get_by_id(self, dataset_version_id: str) -> DatasetVersion | None:
        """按 ``dataset_version_id`` 读取数据版本。"""
        rows = self.store.query(
            """
            SELECT dataset_version_id, version_fingerprint, dataset_digest, data_source, data_start_date, data_end_date,
                   scope_json, import_run_ids_json, degradation_flags_json, warnings_json, created_at, last_used_at
            FROM dataset_versions WHERE dataset_version_id = ?
            """,
            (dataset_version_id,),
        )
        if not rows:
            return None
        return DatasetVersion(**dict(rows[0]))
