"""市场数据导入审计仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import json_dumps, new_id, now_iso
from a_share_quant.domain.models import DataImportRun
from a_share_quant.storage.sqlite_store import SQLiteStore


class DataImportRepository:
    """持久化市场数据导入运行记录与质量事件。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    @staticmethod
    def _normalize_summary_items(values: list[str] | None) -> list[str]:
        """规范化导入摘要中的 flag/warning 列表。

        Boundary Behavior:
            - 去除空字符串；
            - 保持稳定排序，避免同一导入在不同入口产生重复/乱序摘要；
            - 不改变调用方的原始列表对象。
        """
        if not values:
            return []
        return sorted({str(item).strip() for item in values if str(item).strip()})

    def create_run(self, source: str, request_context: dict) -> str:
        """创建导入运行记录并返回 ``import_run_id``。"""
        import_run_id = new_id("import")
        self.store.execute(
            """
            INSERT INTO data_import_runs
            (import_run_id, source, status, request_context_json, started_at, finished_at,
             securities_count, calendar_count, bars_count, degradation_flags_json, warnings_json, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (import_run_id, source, "RUNNING", json_dumps(request_context), now_iso(), None, 0, 0, 0, "[]", "[]", None),
        )
        return import_run_id

    def finish_run(
        self,
        import_run_id: str,
        *,
        status: str,
        securities_count: int = 0,
        calendar_count: int = 0,
        bars_count: int = 0,
        degradation_flags: list[str] | None = None,
        warnings: list[str] | None = None,
        error_message: str | None = None,
    ) -> None:
        """结束导入运行并写入统计摘要。"""
        self.store.execute(
            """
            UPDATE data_import_runs
            SET status = ?,
                finished_at = ?,
                securities_count = ?,
                calendar_count = ?,
                bars_count = ?,
                degradation_flags_json = ?,
                warnings_json = ?,
                error_message = COALESCE(?, error_message)
            WHERE import_run_id = ?
            """,
            (
                status,
                now_iso(),
                securities_count,
                calendar_count,
                bars_count,
                json_dumps(self._normalize_summary_items(degradation_flags)),
                json_dumps(self._normalize_summary_items(warnings)),
                error_message,
                import_run_id,
            ),
        )

    def write_quality_event(self, import_run_id: str, event_type: str, payload: dict, level: str = "INFO") -> None:
        """写入一条导入质量事件。"""
        self.store.execute(
            """
            INSERT INTO data_import_quality_events
            (event_id, import_run_id, event_type, payload_json, level, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (new_id("dq"), import_run_id, event_type, json_dumps(payload), level, now_iso()),
        )

    def list_quality_events(self, import_run_id: str) -> list[dict[str, str]]:
        """列出指定导入运行的质量事件。"""
        rows = self.store.query(
            "SELECT event_type, payload_json, level, created_at FROM data_import_quality_events WHERE import_run_id = ? ORDER BY created_at",
            (import_run_id,),
        )
        return [dict(row) for row in rows]

    def get_run(self, import_run_id: str) -> DataImportRun | None:
        """读取指定导入运行。"""
        rows = self.store.query(
            """
            SELECT import_run_id, source, status, request_context_json, started_at, finished_at,
                   securities_count, calendar_count, bars_count, degradation_flags_json, warnings_json, error_message
            FROM data_import_runs WHERE import_run_id = ?
            """,
            (import_run_id,),
        )
        if not rows:
            return None
        return DataImportRun(**dict(rows[0]))

    def list_runs(self, import_run_ids: list[str]) -> list[DataImportRun]:
        """按给定顺序读取多条导入运行。"""
        if not import_run_ids:
            return []
        placeholders = ",".join("?" for _ in import_run_ids)
        rows = self.store.query(
            f"""
            SELECT import_run_id, source, status, request_context_json, started_at, finished_at,
                   securities_count, calendar_count, bars_count, degradation_flags_json, warnings_json, error_message
            FROM data_import_runs WHERE import_run_id IN ({placeholders})
            """,
            tuple(import_run_ids),
        )
        by_id = {row["import_run_id"]: DataImportRun(**dict(row)) for row in rows}
        return [by_id[item] for item in import_run_ids if item in by_id]

    def get_latest_run(self, *, status: str | None = None) -> DataImportRun | None:
        """读取最近一次导入运行，可按状态过滤。"""
        sql = (
            "SELECT import_run_id, source, status, request_context_json, started_at, finished_at, "
            "securities_count, calendar_count, bars_count, degradation_flags_json, warnings_json, error_message "
            "FROM data_import_runs"
        )
        params: tuple[str, ...] = ()
        if status is not None:
            sql += " WHERE status = ?"
            params = (status,)
        sql += " ORDER BY started_at DESC LIMIT 1"
        rows = self.store.query(sql, params)
        if not rows:
            return None
        return DataImportRun(**dict(rows[0]))

    def get_latest_completed_run(self) -> DataImportRun | None:
        """读取最近一次成功导入。"""
        return self.get_latest_run(status="COMPLETED")
