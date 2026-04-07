"""审计日志仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import json_dumps, new_id, now_iso
from a_share_quant.storage.sqlite_store import SQLiteStore


class AuditRepository:
    """持久化审计日志。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def write(self, run_id: str, trace_id: str, module: str, action: str, entity_type: str, entity_id: str, payload: dict, level: str = "INFO", operator: str = "system") -> None:
        """写入单条审计日志。"""
        self.store.execute(
            """
            INSERT INTO audit_logs
            (log_id, run_id, trace_id, module, action, entity_type, entity_id, payload_json, level, operator, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (new_id("audit"), run_id, trace_id, module, action, entity_type, entity_id, json_dumps(payload), level, operator, now_iso()),
        )
