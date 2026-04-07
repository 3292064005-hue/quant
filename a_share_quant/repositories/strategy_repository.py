"""策略元数据仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import json_dumps, now_iso
from a_share_quant.storage.sqlite_store import SQLiteStore


class StrategyRepository:
    """持久化策略定义。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def save(self, strategy_id: str, strategy_type: str, params: dict, version: str = "1.0.0", enabled: bool = True) -> None:
        now = now_iso()
        self.store.execute(
            """
            INSERT OR REPLACE INTO strategies
            (strategy_id, strategy_type, params_json, version, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (strategy_id, strategy_type, json_dumps(params), version, int(enabled), now, now),
        )
