"""策略元数据仓储。"""
from __future__ import annotations

import json

from a_share_quant.core.utils import json_dumps, now_iso
from a_share_quant.storage.sqlite_store import SQLiteStore


class StrategyRepository:
    """持久化策略定义。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def save(
        self,
        strategy_id: str,
        strategy_type: str,
        params: dict,
        *,
        class_path: str = "",
        version: str = "1.0.0",
        enabled: bool = True,
    ) -> None:
        now = now_iso()
        self.store.execute(
            """
            INSERT OR REPLACE INTO strategies
            (strategy_id, strategy_type, class_path, params_json, version, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (strategy_id, strategy_type, class_path, json_dumps(params), version, int(enabled), now, now),
        )

    def get(self, strategy_id: str) -> dict | None:
        rows = self.store.query(
            "SELECT strategy_id, strategy_type, class_path, params_json, version, enabled, created_at, updated_at FROM strategies WHERE strategy_id = ?",
            (strategy_id,),
        )
        if not rows:
            return None
        row = dict(rows[0])
        row["params"] = json.loads(row.pop("params_json"))
        row["enabled"] = bool(row["enabled"])
        return row

    def list_enabled(self) -> list[dict]:
        rows = self.store.query(
            "SELECT strategy_id, strategy_type, class_path, params_json, version, enabled, created_at, updated_at FROM strategies WHERE enabled = 1 ORDER BY updated_at DESC"
        )
        results: list[dict] = []
        for row in rows:
            item = dict(row)
            item["params"] = json.loads(item.pop("params_json"))
            item["enabled"] = bool(item["enabled"])
            results.append(item)
        return results
