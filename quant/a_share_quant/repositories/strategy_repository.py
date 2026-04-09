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
        component_manifest: dict | None = None,
        capability_tags: list[str] | None = None,
        strategy_blueprint: dict | None = None,
    ) -> None:
        """保存策略定义与组件契约。

        Args:
            strategy_id: 策略标识。
            strategy_type: 策略类型名。
            params: 已解析的初始化参数。
            class_path: 解析后的策略类路径。
            version: 策略版本。
            enabled: 是否启用。
            component_manifest: 正式组件声明。
            capability_tags: 用于检索/筛选的能力标签。
            strategy_blueprint: 正式组件蓝图摘要。

        Returns:
            None。
        """
        now = now_iso()
        self.store.execute(
            """
            INSERT OR REPLACE INTO strategies
            (strategy_id, strategy_type, class_path, params_json, version, enabled, component_manifest_json, strategy_blueprint_json, capability_tags_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_id,
                strategy_type,
                class_path,
                json_dumps(params),
                version,
                int(enabled),
                json_dumps(component_manifest or {}),
                json_dumps(strategy_blueprint or {}),
                json_dumps(capability_tags or []),
                now,
                now,
            ),
        )

    def get(self, strategy_id: str) -> dict | None:
        rows = self.store.query(
            "SELECT strategy_id, strategy_type, class_path, params_json, version, enabled, component_manifest_json, strategy_blueprint_json, capability_tags_json, created_at, updated_at FROM strategies WHERE strategy_id = ?",
            (strategy_id,),
        )
        if not rows:
            return None
        return self._normalize_row(dict(rows[0]))

    def list_enabled(self) -> list[dict]:
        rows = self.store.query(
            "SELECT strategy_id, strategy_type, class_path, params_json, version, enabled, component_manifest_json, strategy_blueprint_json, capability_tags_json, created_at, updated_at FROM strategies WHERE enabled = 1 ORDER BY updated_at DESC"
        )
        return [self._normalize_row(dict(row)) for row in rows]

    @staticmethod
    def _normalize_row(row: dict) -> dict:
        row["params"] = json.loads(row.pop("params_json"))
        row["component_manifest"] = json.loads(row.pop("component_manifest_json") or "{}")
        row["strategy_blueprint"] = json.loads(row.pop("strategy_blueprint_json") or "{}")
        row["capability_tags"] = json.loads(row.pop("capability_tags_json") or "[]")
        row["enabled"] = bool(row["enabled"])
        return row
