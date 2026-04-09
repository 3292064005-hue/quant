"""research 持久化缓存仓储。"""
from __future__ import annotations

import json
from typing import Any

from a_share_quant.core.utils import json_dumps, now_iso
from a_share_quant.storage.sqlite_store import SQLiteStore


class ResearchCacheRepository:
    """为 dataset/feature/signal research 结果提供持久化缓存。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def get(self, *, cache_namespace: str, cache_key: str) -> dict[str, Any] | None:
        rows = self.store.query(
            """
            SELECT cache_namespace, cache_key, artifact_type, request_digest, dataset_version_id, dataset_digest,
                   payload_json, hit_count, created_at, last_used_at
            FROM research_cache_entries
            WHERE cache_namespace = ? AND cache_key = ?
            LIMIT 1
            """,
            (cache_namespace, cache_key),
        )
        if not rows:
            return None
        row = dict(rows[0])
        self.store.execute(
            "UPDATE research_cache_entries SET hit_count = hit_count + 1, last_used_at = ? WHERE cache_namespace = ? AND cache_key = ?",
            (now_iso(), cache_namespace, cache_key),
        )
        row["payload"] = json.loads(row.pop("payload_json") or "{}")
        return row

    def put(
        self,
        *,
        cache_namespace: str,
        cache_key: str,
        artifact_type: str,
        request_digest: str,
        dataset_version_id: str | None,
        dataset_digest: str | None,
        payload: dict[str, Any],
    ) -> None:
        now = now_iso()
        self.store.execute(
            """
            INSERT INTO research_cache_entries
            (cache_namespace, cache_key, artifact_type, request_digest, dataset_version_id, dataset_digest, payload_json, hit_count, created_at, last_used_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_namespace, cache_key) DO UPDATE SET
                artifact_type = excluded.artifact_type,
                request_digest = excluded.request_digest,
                dataset_version_id = excluded.dataset_version_id,
                dataset_digest = excluded.dataset_digest,
                payload_json = excluded.payload_json,
                last_used_at = excluded.last_used_at
            """,
            (
                cache_namespace,
                cache_key,
                artifact_type,
                request_digest,
                dataset_version_id,
                dataset_digest,
                json_dumps(payload),
                0,
                now,
                now,
            ),
        )

    def prune(self, *, cache_namespace: str, max_entries: int) -> int:
        """按最近使用时间裁剪缓存大小。"""
        if max_entries <= 0:
            rows = self.store.query("SELECT COUNT(*) AS count FROM research_cache_entries WHERE cache_namespace = ?", (cache_namespace,))
            count = int(rows[0]["count"]) if rows else 0
            self.store.execute("DELETE FROM research_cache_entries WHERE cache_namespace = ?", (cache_namespace,))
            return count
        rows = self.store.query(
            """
            SELECT cache_key FROM research_cache_entries
            WHERE cache_namespace = ?
            ORDER BY last_used_at DESC, created_at DESC
            LIMIT -1 OFFSET ?
            """,
            (cache_namespace, max_entries),
        )
        keys = [str(row["cache_key"]) for row in rows]
        if not keys:
            return 0
        placeholders = ", ".join("?" for _ in keys)
        self.store.execute(
            f"DELETE FROM research_cache_entries WHERE cache_namespace = ? AND cache_key IN ({placeholders})",
            (cache_namespace, *keys),
        )
        return len(keys)
