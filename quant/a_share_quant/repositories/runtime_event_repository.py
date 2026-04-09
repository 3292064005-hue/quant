"""统一 runtime 事件流仓储。"""
from __future__ import annotations

import json
from typing import Any

from a_share_quant.core.utils import json_dumps, new_id, now_iso
from a_share_quant.core.events import Event
from a_share_quant.storage.sqlite_store import SQLiteStore


class RuntimeEventRepository:
    """持久化跨 research/backtest/operator 的统一事件流。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def append(
        self,
        *,
        source_domain: str,
        stream_scope: str,
        stream_id: str | None,
        event_type: str,
        level: str = "INFO",
        payload: dict[str, Any] | None = None,
        occurred_at: str | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        event = {
            "event_id": event_id or new_id("rtevt"),
            "source_domain": source_domain,
            "stream_scope": stream_scope,
            "stream_id": stream_id,
            "event_type": event_type,
            "level": level,
            "payload": dict(payload or {}),
            "occurred_at": occurred_at or now_iso(),
        }
        self.store.execute(
            """
            INSERT OR REPLACE INTO runtime_events
            (event_id, source_domain, stream_scope, stream_id, event_type, level, payload_json, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["source_domain"],
                event["stream_scope"],
                event["stream_id"],
                event["event_type"],
                event["level"],
                json_dumps(event["payload"]),
                event["occurred_at"],
            ),
        )
        return event

    def append_from_event(self, event: Event, *, source_domain: str, stream_scope: str, stream_id: str | None, level: str = "INFO") -> None:
        self.append(
            source_domain=source_domain,
            stream_scope=stream_scope,
            stream_id=stream_id,
            event_type=event.event_type,
            level=level,
            payload=event.payload,
            occurred_at=event.occurred_at,
            event_id=event.event_id,
        )

    def list_recent(
        self,
        *,
        source_domain: str | None = None,
        stream_scope: str | None = None,
        stream_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = "SELECT event_id, source_domain, stream_scope, stream_id, event_type, level, payload_json, occurred_at FROM runtime_events"
        clauses: list[str] = []
        params: list[Any] = []
        if source_domain is not None:
            clauses.append("source_domain = ?")
            params.append(source_domain)
        if stream_scope is not None:
            clauses.append("stream_scope = ?")
            params.append(stream_scope)
        if stream_id is not None:
            clauses.append("stream_id = ?")
            params.append(stream_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY occurred_at DESC LIMIT ?"
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        normalized: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            normalized.append(
                {
                    "event_id": row["event_id"],
                    "source_domain": row["source_domain"],
                    "stream_scope": row["stream_scope"],
                    "stream_id": row["stream_id"],
                    "event_type": row["event_type"],
                    "level": row["level"],
                    "payload": payload,
                    "occurred_at": row["occurred_at"],
                }
            )
        return normalized
