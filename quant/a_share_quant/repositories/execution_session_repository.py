"""operator 命令会话与事件仓储。"""
from __future__ import annotations

import json
from typing import Any

from a_share_quant.core.utils import json_dumps, new_id, now_iso
from a_share_quant.domain.models import TradeCommandEvent, TradeSessionStatus, TradeSessionSummary
from a_share_quant.storage.sqlite_store import SQLiteStore
from a_share_quant.repositories.runtime_event_repository import RuntimeEventRepository


class ExecutionSessionRepository:
    """持久化 operator 交易会话与事件日志。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.runtime_event_repository = RuntimeEventRepository(store)

    def create_session(
        self,
        *,
        runtime_mode: str,
        broker_provider: str,
        command_type: str,
        command_source: str,
        requested_by: str,
        requested_trade_date: str | None,
        idempotency_key: str | None,
        risk_summary: dict[str, Any] | None = None,
        order_count: int = 0,
        status: TradeSessionStatus = TradeSessionStatus.CREATED,
        account_id: str | None = None,
        broker_event_cursor: str | None = None,
        supervisor_mode: str | None = None,
    ) -> TradeSessionSummary:
        now = now_iso()
        session_id = new_id("session")
        summary = TradeSessionSummary(
            session_id=session_id,
            runtime_mode=runtime_mode,
            broker_provider=broker_provider,
            command_type=command_type,
            command_source=command_source,
            requested_by=requested_by,
            status=status,
            idempotency_key=idempotency_key,
            requested_trade_date=requested_trade_date,
            risk_summary=dict(risk_summary or {}),
            order_count=order_count,
            submitted_count=0,
            rejected_count=0,
            account_id=account_id,
            broker_event_cursor=broker_event_cursor,
            last_synced_at=None,
            supervisor_owner=None,
            supervisor_lease_expires_at=None,
            supervisor_mode=supervisor_mode,
            last_supervised_at=None,
            created_at=now,
            updated_at=now,
        )
        self.store.execute(
            """
            INSERT INTO trade_sessions
            (
                session_id, runtime_mode, broker_provider, command_type, command_source, requested_by,
                status, idempotency_key, requested_trade_date, risk_summary_json, order_count,
                submitted_count, rejected_count, error_message, account_id, broker_event_cursor,
                last_synced_at, supervisor_owner, supervisor_lease_expires_at, supervisor_mode,
                last_supervised_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary.session_id,
                summary.runtime_mode,
                summary.broker_provider,
                summary.command_type,
                summary.command_source,
                summary.requested_by,
                summary.status.value,
                summary.idempotency_key,
                summary.requested_trade_date,
                json_dumps(summary.risk_summary),
                summary.order_count,
                summary.submitted_count,
                summary.rejected_count,
                summary.error_message,
                summary.account_id,
                summary.broker_event_cursor,
                summary.last_synced_at,
                summary.supervisor_owner,
                summary.supervisor_lease_expires_at,
                summary.supervisor_mode,
                summary.last_supervised_at,
                summary.created_at,
                summary.updated_at,
            ),
        )
        return summary

    def update_session(
        self,
        session_id: str,
        *,
        status: TradeSessionStatus,
        submitted_count: int,
        rejected_count: int,
        risk_summary: dict[str, Any] | None = None,
        error_message: str | None = None,
        broker_event_cursor: str | None = None,
        last_synced_at: str | None = None,
        supervisor_mode: str | None = None,
        last_supervised_at: str | None = None,
    ) -> None:
        self.store.execute(
            """
            UPDATE trade_sessions
            SET status = ?, submitted_count = ?, rejected_count = ?, risk_summary_json = ?, error_message = ?,
                broker_event_cursor = COALESCE(?, broker_event_cursor),
                last_synced_at = COALESCE(?, last_synced_at),
                supervisor_mode = COALESCE(?, supervisor_mode),
                last_supervised_at = COALESCE(?, last_supervised_at),
                updated_at = ?
            WHERE session_id = ?
            """,
            (
                status.value,
                submitted_count,
                rejected_count,
                json_dumps(risk_summary or {}),
                error_message,
                broker_event_cursor,
                last_synced_at,
                supervisor_mode,
                last_supervised_at,
                now_iso(),
                session_id,
            ),
        )

    def mark_synced(self, session_id: str, *, broker_event_cursor: str | None = None) -> None:
        self.store.execute(
            """
            UPDATE trade_sessions
            SET broker_event_cursor = COALESCE(?, broker_event_cursor), last_synced_at = ?, updated_at = ?
            WHERE session_id = ?
            """,
            (broker_event_cursor, now_iso(), now_iso(), session_id),
        )


    def claim_sessions_for_supervisor(
        self,
        owner_id: str,
        *,
        statuses: list[TradeSessionStatus],
        lease_expires_at: str,
        account_id: str | None = None,
        session_id: str | None = None,
        limit: int = 50,
        now: str | None = None,
        supervisor_mode: str | None = None,
    ) -> list[TradeSessionSummary]:
        """为 supervisor 领取一批待处理会话。

        Boundary Behavior:
            - 仅领取租约为空、租约已过期，或已由当前 owner 持有的会话；
            - claim 采用 compare-and-swap 语义，以 ``UPDATE ... WHERE eligibility`` 的影响行数作为唯一成功依据；
            - 若指定 ``session_id``，则只尝试领取该会话。
        """
        if not statuses:
            return []
        current_time = now or now_iso()
        placeholders = ", ".join("?" for _ in statuses)
        clauses = [f"status IN ({placeholders})", "(supervisor_lease_expires_at IS NULL OR supervisor_lease_expires_at <= ? OR supervisor_owner = ?)"]
        params: list[Any] = [status.value for status in statuses]
        params.extend([current_time, owner_id])
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if session_id is not None:
            clauses.append("session_id = ?")
            params.append(session_id)
        sql = "SELECT session_id FROM trade_sessions WHERE " + " AND ".join(clauses) + " ORDER BY created_at ASC LIMIT ?"
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        claimed: list[TradeSessionSummary] = []
        status_placeholders = ", ".join("?" for _ in statuses)
        for row in rows:
            target_session_id = str(row["session_id"])
            where_account = " AND account_id = ?" if account_id is not None else ""
            rowcount = self.store.execute_rowcount(
                f"""
                UPDATE trade_sessions
                SET supervisor_owner = ?, supervisor_lease_expires_at = ?,
                    supervisor_mode = COALESCE(?, supervisor_mode),
                    last_supervised_at = ?, updated_at = ?
                WHERE session_id = ?
                  AND status IN ({status_placeholders})
                  AND (supervisor_lease_expires_at IS NULL OR supervisor_lease_expires_at <= ? OR supervisor_owner = ?)
                  {where_account}
                """,
                (
                    owner_id,
                    lease_expires_at,
                    supervisor_mode,
                    current_time,
                    current_time,
                    target_session_id,
                    *[status.value for status in statuses],
                    current_time,
                    owner_id,
                    *(([account_id] if account_id is not None else [])),
                ),
            )
            if rowcount <= 0:
                continue
            summary = self.get(target_session_id)
            if summary is not None and summary.supervisor_owner == owner_id:
                claimed.append(summary)
        return claimed

    def renew_supervisor_claim(
        self,
        session_id: str,
        *,
        owner_id: str,
        lease_expires_at: str,
        now: str | None = None,
        supervisor_mode: str | None = None,
    ) -> bool:
        """续租 supervisor 会话租约。

        Returns:
            ``True`` 表示当前 owner 仍持有该会话并成功续租；
            ``False`` 表示 ownership 已丢失、租约已过期或会话不存在。

        Boundary Behavior:
            - 续租以 ``UPDATE`` 的影响行数作为唯一成功依据；
            - 不进行单独的预读检查，避免在竞争窗口内把已经失去 ownership 的会话误判为续租成功。
        """
        current_time = now or now_iso()
        rowcount = self.store.execute_rowcount(
            """
            UPDATE trade_sessions
            SET supervisor_lease_expires_at = ?,
                supervisor_mode = COALESCE(?, supervisor_mode),
                last_supervised_at = ?,
                updated_at = ?
            WHERE session_id = ?
              AND supervisor_owner = ?
            """,
            (lease_expires_at, supervisor_mode, current_time, current_time, session_id, owner_id),
        )
        return rowcount > 0


    def release_supervisor_claim(self, session_id: str, *, owner_id: str | None = None) -> bool:
        """释放 supervisor 租约。

        Returns:
            ``True`` 表示当前 owner 的 claim 已实际释放；``False`` 表示本次释放为 no-op。
        """
        clauses = ["session_id = ?"]
        params: list[Any] = [session_id]
        if owner_id is not None:
            clauses.append("supervisor_owner = ?")
            params.append(owner_id)
        rowcount = self.store.execute_rowcount(
            f"UPDATE trade_sessions SET supervisor_owner = NULL, supervisor_lease_expires_at = NULL, updated_at = ? WHERE {' AND '.join(clauses)}",
            (now_iso(), *params),
        )
        return rowcount > 0

    def append_event(self, session_id: str, *, event_type: str, level: str, payload: dict[str, Any] | None = None) -> TradeCommandEvent:
        event = TradeCommandEvent(
            event_id=new_id("event"),
            session_id=session_id,
            event_type=event_type,
            level=level,
            payload=dict(payload or {}),
            created_at=now_iso(),
        )
        self.store.execute(
            """
            INSERT INTO trade_command_events (event_id, session_id, event_type, level, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event.event_id, event.session_id, event.event_type, event.level, json_dumps(event.payload), event.created_at),
        )
        self.runtime_event_repository.append(
            source_domain="operator",
            stream_scope="trade_session",
            stream_id=session_id,
            event_type=event.event_type,
            level=event.level,
            payload=event.payload,
            occurred_at=event.created_at,
            event_id=event.event_id,
        )
        return event

    def get_by_idempotency_key(self, idempotency_key: str) -> TradeSessionSummary | None:
        rows = self.store.query(
            "SELECT * FROM trade_sessions WHERE idempotency_key = ? ORDER BY created_at DESC LIMIT 1",
            (idempotency_key,),
        )
        if not rows:
            return None
        return self._row_to_summary(dict(rows[0]))

    def get(self, session_id: str) -> TradeSessionSummary | None:
        rows = self.store.query("SELECT * FROM trade_sessions WHERE session_id = ?", (session_id,))
        if not rows:
            return None
        return self._row_to_summary(dict(rows[0]))

    def get_latest(self) -> TradeSessionSummary | None:
        rows = self.store.query("SELECT * FROM trade_sessions ORDER BY created_at DESC LIMIT 1")
        if not rows:
            return None
        return self._row_to_summary(dict(rows[0]))

    def list_sessions(
        self,
        *,
        statuses: list[TradeSessionStatus] | None = None,
        account_id: str | None = None,
        supervisor_owner: str | None = None,
        limit: int = 50,
    ) -> list[TradeSessionSummary]:
        sql = "SELECT * FROM trade_sessions"
        params: list[Any] = []
        clauses: list[str] = []
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(status.value for status in statuses)
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if supervisor_owner is not None:
            clauses.append("supervisor_owner = ?")
            params.append(supervisor_owner)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        return [self._row_to_summary(dict(row)) for row in rows]

    def list_events(self, session_id: str, *, limit: int = 100) -> list[TradeCommandEvent]:
        rows = self.store.query(
            "SELECT event_id, session_id, event_type, level, payload_json, created_at FROM trade_command_events WHERE session_id = ? ORDER BY created_at ASC LIMIT ?",
            (session_id, limit),
        )
        events: list[TradeCommandEvent] = []
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            events.append(
                TradeCommandEvent(
                    event_id=row["event_id"],
                    session_id=row["session_id"],
                    event_type=row["event_type"],
                    level=row["level"],
                    payload=payload,
                    created_at=row["created_at"],
                )
            )
        return events

    @staticmethod
    def _row_to_summary(row: dict[str, Any]) -> TradeSessionSummary:
        return TradeSessionSummary(
            session_id=row["session_id"],
            runtime_mode=row["runtime_mode"],
            broker_provider=row["broker_provider"],
            command_type=row["command_type"],
            command_source=row["command_source"],
            requested_by=row["requested_by"],
            status=TradeSessionStatus(row["status"]),
            idempotency_key=row.get("idempotency_key"),
            requested_trade_date=row.get("requested_trade_date"),
            risk_summary=json.loads(row.get("risk_summary_json") or "{}"),
            order_count=int(row.get("order_count") or 0),
            submitted_count=int(row.get("submitted_count") or 0),
            rejected_count=int(row.get("rejected_count") or 0),
            error_message=row.get("error_message"),
            account_id=row.get("account_id"),
            broker_event_cursor=row.get("broker_event_cursor"),
            last_synced_at=row.get("last_synced_at"),
            supervisor_owner=row.get("supervisor_owner"),
            supervisor_lease_expires_at=row.get("supervisor_lease_expires_at"),
            supervisor_mode=row.get("supervisor_mode"),
            last_supervised_at=row.get("last_supervised_at"),
            created_at=row.get("created_at") or "",
            updated_at=row.get("updated_at") or "",
        )
