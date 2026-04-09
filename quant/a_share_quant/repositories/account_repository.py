"""账户与持仓仓储。"""
from __future__ import annotations

from datetime import date

from a_share_quant.core.utils import new_id, now_iso
from a_share_quant.domain.models import AccountSnapshot, PositionSnapshot
from a_share_quant.storage.sqlite_store import SQLiteStore


class AccountRepository:
    """持久化账户与持仓快照。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def save_account_snapshot(
        self,
        run_id: str,
        trade_date: date,
        snapshot: AccountSnapshot,
        *,
        account_id: str | None = None,
    ) -> None:
        """写入账户快照。"""
        self.store.execute(
            """
            INSERT OR REPLACE INTO account_snapshots
            (snapshot_id, run_id, trade_date, account_id, cash, available_cash, market_value, total_assets, pnl, cum_pnl, daily_pnl, drawdown, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("acct"),
                run_id,
                trade_date.isoformat(),
                account_id,
                snapshot.cash,
                snapshot.available_cash,
                snapshot.market_value,
                snapshot.total_assets,
                snapshot.pnl,
                snapshot.cum_pnl if snapshot.cum_pnl is not None else snapshot.pnl,
                snapshot.daily_pnl if snapshot.daily_pnl is not None else 0.0,
                snapshot.drawdown,
                now_iso(),
            ),
        )

    def save_position_snapshots(
        self,
        run_id: str,
        trade_date: date,
        positions: list[PositionSnapshot],
        *,
        account_id: str | None = None,
    ) -> None:
        """批量写入持仓快照。"""
        rows = [
            (new_id("pos"), run_id, trade_date.isoformat(), account_id, pos.ts_code, pos.quantity, pos.available_quantity, pos.avg_cost, pos.market_value, pos.unrealized_pnl, now_iso())
            for pos in positions
        ]
        if rows:
            self.store.executemany(
                """
                INSERT OR REPLACE INTO position_snapshots
                (snapshot_id, run_id, trade_date, account_id, ts_code, quantity, available_quantity, avg_cost, market_value, unrealized_pnl, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def load_equity_curve(self, run_id: str) -> tuple[list[date], list[float]]:
        """读取指定运行的 EOD 净值曲线。"""
        rows = self.store.query(
            "SELECT trade_date, total_assets FROM account_snapshots WHERE run_id = ? ORDER BY trade_date",
            (run_id,),
        )
        return ([date.fromisoformat(row["trade_date"]) for row in rows], [float(row["total_assets"]) for row in rows])


    def load_latest_account_snapshots(self, *, account_id: str | None = None, limit: int = 20) -> list[dict[str, object]]:
        """读取最近账户快照，用于 operator/product 读平面。"""
        sql = (
            "SELECT snapshot_id, run_id, trade_date, account_id, cash, available_cash, market_value, total_assets, pnl, cum_pnl, daily_pnl, drawdown, created_at "
            "FROM account_snapshots"
        )
        params: list[object] = []
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params.append(account_id)
        sql += " ORDER BY trade_date DESC, created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(row) for row in self.store.query(sql, tuple(params))]

    def load_latest_position_snapshots(self, *, account_id: str | None = None, limit: int = 50) -> list[dict[str, object]]:
        """读取最近持仓快照，用于 operator/product 读平面。"""
        sql = (
            "SELECT snapshot_id, run_id, trade_date, account_id, ts_code, quantity, available_quantity, avg_cost, market_value, unrealized_pnl, created_at "
            "FROM position_snapshots"
        )
        params: list[object] = []
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params.append(account_id)
        sql += " ORDER BY trade_date DESC, created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(row) for row in self.store.query(sql, tuple(params))]


    def save_operator_account_snapshot(
        self,
        session_id: str,
        trade_date: date,
        snapshot: AccountSnapshot,
        *,
        account_id: str | None = None,
        source: str,
        capture_id: str | None = None,
        captured_at: str | None = None,
    ) -> str:
        """写入 operator 账户快照。"""
        resolved_capture_id = capture_id or new_id("ocap")
        created_at = captured_at or now_iso()
        self.store.execute(
            """
            INSERT OR REPLACE INTO operator_account_snapshots
            (snapshot_id, capture_id, session_id, trade_date, account_id, source, cash, available_cash, market_value, total_assets, pnl, cum_pnl, daily_pnl, drawdown, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("oacct"),
                resolved_capture_id,
                session_id,
                trade_date.isoformat(),
                account_id,
                source,
                snapshot.cash,
                snapshot.available_cash,
                snapshot.market_value,
                snapshot.total_assets,
                snapshot.pnl,
                snapshot.cum_pnl if snapshot.cum_pnl is not None else snapshot.pnl,
                snapshot.daily_pnl if snapshot.daily_pnl is not None else 0.0,
                snapshot.drawdown,
                created_at,
            ),
        )
        return resolved_capture_id

    def save_operator_position_snapshots(
        self,
        session_id: str,
        trade_date: date,
        positions: list[PositionSnapshot],
        *,
        account_id: str | None = None,
        source: str,
        capture_id: str,
        captured_at: str | None = None,
    ) -> None:
        """批量写入 operator 持仓快照。"""
        created_at = captured_at or now_iso()
        rows = [
            (
                new_id("opos"),
                capture_id,
                session_id,
                trade_date.isoformat(),
                account_id,
                source,
                pos.ts_code,
                pos.quantity,
                pos.available_quantity,
                pos.avg_cost,
                pos.market_value,
                pos.unrealized_pnl,
                created_at,
            )
            for pos in positions
        ]
        if rows:
            self.store.executemany(
                """
                INSERT OR REPLACE INTO operator_position_snapshots
                (snapshot_id, capture_id, session_id, trade_date, account_id, source, ts_code, quantity, available_quantity, avg_cost, market_value, unrealized_pnl, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def load_latest_operator_account_snapshot(self, *, account_id: str | None = None) -> dict[str, object] | None:
        """读取最近一条 operator 账户快照。"""
        sql = (
            "SELECT snapshot_id, capture_id, session_id, trade_date, account_id, source, cash, available_cash, market_value, total_assets, pnl, cum_pnl, daily_pnl, drawdown, created_at "
            "FROM operator_account_snapshots"
        )
        params: list[object] = []
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params.append(account_id)
        sql += " ORDER BY created_at DESC LIMIT 1"
        rows = self.store.query(sql, tuple(params))
        return dict(rows[0]) if rows else None

    def load_latest_operator_position_snapshots(self, *, account_id: str | None = None, capture_id: str | None = None, limit: int = 100) -> list[dict[str, object]]:
        """读取最近一次 operator 持仓快照。"""
        resolved_capture_id = capture_id
        if resolved_capture_id is None:
            latest = self.load_latest_operator_account_snapshot(account_id=account_id)
            if latest is None:
                return []
            resolved_capture_id = str(latest["capture_id"])
        sql = (
            "SELECT snapshot_id, capture_id, session_id, trade_date, account_id, source, ts_code, quantity, available_quantity, avg_cost, market_value, unrealized_pnl, created_at "
            "FROM operator_position_snapshots WHERE capture_id = ?"
        )
        params: list[object] = [resolved_capture_id]
        if account_id is not None:
            sql += " AND account_id = ?"
            params.append(account_id)
        sql += " ORDER BY ts_code ASC LIMIT ?"
        params.append(limit)
        return [dict(row) for row in self.store.query(sql, tuple(params))]
