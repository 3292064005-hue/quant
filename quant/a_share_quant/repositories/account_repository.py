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

    def save_account_snapshot(self, run_id: str, trade_date: date, snapshot: AccountSnapshot) -> None:
        """写入账户快照。"""
        self.store.execute(
            """
            INSERT OR REPLACE INTO account_snapshots
            (snapshot_id, run_id, trade_date, cash, available_cash, market_value, total_assets, pnl, cum_pnl, daily_pnl, drawdown, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("acct"),
                run_id,
                trade_date.isoformat(),
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

    def save_position_snapshots(self, run_id: str, trade_date: date, positions: list[PositionSnapshot]) -> None:
        """批量写入持仓快照。"""
        rows = [
            (new_id("pos"), run_id, trade_date.isoformat(), pos.ts_code, pos.quantity, pos.available_quantity, pos.avg_cost, pos.market_value, pos.unrealized_pnl, now_iso())
            for pos in positions
        ]
        if rows:
            self.store.executemany(
                """
                INSERT OR REPLACE INTO position_snapshots
                (snapshot_id, run_id, trade_date, ts_code, quantity, available_quantity, avg_cost, market_value, unrealized_pnl, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
