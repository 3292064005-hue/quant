"""回测运行仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import json_dumps, now_iso
from a_share_quant.domain.models import BacktestRunStatus
from a_share_quant.storage.sqlite_store import SQLiteStore


class BacktestRunRepository:
    """持久化回测运行元数据。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def create_run(self, run_id: str, strategy_id: str, config_snapshot: dict) -> None:
        self.store.execute(
            """
            INSERT INTO backtest_runs
            (run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, strategy_id, BacktestRunStatus.RUNNING.value, json_dumps(config_snapshot), now_iso(), None, None, None),
        )

    def finish_run(self, run_id: str, status: BacktestRunStatus, error_message: str | None = None, report_path: str | None = None) -> None:
        self.store.execute(
            """
            UPDATE backtest_runs
            SET status = ?, finished_at = ?, error_message = COALESCE(?, error_message), report_path = COALESCE(?, report_path)
            WHERE run_id = ?
            """,
            (status.value, now_iso(), error_message, report_path, run_id),
        )
