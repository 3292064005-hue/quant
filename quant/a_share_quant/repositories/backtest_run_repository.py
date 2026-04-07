"""回测运行仓储。"""
from __future__ import annotations

from a_share_quant.core.utils import json_dumps, now_iso
from a_share_quant.domain.models import BacktestRun, BacktestRunStatus, DataLineage, RunArtifacts
from a_share_quant.storage.sqlite_store import SQLiteStore


class BacktestRunRepository:
    """持久化回测运行元数据。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def create_run(
        self,
        run_id: str,
        strategy_id: str,
        config_snapshot: dict,
        *,
        data_lineage: DataLineage | None = None,
        artifacts: RunArtifacts | None = None,
    ) -> None:
        """创建回测运行记录。

        Args:
            run_id: 运行标识。
            strategy_id: 策略标识。
            config_snapshot: 运行配置快照。
            data_lineage: 当前运行使用的数据谱系摘要。
            artifacts: 入口、策略版本、运行模式等初始产物清单。
        """
        lineage = data_lineage or DataLineage()
        manifest = artifacts or RunArtifacts()
        self.store.execute(
            """
            INSERT INTO backtest_runs
            (run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path,
             import_run_id, data_source, data_start_date, data_end_date, dataset_digest,
             degradation_flags_json, warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                strategy_id,
                BacktestRunStatus.RUNNING.value,
                json_dumps(config_snapshot),
                now_iso(),
                None,
                None,
                None,
                lineage.import_run_id,
                lineage.data_source,
                lineage.data_start_date,
                lineage.data_end_date,
                lineage.dataset_digest,
                json_dumps(lineage.degradation_flags),
                json_dumps(lineage.warnings),
                manifest.entrypoint,
                manifest.strategy_version,
                manifest.runtime_mode,
                json_dumps(manifest.report_paths),
            ),
        )

    def finish_run(
        self,
        run_id: str,
        status: BacktestRunStatus,
        error_message: str | None = None,
        report_path: str | None = None,
        report_artifacts: list[str] | None = None,
    ) -> None:
        """结束运行并补写报告产物。"""
        self.store.execute(
            """
            UPDATE backtest_runs
            SET status = ?,
                finished_at = ?,
                error_message = COALESCE(?, error_message),
                report_path = COALESCE(?, report_path),
                report_artifacts_json = COALESCE(?, report_artifacts_json)
            WHERE run_id = ?
            """,
            (
                status.value,
                now_iso(),
                error_message,
                report_path,
                json_dumps(report_artifacts) if report_artifacts is not None else None,
                run_id,
            ),
        )

    def get_run(self, run_id: str) -> BacktestRun | None:
        rows = self.store.query(
            """
            SELECT run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path,
                   import_run_id, data_source, data_start_date, data_end_date, dataset_digest,
                   degradation_flags_json, warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json
            FROM backtest_runs WHERE run_id = ?
            """,
            (run_id,),
        )
        if not rows:
            return None
        row = rows[0]
        return BacktestRun(
            run_id=row["run_id"],
            strategy_id=row["strategy_id"],
            status=BacktestRunStatus(row["status"]),
            config_snapshot_json=row["config_snapshot_json"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_message=row["error_message"],
            report_path=row["report_path"],
            import_run_id=row["import_run_id"],
            data_source=row["data_source"],
            data_start_date=row["data_start_date"],
            data_end_date=row["data_end_date"],
            dataset_digest=row["dataset_digest"],
            degradation_flags_json=row["degradation_flags_json"],
            warnings_json=row["warnings_json"],
            entrypoint=row["entrypoint"],
            strategy_version=row["strategy_version"],
            runtime_mode=row["runtime_mode"],
            report_artifacts_json=row["report_artifacts_json"],
        )

    def get_latest_run(self, status: BacktestRunStatus | None = None) -> BacktestRun | None:
        sql = (
            "SELECT run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path, "
            "import_run_id, data_source, data_start_date, data_end_date, dataset_digest, degradation_flags_json, warnings_json, "
            "entrypoint, strategy_version, runtime_mode, report_artifacts_json FROM backtest_runs"
        )
        params: tuple[str, ...] = ()
        if status is not None:
            sql += " WHERE status = ?"
            params = (status.value,)
        sql += " ORDER BY started_at DESC LIMIT 1"
        rows = self.store.query(sql, params)
        if not rows:
            return None
        row = rows[0]
        return BacktestRun(
            run_id=row["run_id"],
            strategy_id=row["strategy_id"],
            status=BacktestRunStatus(row["status"]),
            config_snapshot_json=row["config_snapshot_json"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_message=row["error_message"],
            report_path=row["report_path"],
            import_run_id=row["import_run_id"],
            data_source=row["data_source"],
            data_start_date=row["data_start_date"],
            data_end_date=row["data_end_date"],
            dataset_digest=row["dataset_digest"],
            degradation_flags_json=row["degradation_flags_json"],
            warnings_json=row["warnings_json"],
            entrypoint=row["entrypoint"],
            strategy_version=row["strategy_version"],
            runtime_mode=row["runtime_mode"],
            report_artifacts_json=row["report_artifacts_json"],
        )
