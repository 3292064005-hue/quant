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
        run_events: list[dict] | None = None,
    ) -> None:
        """创建回测运行记录。

        Args:
            run_id: 运行标识。
            strategy_id: 策略标识。
            config_snapshot: 运行时配置快照。
            data_lineage: 初始数据谱系，可为空。
            artifacts: 初始 manifest，可为空。

        Returns:
            None。

        Boundary Behavior:
            - ``run_manifest_json`` 会完整持久化当前 manifest；
            - ``report_artifacts_json`` 继续保留为历史兼容字段，仅存路径列表；
            - ``run_events_json`` 为正式数据库事件产物，避免完整事件明细仅依赖 sidecar 文件。
        """
        lineage = data_lineage or DataLineage()
        manifest = artifacts or RunArtifacts()
        self.store.execute(
            """
            INSERT INTO backtest_runs
            (run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path,
             dataset_version_id, import_run_id, import_run_ids_json, data_source, data_start_date, data_end_date, dataset_digest,
             degradation_flags_json, warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json, run_manifest_json, run_events_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                lineage.dataset_version_id,
                lineage.import_run_id,
                json_dumps(lineage.import_run_ids),
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
                json_dumps(manifest),
                json_dumps(run_events or []),
            ),
        )

    def update_lineage(self, run_id: str, lineage: DataLineage) -> None:
        """在 run 已创建后补写最终数据谱系。"""
        self.store.execute(
            """
            UPDATE backtest_runs
            SET dataset_version_id = ?,
                import_run_id = ?,
                import_run_ids_json = ?,
                data_source = ?,
                data_start_date = ?,
                data_end_date = ?,
                dataset_digest = ?,
                degradation_flags_json = ?,
                warnings_json = ?
            WHERE run_id = ?
            """,
            (
                lineage.dataset_version_id,
                lineage.import_run_id,
                json_dumps(lineage.import_run_ids),
                lineage.data_source,
                lineage.data_start_date,
                lineage.data_end_date,
                lineage.dataset_digest,
                json_dumps(lineage.degradation_flags),
                json_dumps(lineage.warnings),
                run_id,
            ),
        )

    def update_manifest(self, run_id: str, artifacts: RunArtifacts) -> None:
        """补写或更新 run manifest。"""
        self.store.execute(
            """
            UPDATE backtest_runs
            SET entrypoint = ?,
                strategy_version = ?,
                runtime_mode = ?,
                report_artifacts_json = ?,
                run_manifest_json = ?
            WHERE run_id = ?
            """,
            (
                artifacts.entrypoint,
                artifacts.strategy_version,
                artifacts.runtime_mode,
                json_dumps(artifacts.report_paths),
                json_dumps(artifacts),
                run_id,
            ),
        )

    def finish_run(
        self,
        run_id: str,
        status: BacktestRunStatus,
        error_message: str | None = None,
        report_path: str | None = None,
        report_artifacts: list[str] | None = None,
        run_manifest: RunArtifacts | None = None,
        run_events: list[dict] | None = None,
        *,
        set_finished_at: bool = True,
        overwrite_error_message: bool = False,
    ) -> None:
        """更新运行阶段并按需结束运行。

        Args:
            run_id: 运行标识。
            status: 新状态。
            error_message: 可选错误信息。
            report_path: 主报告路径。
            report_artifacts: 报告产物列表。
            run_manifest: 最新运行 manifest。
            run_events: 完整运行事件。
            set_finished_at: 为 ``True`` 时写入完成时间；用于最终阶段收口。
            overwrite_error_message: 为 ``True`` 时允许显式覆盖/清空历史 error_message。

        Returns:
            None。

        Boundary Behavior:
            - ``ENGINE_COMPLETED`` 阶段必须以 ``set_finished_at=False`` 更新，避免把业务完成误写成最终结束；
            - 终态可显式覆盖错误消息，以支持 ``ARTIFACT_EXPORT_FAILED -> COMPLETED`` 的恢复链。
        """
        error_expression = "?" if overwrite_error_message else "COALESCE(?, error_message)"
        self.store.execute(
            f"""
            UPDATE backtest_runs
            SET status = ?,
                finished_at = CASE WHEN ? THEN ? ELSE finished_at END,
                error_message = {error_expression},
                report_path = COALESCE(?, report_path),
                report_artifacts_json = COALESCE(?, report_artifacts_json),
                run_manifest_json = COALESCE(?, run_manifest_json),
                run_events_json = COALESCE(?, run_events_json)
            WHERE run_id = ?
            """,
            (
                status.value,
                int(set_finished_at),
                now_iso() if set_finished_at else None,
                error_message,
                report_path,
                json_dumps(report_artifacts) if report_artifacts is not None else None,
                json_dumps(run_manifest) if run_manifest is not None else None,
                json_dumps(run_events) if run_events is not None else None,
                run_id,
            ),
        )

    def get_run(self, run_id: str) -> BacktestRun | None:
        """读取指定回测运行。"""
        rows = self.store.query(
            """
            SELECT run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path,
                   dataset_version_id, import_run_id, import_run_ids_json, data_source, data_start_date, data_end_date, dataset_digest,
                   degradation_flags_json, warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json, run_manifest_json, run_events_json
            FROM backtest_runs WHERE run_id = ?
            """,
            (run_id,),
        )
        if not rows:
            return None
        return self._row_to_run(rows[0])

    def get_latest_run(self, status: BacktestRunStatus | None = None) -> BacktestRun | None:
        """读取最近一次回测运行，可按状态过滤。"""
        statuses = [status] if status is not None else None
        return self.get_latest_run_by_statuses(statuses)

    def get_latest_run_by_statuses(self, statuses: list[BacktestRunStatus] | None = None) -> BacktestRun | None:
        """读取最近一次回测运行，可按多个状态过滤。"""
        sql = (
            "SELECT run_id, strategy_id, status, config_snapshot_json, started_at, finished_at, error_message, report_path, "
            "dataset_version_id, import_run_id, import_run_ids_json, data_source, data_start_date, data_end_date, dataset_digest, "
            "degradation_flags_json, warnings_json, entrypoint, strategy_version, runtime_mode, report_artifacts_json, run_manifest_json, run_events_json "
            "FROM backtest_runs"
        )
        params: tuple[str, ...] = ()
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            sql += f" WHERE status IN ({placeholders})"
            params = tuple(item.value for item in statuses)
        sql += " ORDER BY started_at DESC LIMIT 1"
        rows = self.store.query(sql, params)
        if not rows:
            return None
        return self._row_to_run(rows[0])

    @staticmethod
    def _row_to_run(row) -> BacktestRun:
        return BacktestRun(
            run_id=row["run_id"],
            strategy_id=row["strategy_id"],
            status=BacktestRunStatus(row["status"]),
            config_snapshot_json=row["config_snapshot_json"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_message=row["error_message"],
            report_path=row["report_path"],
            dataset_version_id=row["dataset_version_id"],
            import_run_id=row["import_run_id"],
            import_run_ids_json=row["import_run_ids_json"],
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
            run_manifest_json=row["run_manifest_json"],
            run_events_json=row["run_events_json"],
        )
