"""研究工作流运行记录仓储。"""
from __future__ import annotations

import json
from typing import Any

from a_share_quant.contracts.versioned_contracts import parse_signal_snapshot_payload
from a_share_quant.core.utils import json_dumps, new_id, now_iso
from a_share_quant.storage.sqlite_store import SQLiteStore


class ResearchRunRepository:
    """持久化 research workflow 产物摘要与会话谱系。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def create_run(
        self,
        *,
        workflow_name: str,
        artifact_type: str,
        dataset_version_id: str | None,
        dataset_digest: str | None,
        request: dict,
        result: dict,
        research_session_id: str | None = None,
        parent_research_run_id: str | None = None,
        root_research_run_id: str | None = None,
        step_name: str | None = None,
        is_primary_run: bool = True,
    ) -> str:
        """保存研究运行记录并返回 research_run_id。

        Args:
            workflow_name: 工作流名称。
            artifact_type: 正式产物类型。
            dataset_version_id: 数据版本标识。
            dataset_digest: 数据摘要指纹。
            request: 请求载荷。
            result: 结果载荷。
            research_session_id: 同一 research 命令会话标识；用于把 experiment 与其内部步骤关联起来。
            parent_research_run_id: 父 research run；通常用于 experiment -> dataset/feature/signal 子步骤。
            root_research_run_id: 根 research run；便于后续 replay/谱系查询以主记录为入口回溯整棵树。
            step_name: 在 session 内的步骤名，例如 ``dataset_summary`` / ``signal_snapshot``。
            is_primary_run: 是否为用户显式触发、应出现在 recent-runs/UI 主视图中的主记录。

        Returns:
            新生成的 research_run_id。
        """
        research_run_id = new_id("research")
        resolved_root_research_run_id = root_research_run_id or research_run_id
        self.store.execute(
            """
            INSERT INTO research_runs
            (
                research_run_id,
                workflow_name,
                artifact_type,
                dataset_version_id,
                dataset_digest,
                request_json,
                result_json,
                research_session_id,
                parent_research_run_id,
                root_research_run_id,
                step_name,
                is_primary_run,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                research_run_id,
                workflow_name,
                artifact_type,
                dataset_version_id,
                dataset_digest,
                json_dumps(request),
                json_dumps(result),
                research_session_id,
                parent_research_run_id,
                resolved_root_research_run_id,
                step_name,
                1 if is_primary_run else 0,
                now_iso(),
            ),
        )
        if parent_research_run_id:
            self.upsert_edge(parent_research_run_id, research_run_id, edge_kind="contains_step")
        if root_research_run_id and root_research_run_id != research_run_id:
            self.upsert_edge(root_research_run_id, research_run_id, edge_kind="session_member")
        self._sync_dataset_related_edges(
            research_run_id,
            dataset_version_id=dataset_version_id,
            dataset_digest=dataset_digest,
            primary_only=is_primary_run,
        )
        return research_run_id

    def update_lineage(
        self,
        research_run_id: str,
        *,
        research_session_id: str | None,
        parent_research_run_id: str | None,
        root_research_run_id: str | None,
        step_name: str | None,
        is_primary_run: bool,
    ) -> None:
        """更新 research run 的谱系元信息。"""
        self.store.execute(
            """
            UPDATE research_runs
            SET research_session_id = ?,
                parent_research_run_id = ?,
                root_research_run_id = ?,
                step_name = ?,
                is_primary_run = ?
            WHERE research_run_id = ?
            """,
            (
                research_session_id,
                parent_research_run_id,
                root_research_run_id or research_run_id,
                step_name,
                1 if is_primary_run else 0,
                research_run_id,
            ),
        )

    def get(self, research_run_id: str) -> dict | None:
        rows = self.store.query(
            """
            SELECT research_run_id, workflow_name, artifact_type, dataset_version_id, dataset_digest,
                   request_json, result_json, research_session_id, parent_research_run_id,
                   root_research_run_id, step_name, is_primary_run, created_at
            FROM research_runs
            WHERE research_run_id = ?
            """,
            (research_run_id,),
        )
        if not rows:
            return None
        return self._normalize(dict(rows[0]))

    def get_latest(self, *, artifact_type: str | None = None, primary_only: bool = True) -> dict | None:
        """读取最近一条研究运行，可按 artifact_type 与主记录过滤。"""
        sql = (
            "SELECT research_run_id, workflow_name, artifact_type, dataset_version_id, dataset_digest, "
            "request_json, result_json, research_session_id, parent_research_run_id, root_research_run_id, "
            "step_name, is_primary_run, created_at FROM research_runs"
        )
        clauses: list[str] = []
        params: list[Any] = []
        if artifact_type is not None:
            clauses.append("artifact_type = ?")
            params.append(artifact_type)
        if primary_only:
            clauses.append("is_primary_run = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT 1"
        rows = self.store.query(sql, tuple(params))
        if not rows:
            return None
        return self._normalize(dict(rows[0]))

    def list_recent(self, limit: int = 20, *, primary_only: bool = True) -> list[dict]:
        """列出最近 research runs。

        默认仅返回主记录，避免把 experiment 的内部 dataset/feature/signal 子步骤误当成用户级 recent-runs。
        """
        sql = (
            "SELECT research_run_id, workflow_name, artifact_type, dataset_version_id, dataset_digest, "
            "request_json, result_json, research_session_id, parent_research_run_id, root_research_run_id, "
            "step_name, is_primary_run, created_at FROM research_runs"
        )
        params: list[Any] = []
        if primary_only:
            sql += " WHERE is_primary_run = 1"
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        return [self._normalize(dict(row)) for row in rows]

    def list_children(self, parent_research_run_id: str) -> list[dict]:
        """列出指定主 research run 的内部子步骤。"""
        rows = self.store.query(
            """
            SELECT research_run_id, workflow_name, artifact_type, dataset_version_id, dataset_digest,
                   request_json, result_json, research_session_id, parent_research_run_id,
                   root_research_run_id, step_name, is_primary_run, created_at
            FROM research_runs
            WHERE parent_research_run_id = ?
            ORDER BY created_at ASC
            """,
            (parent_research_run_id,),
        )
        return [self._normalize(dict(row)) for row in rows]

    def list_related_by_dataset(
        self,
        *,
        dataset_version_id: str | None,
        dataset_digest: str | None,
        exclude_research_run_ids: list[str] | None = None,
        limit: int = 100,
        primary_only: bool = True,
    ) -> list[dict]:
        """按 dataset 维度查询相关 research runs，而不是只看 recent window。"""
        clauses: list[str] = []
        params: list[Any] = []
        if dataset_version_id:
            clauses.append("dataset_version_id = ?")
            params.append(dataset_version_id)
        if dataset_digest:
            clauses.append("dataset_digest = ?")
            params.append(dataset_digest)
        if not clauses:
            return []
        dataset_clause = "(" + " OR ".join(clauses) + ")"
        clauses = [dataset_clause]
        if primary_only:
            clauses.append("is_primary_run = 1")
        excluded = [item for item in (exclude_research_run_ids or []) if item]
        if excluded:
            placeholders = ", ".join("?" for _ in excluded)
            clauses.append(f"research_run_id NOT IN ({placeholders})")
            params.extend(excluded)
        sql = (
            "SELECT research_run_id, workflow_name, artifact_type, dataset_version_id, dataset_digest, "
            "request_json, result_json, research_session_id, parent_research_run_id, root_research_run_id, "
            "step_name, is_primary_run, created_at FROM research_runs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC LIMIT ?"
        )
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        return [self._normalize(dict(row)) for row in rows]


    def upsert_edge(
        self,
        src_research_run_id: str,
        dst_research_run_id: str,
        *,
        edge_kind: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if not src_research_run_id or not dst_research_run_id or src_research_run_id == dst_research_run_id:
            return
        self.store.execute(
            """
            INSERT OR IGNORE INTO research_run_edges
            (edge_id, src_research_run_id, dst_research_run_id, edge_kind, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (new_id("edge"), src_research_run_id, dst_research_run_id, edge_kind, json_dumps(payload or {}), now_iso()),
        )

    def list_related_via_edges(
        self,
        research_run_id: str,
        *,
        edge_kinds: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict]:
        clauses = ["e.src_research_run_id = ?"]
        params: list[Any] = [research_run_id]
        if edge_kinds:
            placeholders = ", ".join("?" for _ in edge_kinds)
            clauses.append(f"e.edge_kind IN ({placeholders})")
            params.extend(edge_kinds)
        sql = (
            "SELECT r.research_run_id, r.workflow_name, r.artifact_type, r.dataset_version_id, r.dataset_digest, "
            "r.request_json, r.result_json, r.research_session_id, r.parent_research_run_id, r.root_research_run_id, "
            "r.step_name, r.is_primary_run, r.created_at "
            "FROM research_run_edges e JOIN research_runs r ON r.research_run_id = e.dst_research_run_id WHERE "
            + " AND ".join(clauses)
            + " ORDER BY r.created_at DESC LIMIT ?"
        )
        params.append(limit)
        rows = self.store.query(sql, tuple(params))
        return [self._normalize(dict(row)) for row in rows]

    def _sync_dataset_related_edges(
        self,
        research_run_id: str,
        *,
        dataset_version_id: str | None,
        dataset_digest: str | None,
        primary_only: bool,
    ) -> None:
        if not dataset_version_id and not dataset_digest:
            return
        related = self.list_related_by_dataset(
            dataset_version_id=dataset_version_id,
            dataset_digest=dataset_digest,
            exclude_research_run_ids=[research_run_id],
            limit=200,
            primary_only=primary_only,
        )
        for row in related:
            peer_id = row.get("research_run_id")
            if not peer_id:
                continue
            self.upsert_edge(research_run_id, peer_id, edge_kind="related_by_dataset")
            self.upsert_edge(peer_id, research_run_id, edge_kind="related_by_dataset")

    def load_signal_snapshot(self, research_run_id: str | None = None) -> dict[str, Any]:
        """读取并验证正式 signal_snapshot 产物。

        Args:
            research_run_id: 指定 research 运行标识；缺省时读取最近一次 ``signal_snapshot`` 主记录。

        Returns:
            归一化后的 signal_snapshot 载荷。

        Raises:
            ValueError: research run 不存在、artifact 类型不匹配、或结果结构非法时抛出。
        """
        row = self.get(research_run_id) if research_run_id else self.get_latest(artifact_type="signal_snapshot", primary_only=True)
        if row is None:
            if research_run_id:
                raise ValueError(f"未找到 research run: {research_run_id}")
            raise ValueError("数据库中不存在可用的 signal_snapshot research run")
        if row["artifact_type"] != "signal_snapshot":
            raise ValueError(
                f"research run {row['research_run_id']} 不是 signal_snapshot，而是 {row['artifact_type']}"
            )
        result = row.get("result")
        if not isinstance(result, dict):
            raise ValueError(f"research run {row['research_run_id']} 的 result 不是对象")
        selected_symbols = result.get("selected_symbols")
        if not isinstance(selected_symbols, list):
            raise ValueError(f"research run {row['research_run_id']} 缺少 selected_symbols 列表")
        normalized_symbols: list[dict[str, Any]] = []
        for item in selected_symbols:
            if not isinstance(item, dict):
                continue
            ts_code = str(item.get("ts_code", "")).strip()
            if not ts_code:
                continue
            normalized_symbols.append(
                {
                    "ts_code": ts_code,
                    "score": float(item.get("score", 0.0) or 0.0),
                    "target_weight": float(item.get("target_weight", 0.0) or 0.0),
                }
            )
        payload = dict(result)
        payload["artifact_schema_version"] = int(payload.get("artifact_schema_version") or 1)
        payload["artifact_type"] = "signal_snapshot"
        payload["selected_symbols"] = normalized_symbols
        promotion_package = payload.get("promotion_package")
        if not isinstance(promotion_package, dict):
            raise ValueError(f"research run {row['research_run_id']} 缺少 promotion_package")
        payload["promotion_package"] = promotion_package
        payload["research_run_id"] = row["research_run_id"]
        payload["dataset_version_id"] = row.get("dataset_version_id")
        payload["dataset_digest"] = row.get("dataset_digest")
        payload["research_session_id"] = row.get("research_session_id")
        payload["root_research_run_id"] = row.get("root_research_run_id")
        dataset_summary = dict(payload.get("dataset_summary") or {})
        dataset_summary.setdefault("dataset_version_id", row.get("dataset_version_id"))
        dataset_summary.setdefault("dataset_digest", row.get("dataset_digest"))
        payload["dataset_summary"] = dataset_summary
        try:
            return parse_signal_snapshot_payload(payload).model_dump(mode="python")
        except Exception as exc:
            raise ValueError(f"research run {row['research_run_id']} 的 signal_snapshot 合同非法: {exc}") from exc

    @staticmethod
    def _normalize(row: dict) -> dict:
        row["request"] = json.loads(row.pop("request_json") or "{}")
        row["result"] = json.loads(row.pop("result_json") or "{}")
        row["is_primary_run"] = bool(row.get("is_primary_run", 0))
        return row
