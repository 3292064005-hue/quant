from pathlib import Path

from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


def _build_repo(tmp_path: Path) -> ResearchRunRepository:
    store = SQLiteStore(str(tmp_path / "research_edges.db"))
    store.init_schema(load_schema_sql())
    return ResearchRunRepository(store)


def test_research_repository_materializes_dataset_related_edges(tmp_path: Path) -> None:
    repository = _build_repo(tmp_path)
    run_a = repository.create_run(
        workflow_name="workflow.research",
        artifact_type="signal_snapshot",
        dataset_version_id="dataset-v1",
        dataset_digest="digest-v1",
        request={"feature_name": "momentum"},
        result={"selected_symbols": []},
        is_primary_run=True,
    )
    run_b = repository.create_run(
        workflow_name="workflow.research",
        artifact_type="signal_snapshot",
        dataset_version_id="dataset-v1",
        dataset_digest="digest-v1",
        request={"feature_name": "momentum", "lookback": 5},
        result={"selected_symbols": []},
        is_primary_run=True,
    )
    related = repository.list_related_via_edges(run_a, edge_kinds=["related_by_dataset"])
    assert [item["research_run_id"] for item in related] == [run_b]
