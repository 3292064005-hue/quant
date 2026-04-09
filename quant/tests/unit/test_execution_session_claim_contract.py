from pathlib import Path

from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import TradeSessionStatus
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


def _build_repo(tmp_path: Path) -> ExecutionSessionRepository:
    store = SQLiteStore(str(tmp_path / "sessions.db"))
    store.init_schema(load_schema_sql())
    return ExecutionSessionRepository(store)


def test_claim_sessions_for_supervisor_uses_compare_and_swap_semantics(tmp_path: Path) -> None:
    repository = _build_repo(tmp_path)
    session = repository.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2026-01-05",
        idempotency_key=None,
        status=TradeSessionStatus.RUNNING,
        account_id="acct-A",
    )
    first = repository.claim_sessions_for_supervisor(
        "owner-A",
        statuses=[TradeSessionStatus.RUNNING],
        lease_expires_at="2099-01-01T00:00:00+00:00",
        session_id=session.session_id,
        now="2026-01-05T09:30:00+00:00",
    )
    second = repository.claim_sessions_for_supervisor(
        "owner-B",
        statuses=[TradeSessionStatus.RUNNING],
        lease_expires_at="2099-01-01T00:00:00+00:00",
        session_id=session.session_id,
        now="2026-01-05T09:30:01+00:00",
    )
    assert [item.session_id for item in first] == [session.session_id]
    assert second == []
    latest = repository.get(session.session_id)
    assert latest is not None
    assert latest.supervisor_owner == "owner-A"


def test_release_supervisor_claim_reports_noop_when_owner_not_held(tmp_path: Path) -> None:
    repository = _build_repo(tmp_path)
    session = repository.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2026-01-05",
        idempotency_key=None,
        status=TradeSessionStatus.RUNNING,
        account_id="acct-A",
    )
    repository.claim_sessions_for_supervisor(
        "owner-A",
        statuses=[TradeSessionStatus.RUNNING],
        lease_expires_at="2099-01-01T00:00:00+00:00",
        session_id=session.session_id,
        now="2026-01-05T09:30:00+00:00",
    )
    assert repository.release_supervisor_claim(session.session_id, owner_id="owner-B") is False
    assert repository.release_supervisor_claim(session.session_id, owner_id="owner-A") is True



def test_renew_supervisor_claim_returns_false_when_update_does_not_affect_rows(tmp_path: Path, monkeypatch) -> None:
    repository = _build_repo(tmp_path)
    session = repository.create_session(
        runtime_mode="paper_trade",
        broker_provider="qmt",
        command_type="submit_orders",
        command_source="test",
        requested_by="tester",
        requested_trade_date="2026-01-05",
        idempotency_key=None,
        status=TradeSessionStatus.RUNNING,
        account_id="acct-A",
    )
    repository.claim_sessions_for_supervisor(
        "owner-A",
        statuses=[TradeSessionStatus.RUNNING],
        lease_expires_at="2099-01-01T00:00:00+00:00",
        session_id=session.session_id,
        now="2026-01-05T09:30:00+00:00",
    )

    original_execute_rowcount = repository.store.execute_rowcount

    def _fake_execute_rowcount(sql: str, params: tuple[object, ...]) -> int:
        if "UPDATE trade_sessions" in sql and "supervisor_owner = ?" in sql:
            return 0
        return original_execute_rowcount(sql, params)

    monkeypatch.setattr(repository.store, "execute_rowcount", _fake_execute_rowcount)
    assert repository.renew_supervisor_claim(
        session.session_id,
        owner_id="owner-A",
        lease_expires_at="2099-01-01T00:05:00+00:00",
        now="2026-01-05T09:31:00+00:00",
    ) is False
