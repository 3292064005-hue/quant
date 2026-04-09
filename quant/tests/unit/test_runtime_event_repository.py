from pathlib import Path

from a_share_quant.core.events import Event, EventBus, EventJournal, EventType
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.domain.models import TradeSessionStatus
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository
from a_share_quant.repositories.runtime_event_repository import RuntimeEventRepository
from a_share_quant.storage.sqlite_store import SQLiteStore


def _build_store(tmp_path: Path) -> SQLiteStore:
    store = SQLiteStore(str(tmp_path / "runtime_events.db"))
    store.init_schema(load_schema_sql())
    return store


def test_execution_session_events_are_mirrored_to_runtime_event_stream(tmp_path: Path) -> None:
    store = _build_store(tmp_path)
    repository = ExecutionSessionRepository(store)
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
    repository.append_event(session.session_id, event_type="ORDER_ACCEPTED", level="INFO", payload={"order_id": "o1"})
    runtime_events = RuntimeEventRepository(store).list_recent(source_domain="operator", stream_scope="trade_session")
    assert runtime_events[0]["stream_id"] == session.session_id
    assert runtime_events[0]["event_type"] == "ORDER_ACCEPTED"


def test_event_bus_journal_can_sink_to_runtime_event_stream(tmp_path: Path) -> None:
    store = _build_store(tmp_path)
    repo = RuntimeEventRepository(store)
    bus = EventBus(
        journal=EventJournal(
            sink=lambda event: repo.append_from_event(
                event,
                source_domain="backtest",
                stream_scope="run_event",
                stream_id=event.payload.get("run_id") if isinstance(event.payload, dict) else None,
            )
        )
    )
    bus.publish(Event(event_type=EventType.DAY_CLOSED, payload={"run_id": "run-1", "trade_date": "2026-01-05"}))
    events = repo.list_recent(source_domain="backtest", stream_scope="run_event")
    assert events[0]["stream_id"] == "run-1"
    assert events[0]["event_type"] == EventType.DAY_CLOSED
