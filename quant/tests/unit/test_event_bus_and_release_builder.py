from __future__ import annotations

import zipfile
from pathlib import Path

from a_share_quant.core.events import EventBus, EventType
from scripts.build_clean_release import build_release


def test_event_bus_records_history_and_supports_replay() -> None:
    bus = EventBus()
    bus.publish_type(EventType.ORDER_ACCEPTED, {"order_id": "o1"})
    bus.publish_type(EventType.ORDER_FILLED, {"order_id": "o1"})

    replayed: list[tuple[str, str]] = []
    count = bus.replay_history(lambda event: replayed.append((event.event_type, event.payload["order_id"])))

    assert count == 2
    assert replayed == [
        (EventType.ORDER_ACCEPTED, "o1"),
        (EventType.ORDER_FILLED, "o1"),
    ]
    assert len(bus.history_snapshot()) == 2



def test_build_clean_release_excludes_runtime_cache_and_local_metadata(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("demo", encoding="utf-8")
    (source_dir / "package").mkdir()
    (source_dir / "package" / "module.py").write_text("print('ok')\n", encoding="utf-8")
    (source_dir / "package" / "__pycache__").mkdir()
    (source_dir / "package" / "__pycache__" / "module.cpython-312.pyc").write_bytes(b"x")
    (source_dir / "runtime").mkdir()
    (source_dir / "runtime" / "app.log").write_text("noise", encoding="utf-8")
    (source_dir / ".git").mkdir()
    (source_dir / ".git" / "config").write_text("[core]\n", encoding="utf-8")
    (source_dir / ".coverage").write_text("coverage-data", encoding="utf-8")
    (source_dir / ".coverage.integration").write_text("coverage-shard", encoding="utf-8")

    archive_path = build_release(source_dir, tmp_path / "dist" / "release.zip")

    with zipfile.ZipFile(archive_path) as handle:
        names = set(handle.namelist())

    assert "README.md" in names
    assert "package/module.py" in names
    assert all("__pycache__" not in name for name in names)
    assert all(not name.startswith("runtime/") for name in names)
    assert all(not name.startswith(".git/") for name in names)
    assert ".coverage" not in names
    assert ".coverage.integration" not in names



def test_build_clean_release_excludes_hidden_staging_and_egg_info(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("demo", encoding="utf-8")
    (source_dir / ".out_staging").mkdir()
    (source_dir / ".out_staging" / "nested.txt").write_text("noise", encoding="utf-8")
    (source_dir / "demo.egg-info").mkdir()
    (source_dir / "demo.egg-info" / "PKG-INFO").write_text("meta", encoding="utf-8")

    archive_path = build_release(source_dir, tmp_path / "dist" / "release.zip")

    with zipfile.ZipFile(archive_path) as handle:
        names = set(handle.namelist())
        assert handle.testzip() is None

    assert "README.md" in names
    assert all(".out_staging" not in name for name in names)
    assert all(".egg-info" not in name for name in names)
