from __future__ import annotations

import zipfile
from pathlib import Path

from a_share_quant.core.events import EventBus, EventType
from scripts.build_clean_release import build_release


def _write_distribution_requirements(source_dir: Path) -> None:
    for name, payload in {
        "requirements.txt": "pandas>=2.0\n",
        "requirements-core.txt": "-r requirements.txt\n",
        "requirements-workstation.txt": "-r requirements-core.txt\n",
        "requirements-production.txt": "-r requirements-core.txt\n",
        "requirements-dev.txt": "-r requirements-workstation.txt\npytest>=8.0\n",
    }.items():
        (source_dir / name).write_text(payload, encoding="utf-8")



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
    assert "release_profile.txt" in names
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


def test_build_clean_release_materializes_generated_metadata_only_in_staging(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "pyproject.toml").write_text('[project]\nname = "demo"\n', encoding="utf-8")
    (source_dir / "README.md").write_text("当前发布版本：`0.0.0`", encoding="utf-8")
    (source_dir / "IMPLEMENTATION_SUMMARY.md").write_text("当前交付版本：`0.0.0`", encoding="utf-8")
    docs_dir = source_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "architecture.md").write_text("## 10. v0.0.0 扩展边界", encoding="utf-8")
    package_dir = source_dir / "a_share_quant"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("__version__ = '0.5.6'\n", encoding="utf-8")
    resources_dir = package_dir / "resources"
    resources_dir.mkdir()
    (resources_dir / "__init__.py").write_text("", encoding="utf-8")
    (resources_dir / "configs").mkdir()
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "__init__.py").write_text("", encoding="utf-8")
    configs_dir = source_dir / "configs"
    (configs_dir / "broker").mkdir(parents=True)
    for rel_path, payload in {
        "app.yaml": "app: {}\n",
        "backtest.yaml": "backtest: {}\n",
        "data.yaml": "data: {}\n",
        "risk.yaml": "risk: {}\n",
        "operator_paper_trade.yaml": "extends: app.yaml\noperator: {}\n",
        "operator_paper_trade_demo.yaml": "extends: operator_paper_trade.yaml\noperator: {}\n",
        "research_batch.json": "{}\n",
        "broker/qmt.yaml": "client_factory: demo\n",
        "broker/ptrade.yaml": "client_factory: demo\n",
    }.items():
        target = configs_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")

    _write_distribution_requirements(source_dir)
    archive_path = build_release(source_dir, tmp_path / "dist" / "release.zip")

    assert not any((source_dir / "a_share_quant" / "resources" / "configs").rglob("*.*"))
    assert "0.0.0" in (source_dir / "README.md").read_text(encoding="utf-8")

    with zipfile.ZipFile(archive_path) as handle:
        names = set(handle.namelist())

    assert "a_share_quant/resources/configs/app.yaml" in names
    assert "README.md" in names


def test_build_clean_release_supports_distribution_profile_marker(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("当前发布版本：`0.0.0`", encoding="utf-8")
    (source_dir / "pyproject.toml").write_text('[project]\nname = "demo"\n', encoding="utf-8")
    (source_dir / "IMPLEMENTATION_SUMMARY.md").write_text("当前交付版本：`0.0.0`", encoding="utf-8")
    docs_dir = source_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "architecture.md").write_text("## 10. v0.0.0 扩展边界", encoding="utf-8")
    package_dir = source_dir / "a_share_quant"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("__version__ = '0.5.6'\n", encoding="utf-8")
    resources_dir = package_dir / "resources"
    resources_dir.mkdir()
    (resources_dir / "__init__.py").write_text("", encoding="utf-8")
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "__init__.py").write_text("", encoding="utf-8")
    configs_dir = source_dir / "configs"
    (configs_dir / "broker").mkdir(parents=True)
    for rel_path, payload in {
        "app.yaml": "app: {}\n",
        "backtest.yaml": "backtest: {}\n",
        "data.yaml": "data: {}\n",
        "risk.yaml": "risk: {}\n",
        "operator_paper_trade.yaml": "extends: app.yaml\noperator: {}\n",
        "operator_paper_trade_demo.yaml": "extends: operator_paper_trade.yaml\noperator: {}\n",
        "research_batch.json": "{}\n",
        "broker/qmt.yaml": "client_factory: demo\n",
        "broker/ptrade.yaml": "client_factory: demo\n",
    }.items():
        target = configs_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")

    _write_distribution_requirements(source_dir)
    archive_path = build_release(source_dir, tmp_path / "dist" / "release.zip", distribution_profile="production")

    with zipfile.ZipFile(archive_path) as handle:
        assert handle.read("release_profile.txt").decode("utf-8") == "distribution_profile=production\n"
        manifest = handle.read("distribution_manifest.json").decode("utf-8")
        pyproject = handle.read("pyproject.toml").decode("utf-8")
    assert '"distribution_profile": "production"' in manifest
    assert 'name = "a-share-quant-production"' in pyproject


def test_distribution_profile_releases_materially_differ(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("当前发布版本：`0.0.0`", encoding="utf-8")
    (source_dir / "IMPLEMENTATION_SUMMARY.md").write_text("当前交付版本：`0.0.0`", encoding="utf-8")
    (source_dir / "pyproject.toml").write_text('[project]\nname = "demo"\ndescription = "demo"\n', encoding="utf-8")
    docs_dir = source_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "architecture.md").write_text("## 10. v0.0.0 扩展边界", encoding="utf-8")
    (docs_dir / "operator_manual.md").write_text("operator", encoding="utf-8")
    (docs_dir / "strategy_spec.md").write_text("strategy", encoding="utf-8")
    package_dir = source_dir / "a_share_quant"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("__version__ = '0.5.6'\n", encoding="utf-8")
    ui_dir = package_dir / "ui"
    ui_dir.mkdir()
    (ui_dir / "__init__.py").write_text("", encoding="utf-8")
    (ui_dir / "main_window.py").write_text("print('ui')\n", encoding="utf-8")
    resources_dir = package_dir / "resources"
    resources_dir.mkdir()
    (resources_dir / "__init__.py").write_text("", encoding="utf-8")
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "__init__.py").write_text("", encoding="utf-8")
    for script_name in ["launch_ui.py", "research.py", "operator_snapshot.py", "operator_submit_order.py", "operator_sync_session.py", "operator_reconcile_session.py", "operator_run_supervisor.py"]:
        (scripts_dir / script_name).write_text("print('ok')\n", encoding="utf-8")
    sample_dir = source_dir / "sample_data"
    sample_dir.mkdir()
    (sample_dir / "daily_bars.csv").write_text("x\n", encoding="utf-8")
    configs_dir = source_dir / "configs"
    (configs_dir / "broker").mkdir(parents=True)
    for rel_path, payload in {"app.yaml": "app: {}\n", "backtest.yaml": "backtest: {}\n", "data.yaml": "data: {}\n", "risk.yaml": "risk: {}\n", "operator_paper_trade.yaml": "extends: app.yaml\noperator: {}\n", "operator_paper_trade_demo.yaml": "extends: operator_paper_trade.yaml\noperator: {}\n", "research_batch.json": "{}\n", "broker/qmt.yaml": "client_factory: demo\n", "broker/ptrade.yaml": "client_factory: demo\n"}.items():
        target = configs_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    _write_distribution_requirements(source_dir)
    core_archive = build_release(source_dir, tmp_path / "dist" / "core.zip", distribution_profile="core")
    prod_archive = build_release(source_dir, tmp_path / "dist" / "prod.zip", distribution_profile="production")
    with zipfile.ZipFile(core_archive) as core_handle, zipfile.ZipFile(prod_archive) as prod_handle:
        core_names = set(core_handle.namelist())
        prod_names = set(prod_handle.namelist())
        assert core_handle.read("release_profile.txt").decode("utf-8") == "distribution_profile=core\n"
        assert prod_handle.read("release_profile.txt").decode("utf-8") == "distribution_profile=production\n"
        assert "a_share_quant/ui/main_window.py" not in core_names
        assert "a_share_quant/ui/main_window.py" not in prod_names
        assert "scripts/operator_snapshot.py" not in core_names
        assert "scripts/research.py" not in prod_names
        assert "configs/operator_paper_trade_demo.yaml" not in core_names
        assert "configs/backtest.yaml" not in prod_names
        assert core_names != prod_names


def test_build_clean_release_fails_when_profile_requirements_surface_missing(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("当前发布版本：`0.0.0`", encoding="utf-8")
    (source_dir / "IMPLEMENTATION_SUMMARY.md").write_text("当前交付版本：`0.0.0`", encoding="utf-8")
    (source_dir / "pyproject.toml").write_text('[project]\nname = "demo"\ndescription = "demo"\n', encoding="utf-8")
    docs_dir = source_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "architecture.md").write_text("## 10. v0.0.0 扩展边界", encoding="utf-8")
    package_dir = source_dir / "a_share_quant"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("__version__ = '0.5.6'\n", encoding="utf-8")
    resources_dir = package_dir / "resources"
    resources_dir.mkdir()
    (resources_dir / "__init__.py").write_text("", encoding="utf-8")
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "__init__.py").write_text("", encoding="utf-8")
    configs_dir = source_dir / "configs"
    (configs_dir / "broker").mkdir(parents=True)
    for rel_path, payload in {
        "app.yaml": "app: {}\n",
        "backtest.yaml": "backtest: {}\n",
        "data.yaml": "data: {}\n",
        "risk.yaml": "risk: {}\n",
        "operator_paper_trade.yaml": "extends: app.yaml\noperator: {}\n",
        "operator_paper_trade_demo.yaml": "extends: operator_paper_trade.yaml\noperator: {}\n",
        "research_batch.json": "{}\n",
        "broker/qmt.yaml": "client_factory: demo\n",
        "broker/ptrade.yaml": "client_factory: demo\n",
    }.items():
        target = configs_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    _write_distribution_requirements(source_dir)
    (source_dir / "requirements-core.txt").unlink()

    try:
        build_release(source_dir, tmp_path / "dist" / "core.zip", distribution_profile="core")
    except FileNotFoundError as exc:
        assert "requirements-core.txt" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected FileNotFoundError")
