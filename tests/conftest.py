from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture()
def temp_config_dir(tmp_path: Path) -> Path:
    """复制一套隔离配置并重写运行时路径。"""
    source_dir = PROJECT_ROOT / "configs"
    target_dir = tmp_path / "configs"
    shutil.copytree(source_dir, target_dir)
    app_config_path = target_dir / "app.yaml"
    payload = yaml.safe_load(app_config_path.read_text(encoding="utf-8"))
    payload.setdefault("database", {})["path"] = str(tmp_path / "runtime" / "test.db")
    payload.setdefault("data", {})["reports_dir"] = str(tmp_path / "runtime" / "reports")
    payload.setdefault("data", {})["storage_dir"] = str(tmp_path / "runtime" / "data")
    app_config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return target_dir
