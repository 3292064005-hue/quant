"""配置资源同步工具。"""
from __future__ import annotations

import filecmp
import shutil
from pathlib import Path

from a_share_quant.resources.config_manifest import expected_configs_for_profile

REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_roots(project_root: Path | None = None) -> tuple[Path, Path, Path]:
    root = (project_root or REPO_ROOT).resolve()
    return root, root / "configs", root / "a_share_quant" / "resources" / "configs"


def _detect_distribution_profile(root: Path) -> str:
    marker = root / "release_profile.txt"
    if marker.exists():
        for line in marker.read_text(encoding="utf-8").splitlines():
            if line.startswith("distribution_profile="):
                return line.split("=", 1)[1].strip() or "workstation"
    return "workstation"


def sync_packaged_configs(*, check_only: bool = False, project_root: Path | None = None, distribution_profile: str | None = None) -> list[str]:
    root, config_source_root, packaged_config_root = _resolve_roots(project_root)
    if not config_source_root.exists():
        raise FileNotFoundError(f"配置源目录不存在: {config_source_root}")
    profile = distribution_profile or _detect_distribution_profile(root)
    changed: list[str] = []
    for rel_path in expected_configs_for_profile(profile):
        source_path = config_source_root / rel_path
        packaged_path = packaged_config_root / rel_path
        if not source_path.exists():
            raise FileNotFoundError(f"配置源文件不存在: {source_path}")
        if not packaged_path.exists() or not filecmp.cmp(source_path, packaged_path, shallow=False):
            changed.append(str(rel_path))
            if not check_only:
                packaged_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, packaged_path)
    return changed
