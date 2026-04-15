#!/usr/bin/env python3
"""构建干净发布包。"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.distribution_profile_contract import (
    ALL_DISTRIBUTION_PROFILES,
    build_distribution_manifest,
    ensure_profile_surface_exists,
    get_distribution_profile_spec,
    profile_requirement_filenames,
)
from a_share_quant.resources.config_sync import sync_packaged_configs
from scripts.sync_release_metadata import sync_release_metadata

EXCLUDE_NAMES = {
    ".coverage",
    ".git",
    ".idea",
    ".vscode",
    ".DS_Store",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "build",
    "dist",
    "runtime",
}
EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".db", ".sqlite3", ".zip"}


def _should_skip_name(name: str) -> bool:
    return name in EXCLUDE_NAMES or name.startswith(".coverage.") or name.endswith(".egg-info") or (name.startswith(".") and name.endswith("_staging"))


def _should_skip(path: Path) -> bool:
    return _should_skip_name(path.name) or path.suffix in EXCLUDE_SUFFIXES


def _copy_clean_tree(src: Path, dst: Path) -> None:
    for path in src.rglob("*"):
        relative = path.relative_to(src)
        if any(_should_skip_name(part) for part in relative.parts):
            continue
        if relative.parts[:3] == ("a_share_quant", "resources", "configs"):
            continue
        if _should_skip(path):
            continue
        target = dst / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _looks_like_project_root(root: Path) -> bool:
    required = [root / "pyproject.toml", root / "a_share_quant", root / "configs", root / "scripts"]
    return all(path.exists() for path in required)


def _remove_path_if_exists(target: Path) -> None:
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()



def _apply_profile_surface(staging_dir: Path, distribution_profile: str) -> None:
    spec = ensure_profile_surface_exists(staging_dir, distribution_profile)
    for rel_path in spec.excluded_paths:
        _remove_path_if_exists(staging_dir / rel_path)
    pyproject_path = staging_dir / "pyproject.toml"
    if pyproject_path.exists():
        text = pyproject_path.read_text(encoding="utf-8")
        text = re.sub(r'(?m)^name = ".*"$', f'name = "{spec.project_name}"', text, count=1)
        text = re.sub(r'(?m)^description = ".*"$', f'description = "{spec.description}"', text, count=1)
        pyproject_path.write_text(text, encoding="utf-8")
    selected_requirements = spec.selected_requirements
    known_requirement_names = set(profile_requirement_filenames())
    for req_path in staging_dir.glob("requirements-*.txt"):
        if req_path.name in known_requirement_names and req_path.name != selected_requirements:
            req_path.unlink()
    if not (staging_dir / selected_requirements).exists():
        raise FileNotFoundError(
            f"distribution profile={distribution_profile} 缺少 requirements surface: {selected_requirements}"
        )
    manifest = build_distribution_manifest(distribution_profile)
    (staging_dir / "distribution_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def prepare_project_staging(source_dir: Path, staging_dir: Path, *, distribution_profile: str = "workstation") -> Path:
    if distribution_profile not in ALL_DISTRIBUTION_PROFILES:
        raise ValueError(f"distribution_profile 不支持: {distribution_profile}")
    _copy_clean_tree(source_dir, staging_dir)
    if _looks_like_project_root(staging_dir):
        sync_release_metadata(project_root=staging_dir, check_only=False)
        sync_packaged_configs(project_root=staging_dir, check_only=False)
        _apply_profile_surface(staging_dir, distribution_profile)
    (staging_dir / "release_profile.txt").write_text(f"distribution_profile={distribution_profile}\n", encoding="utf-8")
    return staging_dir


def build_release(source_dir: Path, output_path: Path, *, distribution_profile: str = "workstation") -> Path:
    with tempfile.TemporaryDirectory(prefix=f"{output_path.stem}_staging_") as staging_dir_str:
        staging_dir = Path(staging_dir_str)
        prepare_project_staging(source_dir, staging_dir, distribution_profile=distribution_profile)
        archive_base = output_path.with_suffix("")
        return Path(shutil.make_archive(str(archive_base), "zip", root_dir=staging_dir))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="构建干净发布包")
    parser.add_argument("--source", default=".", help="仓库根目录")
    parser.add_argument("--output", default="dist/a_share_quant_release.zip", help="输出 zip 路径")
    parser.add_argument("--distribution-profile", default="workstation", help="发布形态: core/workstation/production")
    args = parser.parse_args(argv)
    source_dir = Path(args.source).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path = build_release(source_dir, output_path, distribution_profile=args.distribution_profile)
    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
