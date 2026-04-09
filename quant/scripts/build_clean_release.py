#!/usr/bin/env python3
"""构建干净发布包。"""
from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

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
    return (
        name in EXCLUDE_NAMES
        or name.startswith(".coverage.")
        or name.endswith(".egg-info")
        or (name.startswith(".") and name.endswith("_staging"))
    )



def _should_skip(path: Path) -> bool:
    return _should_skip_name(path.name) or path.suffix in EXCLUDE_SUFFIXES



def _copy_clean_tree(src: Path, dst: Path) -> None:
    for path in src.rglob("*"):
        relative = path.relative_to(src)
        if any(_should_skip_name(part) for part in relative.parts):
            continue
        if _should_skip(path):
            continue
        target = dst / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)



def build_release(source_dir: Path, output_path: Path) -> Path:
    """基于临时 staging 目录构建干净发布包。"""
    with tempfile.TemporaryDirectory(prefix=f"{output_path.stem}_staging_") as staging_dir_str:
        staging_dir = Path(staging_dir_str)
        _copy_clean_tree(source_dir, staging_dir)
        archive_base = output_path.with_suffix("")
        archive_path = Path(shutil.make_archive(str(archive_base), "zip", root_dir=staging_dir))
        return archive_path



def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="构建干净发布包")
    parser.add_argument("--source", default=".", help="仓库根目录")
    parser.add_argument("--output", default="dist/a_share_quant_release.zip", help="输出 zip 路径")
    args = parser.parse_args(argv)

    source_dir = Path(args.source).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path = build_release(source_dir, output_path)
    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
