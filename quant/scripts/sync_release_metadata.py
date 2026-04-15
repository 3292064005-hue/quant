#!/usr/bin/env python3
"""同步/校验 release 元数据文本。"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant import __version__

_TARGETS = (
    (Path("README.md"), "当前发布版本：`", "`"),
    (Path("IMPLEMENTATION_SUMMARY.md"), "当前交付版本：`", "`"),
    (Path("docs/architecture.md"), "## 10. v", " 扩展边界"),
)


def _replace_marker(text: str, prefix: str, suffix: str) -> str:
    start = text.find(prefix)
    if start < 0:
        raise ValueError(f"未找到 release marker: {prefix!r}")
    start_value = start + len(prefix)
    end = text.find(suffix, start_value)
    if end < 0:
        raise ValueError(f"未找到 release marker suffix: {suffix!r}")
    return text[:start_value] + __version__ + text[end:]


def sync_release_metadata(*, check_only: bool = False, project_root: Path | None = None) -> list[str]:
    """同步或校验版本元数据。

    Args:
        check_only: 为 ``True`` 时仅返回未同步的文件，不改写磁盘。
        project_root: 目标项目根目录。默认使用当前仓库根目录；构建/验证时可传入 staging root。
    """
    root = (project_root or PROJECT_ROOT).resolve()
    changed: list[str] = []
    for rel_path, prefix, suffix in _TARGETS:
        path = root / rel_path
        original = path.read_text(encoding="utf-8")
        updated = _replace_marker(original, prefix, suffix)
        if updated != original:
            changed.append(str(rel_path))
            if not check_only:
                path.write_text(updated, encoding="utf-8")
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步或校验 release metadata")
    parser.add_argument("--check", action="store_true", help="只校验，不改文件")
    args = parser.parse_args(argv)
    changed = sync_release_metadata(check_only=args.check)
    if args.check and changed:
        raise SystemExit(f"release metadata 未同步: {', '.join(changed)}")
    for item in changed:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
