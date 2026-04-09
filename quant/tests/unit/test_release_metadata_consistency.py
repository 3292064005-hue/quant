from __future__ import annotations

from pathlib import Path

from a_share_quant import __version__


def test_release_metadata_is_consistent() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    implementation_summary = Path("IMPLEMENTATION_SUMMARY.md").read_text(encoding="utf-8")
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")

    assert f"当前发布版本：`{__version__}`" in readme
    assert f"当前交付版本：`{__version__}`" in implementation_summary
    assert f"## 10. v{__version__} 扩展边界" in architecture
