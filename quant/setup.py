from __future__ import annotations

import importlib.util
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


def _load_expected_configs() -> tuple[Path, ...]:
    manifest_path = Path(__file__).resolve().parent / "a_share_quant" / "resources" / "config_manifest.py"
    spec = importlib.util.spec_from_file_location("_config_manifest", manifest_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载配置清单模块: {manifest_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return tuple(module.EXPECTED_CONFIGS)


EXPECTED_CONFIGS = _load_expected_configs()


class build_py(_build_py):
    """在 build 目录内物化安装态 package configs，保持源码树单一真相源。"""

    def run(self) -> None:
        super().run()
        project_root = Path(__file__).resolve().parent
        source_root = project_root / "configs"
        target_root = Path(self.build_lib) / "a_share_quant" / "resources" / "configs"
        for rel_path in EXPECTED_CONFIGS:
            source_path = source_root / rel_path
            if not source_path.exists():
                raise FileNotFoundError(f"配置源文件不存在: {source_path}")
            target_path = target_root / rel_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(source_path.read_bytes())


setup(cmdclass={"build_py": build_py})
