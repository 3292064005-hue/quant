from __future__ import annotations

from pathlib import Path

from setuptools import find_packages

EXPECTED_PACKAGES = {
    'a_share_quant',
    'a_share_quant.adapters',
    'a_share_quant.adapters.broker',
    'a_share_quant.adapters.data',
    'a_share_quant.app',
    'a_share_quant.config',
    'a_share_quant.core',
    'a_share_quant.core.rules',
    'a_share_quant.domain',
    'a_share_quant.engines',
    'a_share_quant.engines.execution_models',
    'a_share_quant.plugins',
    'a_share_quant.providers',
    'a_share_quant.repositories',
    'a_share_quant.services',
    'a_share_quant.storage',
    'a_share_quant.strategies',
    'a_share_quant.ui',
    'a_share_quant.ui.panels',
    'a_share_quant.workflows',
}


def test_source_package_directories_have_init_files() -> None:
    for package_name in EXPECTED_PACKAGES:
        package_path = Path(*package_name.split('.'))
        assert (package_path / '__init__.py').exists(), f'{package_name} 缺少 __init__.py，会导致安装态打包缺包'


def test_setuptools_package_discovery_covers_runtime_packages() -> None:
    discovered = set(find_packages(include=['a_share_quant*']))
    missing = EXPECTED_PACKAGES - discovered
    assert not missing, f'安装态仍缺少包发现: {sorted(missing)}'
