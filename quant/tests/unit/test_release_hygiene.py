from __future__ import annotations

from pathlib import Path


def test_gitignore_covers_release_artifacts() -> None:
    entries = set(Path('.gitignore').read_text(encoding='utf-8').splitlines())
    expected = {
        '__pycache__/',
        '.pytest_cache/',
        'runtime/',
        'build/',
        'dist/',
        '*.egg-info/',
        '.mypy_cache/',
        '.ruff_cache/',
        '.coverage',
        '.coverage.*',
        '.*_staging/',
    }
    missing = expected - entries
    assert not missing, f'.gitignore 缺少发布污染物忽略规则: {sorted(missing)}'


def test_manifest_prunes_release_artifacts() -> None:
    manifest = Path('MANIFEST.in').read_text(encoding='utf-8')
    for rule in [
        'global-exclude .coverage*',
        'prune build',
        'prune dist',
        'prune .git',
        'prune runtime',
        'prune .pytest_cache',
        'prune .mypy_cache',
        'prune .ruff_cache',
        'prune a_share_quant_workstation.egg-info',
        'prune .out_staging',
    ]:
        assert rule in manifest, f'MANIFEST.in 缺少规则: {rule}'
