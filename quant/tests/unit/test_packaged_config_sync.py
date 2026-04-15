from __future__ import annotations

from pathlib import Path

from a_share_quant.app.distribution_profile_contract import get_distribution_profile_spec
from a_share_quant.resources.config_manifest import expected_configs_for_profile
from a_share_quant.resources.config_sync import sync_packaged_configs
from scripts.build_clean_release import prepare_project_staging


def test_repo_tree_keeps_single_config_source() -> None:
    packaged_root = Path("a_share_quant/resources/configs")
    is_release_tree = Path("release_profile.txt").exists()
    expected = expected_configs_for_profile("workstation")
    if is_release_tree:
        assert all((packaged_root / rel_path).exists() for rel_path in expected)
    else:
        assert all(not (packaged_root / rel_path).exists() for rel_path in expected)



def test_prepare_project_staging_materializes_packaged_configs(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    prepare_project_staging(Path(".").resolve(), staging_dir)

    changed = sync_packaged_configs(project_root=staging_dir, check_only=True)
    assert changed == []
    for rel_path in expected_configs_for_profile("workstation"):
        assert (staging_dir / "a_share_quant" / "resources" / "configs" / rel_path).exists(), rel_path


def test_prepare_project_staging_materializes_profile_specific_packaged_configs(tmp_path: Path) -> None:
    profile = "production"
    profile_spec = get_distribution_profile_spec(profile)
    if Path("release_profile.txt").exists() and not Path(profile_spec.selected_requirements).exists():
        return
    staging_dir = tmp_path / "production_staging"
    prepare_project_staging(Path(".").resolve(), staging_dir, distribution_profile=profile)
    changed = sync_packaged_configs(project_root=staging_dir, check_only=True, distribution_profile=profile)
    assert changed == []
    expected = expected_configs_for_profile(profile)
    packaged_root = staging_dir / "a_share_quant" / "resources" / "configs"
    assert all((packaged_root / rel_path).exists() for rel_path in expected)
    assert not (packaged_root / "backtest.yaml").exists()
    assert not (packaged_root / "research_batch.json").exists()
