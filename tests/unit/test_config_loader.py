from pathlib import Path

from a_share_quant.config.loader import ConfigLoader


def test_config_loader_merges_companion_files(temp_config_dir: Path) -> None:
    payload = (temp_config_dir / "risk.yaml").read_text(encoding="utf-8")
    assert "block_st" in payload
    config = ConfigLoader.load(temp_config_dir / "app.yaml")
    assert config.risk.rules.block_st is True
    assert config.backtest.metrics.annual_trading_days == 252


import pytest

from a_share_quant.config.loader import ConfigLoaderError


def test_config_loader_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigLoaderError):
        ConfigLoader.load(tmp_path / "missing.yaml")
