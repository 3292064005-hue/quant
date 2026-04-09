from __future__ import annotations

from pathlib import Path


def test_packaged_configs_match_repo_configs() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    repo_configs = repo_root / "configs"
    packaged_configs = repo_root / "a_share_quant" / "resources" / "configs"

    expected_files = [
        Path("app.yaml"),
        Path("backtest.yaml"),
        Path("data.yaml"),
        Path("risk.yaml"),
        Path("operator_paper_trade.yaml"),
        Path("operator_paper_trade_demo.yaml"),
        Path("research_batch.json"),
        Path("broker/qmt.yaml"),
        Path("broker/ptrade.yaml"),
    ]

    for rel_path in expected_files:
        repo_file = repo_configs / rel_path
        packaged_file = packaged_configs / rel_path
        assert repo_file.exists(), f"repo 配置缺失: {rel_path}"
        assert packaged_file.exists(), f"打包配置缺失: {rel_path}"
        assert repo_file.read_bytes() == packaged_file.read_bytes(), f"repo 与打包配置不一致: {rel_path}"
