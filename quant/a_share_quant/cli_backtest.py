"""backtest 命令域 CLI。"""
from __future__ import annotations

import argparse

from a_share_quant.cli import _resolve_import_csv, _run_default_backtest


def main_daily_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行默认策略回测（默认只消费库内已有数据）")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--csv", default="", help="兼容旧参数名；提供后会先导入该 CSV 再回测")
    parser.add_argument("--import-csv", default="", help="显式导入该 CSV 后再执行回测")
    parser.add_argument("--research-run-id", default=None, help="可选 research signal_snapshot 运行标识；传入后回测将消费该研究信号")
    args = parser.parse_args(argv)
    return _run_default_backtest(
        args.config,
        import_csv_path=_resolve_import_csv(args),
        entrypoint="cli_backtest.main_daily_run",
        research_signal_run_id=args.research_run_id,
    )
