"""data/admin 命令域 CLI。"""
from __future__ import annotations

import argparse

from a_share_quant.app.bootstrap import bootstrap_data_context, bootstrap_storage_context
from a_share_quant.cli import _parse_symbols


def main_init_db(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="初始化数据库")
    parser.add_argument("--config", default="configs/app.yaml")
    args = parser.parse_args(argv)
    with bootstrap_storage_context(args.config) as context:
        print({"database": context.config.database.path, "status": "initialized"})
        return 0


def main_sync_market_data(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步市场数据")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--provider", default=None, choices=["csv", "tushare", "akshare"])
    parser.add_argument("--csv", help="CSV 导入路径；provider=csv 时必填")
    parser.add_argument("--start-date", help="开始日期，格式 YYYYMMDD")
    parser.add_argument("--end-date", help="结束日期，格式 YYYYMMDD")
    parser.add_argument("--symbols", help="逗号分隔的 ts_code 列表，例如 600000.SH,000001.SZ")
    args = parser.parse_args(argv)

    with bootstrap_data_context(args.config) as context:
        capabilities = context.research_capabilities()
        data_service = capabilities.require_data_service()
        provider = (args.provider or capabilities.config.data.provider).lower()
        if provider == "csv":
            if not args.csv:
                raise SystemExit("provider=csv 时必须提供 --csv")
            bundle = data_service.import_csv(args.csv, encoding=capabilities.config.data.default_csv_encoding)
        else:
            if not args.start_date or not args.end_date:
                raise SystemExit("在线同步时必须同时提供 --start-date 和 --end-date")
            bundle = data_service.sync_from_provider(
                provider_name=provider,
                start_date=args.start_date,
                end_date=args.end_date,
                ts_codes=_parse_symbols(args.symbols),
                exchange=capabilities.config.data.default_exchange,
            )
        print(
            {
                "provider": provider,
                "symbols": len(bundle.securities),
                "calendar_entries": len(bundle.calendar),
                "bar_count": len(bundle.bars),
                "degradation_flags": bundle.degradation_flags,
                "warnings": bundle.warnings,
                "import_run_id": data_service.last_import_run_id,
                "status": "imported",
            }
        )
        return 0
