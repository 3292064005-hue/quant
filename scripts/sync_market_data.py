from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.bootstrap import bootstrap


def _parse_symbols(symbols: str | None) -> list[str] | None:
    if symbols is None:
        return None
    items = [item.strip() for item in symbols.split(",") if item.strip()]
    return items or None


def main() -> int:
    parser = argparse.ArgumentParser(description="同步市场数据")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--provider", default=None, choices=["csv", "tushare", "akshare"])
    parser.add_argument("--csv", help="CSV 导入路径；provider=csv 时必填")
    parser.add_argument("--start-date", help="开始日期，格式 YYYYMMDD")
    parser.add_argument("--end-date", help="结束日期，格式 YYYYMMDD")
    parser.add_argument("--symbols", help="逗号分隔的 ts_code 列表，例如 600000.SH,000001.SZ")
    args = parser.parse_args()

    context = bootstrap(args.config)
    try:
        provider = (args.provider or context.config.data.provider).lower()
        if provider == "csv":
            if not args.csv:
                raise SystemExit("provider=csv 时必须提供 --csv")
            bundle = context.data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
        else:
            if not args.start_date or not args.end_date:
                raise SystemExit("在线同步时必须同时提供 --start-date 和 --end-date")
            bundle = context.data_service.sync_from_provider(
                provider_name=provider,
                start_date=args.start_date,
                end_date=args.end_date,
                ts_codes=_parse_symbols(args.symbols),
                exchange=context.config.data.default_exchange,
            )
        print(
            {
                "provider": provider,
                "symbols": len(bundle.securities),
                "calendar_entries": len(bundle.calendar),
                "bar_count": len(bundle.bars),
                "status": "imported",
            }
        )
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
