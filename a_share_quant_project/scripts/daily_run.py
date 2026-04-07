from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.bootstrap import bootstrap


def main() -> int:
    parser = argparse.ArgumentParser(description="执行默认策略回测")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--csv", default="sample_data/daily_bars.csv")
    parser.add_argument("--skip-import", action="store_true", help="跳过 CSV 导入，直接使用数据库中已有行情")
    args = parser.parse_args()
    context = bootstrap(args.config)
    try:
        if not args.skip_import:
            context.data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
        bars_by_symbol, securities = context.data_service.load_market_data()
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, bars_by_symbol, securities)
        print({"metrics": result.metrics, "order_count": result.order_count, "fill_count": result.fill_count})
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
