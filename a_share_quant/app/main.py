"""主入口。"""
from __future__ import annotations

import argparse

from a_share_quant.app.bootstrap import bootstrap


def main() -> int:
    parser = argparse.ArgumentParser(description="A 股量化研究与交易工作站")
    parser.add_argument("--config", default="configs/app.yaml", help="配置文件路径")
    parser.add_argument("--import-csv", default="", help="导入 CSV 后执行回测")
    args = parser.parse_args()

    context = bootstrap(args.config)
    try:
        if args.import_csv:
            context.data_service.import_csv(args.import_csv, encoding=context.config.data.default_csv_encoding)
        bars_by_symbol, securities = context.data_service.load_market_data()
        strategy = context.strategy_service.build_default()
        result = context.backtest_service.run(strategy, bars_by_symbol, securities)
        print({
            "strategy_id": result.strategy_id,
            "run_id": result.run_id,
            "order_count": result.order_count,
            "fill_count": result.fill_count,
            "metrics": result.metrics,
            "report": result.report_path,
        })
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
