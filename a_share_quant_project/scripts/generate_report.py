from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.bootstrap import bootstrap
from a_share_quant.domain.models import BacktestResult


def main() -> int:
    parser = argparse.ArgumentParser(description="生成空模板报告")
    parser.add_argument("--config", default="configs/app.yaml")
    args = parser.parse_args()
    context = bootstrap(args.config)
    try:
        result = BacktestResult(strategy_id=context.config.strategy.strategy_id, run_id="manual")
        path = context.backtest_service.report_service.write_backtest_report(result)
        print({"report": str(Path(path))})
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
