from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.bootstrap import bootstrap


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化数据库")
    parser.add_argument("--config", default="configs/app.yaml")
    args = parser.parse_args()
    context = bootstrap(args.config)
    try:
        print({"database": context.config.database.path, "status": "initialized"})
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
