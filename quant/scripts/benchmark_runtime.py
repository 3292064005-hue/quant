#!/usr/bin/env python3
"""Research/runtime 性能基线脚本。"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from a_share_quant.app.bootstrap import bootstrap_data_context


def _run_once(workflow, *, artifact: str, lookback: int, top_n: int) -> dict[str, Any]:
    started = time.perf_counter()
    if artifact == "dataset":
        payload = workflow.load_snapshot_summary()
    elif artifact == "feature":
        payload = workflow.run_feature_snapshot(feature_name="momentum", lookback=lookback)
    elif artifact == "signal":
        payload = workflow.run_signal_snapshot(feature_name="momentum", lookback=lookback, top_n=top_n)
    else:
        payload = workflow.summarize_experiment(feature_name="momentum", lookback=lookback, top_n=top_n)
    elapsed = time.perf_counter() - started
    return {"elapsed_seconds": elapsed, "payload": payload}


def main() -> int:
    parser = argparse.ArgumentParser(description="运行 research/runtime 性能基线")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--csv", default=None, help="可选 CSV；若当前库无数据且提供该参数，则先导入样本后再基线")
    parser.add_argument("--artifact", default="dataset", choices=["dataset", "feature", "signal", "experiment"])
    parser.add_argument("--lookback", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()

    if args.iterations <= 0:
        raise SystemExit("--iterations 必须大于 0")

    with bootstrap_data_context(args.config) as context:
        dataset_provider = context.require_provider_registry().get("provider.dataset")
        data_service = context.require_data_service()
        summary = dataset_provider.summarize_snapshot()
        if summary.total_bar_count == 0:
            if args.csv:
                data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
                summary = dataset_provider.summarize_snapshot()
            if summary.total_bar_count == 0:
                raise SystemExit("benchmark 失败：当前数据集为空；请先导入数据或通过 --csv 提供样本")
        workflow = context.require_workflow_registry().get("workflow.research")
        results = [
            _run_once(workflow, artifact=args.artifact, lookback=args.lookback, top_n=args.top_n)
            for _ in range(args.iterations)
        ]

    elapsed_samples = [round(item["elapsed_seconds"], 6) for item in results]
    payload = {
        "config": str(Path(args.config).resolve()),
        "artifact": args.artifact,
        "iterations": args.iterations,
        "elapsed_seconds": {
            "samples": elapsed_samples,
            "min": round(min(elapsed_samples), 6),
            "median": round(statistics.median(elapsed_samples), 6),
            "max": round(max(elapsed_samples), 6),
        },
        "dataset_summary": summary.to_dict(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
