"""research/backtest lane CLI 入口。"""
from __future__ import annotations

import argparse
import json

from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.cli import (
    _PACKAGED_SAMPLE_SENTINEL,
    _json_default,
    _parse_iso_date,
    _parse_symbols,
    _require_research_backtest_mode,
    _resolve_import_csv,
    _run_default_backtest,
)
from a_share_quant.workflows.research_workflow import load_research_task_specs


def main_app(argv: list[str] | None = None) -> int:
    """官方 research/backtest CLI 主入口。"""
    parser = argparse.ArgumentParser(description="A 股量化研究与交易工作站")
    parser.add_argument("--config", default="configs/app.yaml", help="配置文件路径")
    parser.add_argument("--csv", default="", help="显式导入后再回测的 CSV 路径；未提供时默认导入 sample_data")
    parser.add_argument("--import-csv", default="", help="兼容旧参数名，等价于 --csv")
    parser.add_argument("--use-existing-data", action="store_true", help="跳过导入，只使用数据库中已有行情")
    parser.add_argument("--research-run-id", default=None, help="可选 research signal_snapshot 运行标识；传入后回测将消费该研究信号")
    args = parser.parse_args(argv)

    import_csv_path = _resolve_import_csv(args)
    if args.use_existing_data and import_csv_path:
        raise SystemExit("--use-existing-data 与 --csv/--import-csv 不能同时提供")
    if not args.use_existing_data and import_csv_path is None:
        import_csv_path = _PACKAGED_SAMPLE_SENTINEL
    return _run_default_backtest(
        args.config,
        import_csv_path=import_csv_path,
        entrypoint="cli.main_app",
        research_signal_run_id=args.research_run_id,
    )


def main_research(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行 research workflow 正式入口")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument(
        "--artifact",
        default="experiment",
        choices=["dataset", "feature", "signal", "experiment", "experiment-batch", "recent-runs"],
    )
    parser.add_argument("--feature-name", default="momentum")
    parser.add_argument("--lookback", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=2)
    parser.add_argument("--start-date", default=None, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--symbols", default=None, help="逗号分隔 ts_code 列表")
    parser.add_argument("--csv", default=None, help="可选 CSV；提供后会先导入再执行 research")
    parser.add_argument("--batch-spec", default=None, help="JSON 文件；artifact=experiment-batch 时必填")
    parser.add_argument("--record", action="store_true", help="对 dataset/feature/signal 查询型 research 产物显式落正式 research_runs")
    args = parser.parse_args(argv)

    _require_research_backtest_mode(args.config)
    with bootstrap_data_context(args.config) as context:
        capabilities = context.research_capabilities()
        data_service = capabilities.require_data_service()
        if args.csv:
            data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
        workflow = capabilities.require_workflow_registry().get("workflow.research")
        common_kwargs = {
            "start_date": _parse_iso_date(args.start_date),
            "end_date": _parse_iso_date(args.end_date),
            "ts_codes": _parse_symbols(args.symbols),
        }
        if args.artifact == "dataset":
            payload = workflow.load_snapshot_summary(record=args.record, **common_kwargs)
        elif args.artifact == "feature":
            payload = workflow.run_feature_snapshot(feature_name=args.feature_name, lookback=args.lookback, record=args.record, **common_kwargs)
        elif args.artifact == "signal":
            payload = workflow.run_signal_snapshot(feature_name=args.feature_name, lookback=args.lookback, top_n=args.top_n, record=args.record, **common_kwargs)
        elif args.artifact == "experiment":
            payload = workflow.summarize_experiment(feature_name=args.feature_name, lookback=args.lookback, top_n=args.top_n, **common_kwargs)
        elif args.artifact == "experiment-batch":
            if not args.batch_spec:
                raise SystemExit("artifact=experiment-batch 时必须提供 --batch-spec")
            task_specs = load_research_task_specs(args.batch_spec)
            payload = workflow.summarize_experiment_batch(task_specs)
        else:
            payload = workflow.list_recent_runs()
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
        return 0
