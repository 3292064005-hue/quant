"""统一 CLI 入口。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from a_share_quant.app.bootstrap import (
    bootstrap,
    bootstrap_data_context,
    bootstrap_report_context,
    bootstrap_storage_context,
)
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.broker_client_loader import BrokerClientFactoryError, load_broker_client
from a_share_quant.core.runtime_checks import check_broker_runtime, check_data_provider_runtime, check_ui_runtime


def _parse_symbols(symbols: str | None) -> list[str] | None:
    if symbols is None:
        return None
    items = [item.strip() for item in symbols.split(",") if item.strip()]
    return items or None


def _load_optional_json_payload(path: str | None) -> dict | None:
    if not path:
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("broker payload 文件根节点必须是 JSON object")
    return payload


def _resolve_daily_run_csv(args: argparse.Namespace) -> str:
    return args.csv or args.import_csv or "sample_data/daily_bars.csv"


def _load_broker_client_for_cli(config, *, broker_client_factory: str | None) -> object | None:
    try:
        return load_broker_client(config, factory_path_override=broker_client_factory)
    except BrokerClientFactoryError as exc:
        raise SystemExit(str(exc)) from exc


def _require_research_backtest_mode(config_path: str) -> None:
    config = ConfigLoader.load(config_path)
    if config.app.runtime_mode != "research_backtest":
        raise SystemExit(
            f"当前命令只支持 research_backtest；收到 app.runtime_mode={config.app.runtime_mode}。"
            "请改用 research_backtest 配置，或为未来的 paper/live operator workflow 单独建入口。"
        )
    if config.broker.provider.lower() != "mock":
        raise SystemExit(
            f"research_backtest 模式下 broker.provider 必须为 mock；当前为 {config.broker.provider}。"
            "真实 broker 仅用于 runtime 校验或未来独立 paper/live orchestration。"
        )


def _run_default_backtest(
    config_path: str,
    *,
    csv_path: str,
    skip_import: bool,
    entrypoint: str = "cli.main_app",
) -> int:
    _require_research_backtest_mode(config_path)
    with bootstrap(config_path) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        backtest_service = context.require_backtest_service()
        if not skip_import:
            data_service.import_csv(csv_path, encoding=context.config.data.default_csv_encoding)
        strategy = strategy_service.build_default()
        result = backtest_service.run(strategy, entrypoint=entrypoint)
        print(
            {
                "strategy_id": result.strategy_id,
                "run_id": result.run_id,
                "order_count": result.order_count,
                "fill_count": result.fill_count,
                "metrics": result.metrics,
                "report": result.report_path,
                "data_lineage": {
                    "import_run_id": result.data_lineage.import_run_id,
                    "data_source": result.data_lineage.data_source,
                    "dataset_digest": result.data_lineage.dataset_digest,
                },
            }
        )
        return 0


def main_app(argv: list[str] | None = None) -> int:
    """官方 CLI 主入口。"""
    parser = argparse.ArgumentParser(description="A 股量化研究与交易工作站")
    parser.add_argument("--config", default="configs/app.yaml", help="配置文件路径")
    parser.add_argument("--csv", default="", help="回测前导入的 CSV 路径")
    parser.add_argument("--import-csv", default="", help="兼容旧参数名，等价于 --csv")
    parser.add_argument("--skip-import", action="store_true", help="跳过 CSV 导入，直接使用数据库中已有行情")
    args = parser.parse_args(argv)
    return _run_default_backtest(
        args.config,
        csv_path=_resolve_daily_run_csv(args),
        skip_import=args.skip_import,
        entrypoint="cli.main_app",
    )


def main_check_runtime(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行时健康检查")
    parser.add_argument("--config", default="configs/app.yaml", help="配置文件路径")
    parser.add_argument("--check-ui", action="store_true", help="同时检查 PySide6 UI 运行条件")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="只要当前检查项存在失败即返回非零退出码；不等价于真实 broker 完整可运行性认证",
    )
    parser.add_argument(
        "--broker-sample-payload-file",
        default=None,
        help="可选 JSON 文件，根节点可包含 account/positions/fill，用于验证 broker 领域映射契约",
    )
    parser.add_argument(
        "--broker-client-factory",
        default=None,
        help="可选 broker client factory 路径；提供后会尝试构造真实客户端并校验方法契约",
    )
    args = parser.parse_args(argv)

    config = ConfigLoader.load(args.config)
    results = []
    token_present = bool(config.data.tushare_token or os.getenv(config.data.tushare_token_env))
    results.append(check_data_provider_runtime(config.data.provider, token_present=token_present).to_dict())
    sample_payloads = _load_optional_json_payload(args.broker_sample_payload_file)
    broker_client = _load_broker_client_for_cli(config, broker_client_factory=args.broker_client_factory)
    results.append(
        check_broker_runtime(
            config.broker.provider,
            endpoint=config.broker.endpoint,
            account_id=config.broker.account_id,
            injected_client=broker_client,
            sample_payloads=sample_payloads,
            allow_shallow_client_check=True,
            strict_contract_mapping=config.broker.strict_contract_mapping,
            runtime_mode=config.app.runtime_mode,
        ).to_dict()
    )
    if args.check_ui:
        results.append(check_ui_runtime().to_dict())

    payload = {
        "config": str(Path(args.config).resolve()),
        "results": results,
        "ok": all(item["ok"] for item in results),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] or not args.strict else 2


def main_daily_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行默认策略回测")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--csv", default="sample_data/daily_bars.csv")
    parser.add_argument("--skip-import", action="store_true", help="跳过 CSV 导入，直接使用数据库中已有行情")
    args = parser.parse_args(argv)
    return _run_default_backtest(
        args.config,
        csv_path=args.csv,
        skip_import=args.skip_import,
        entrypoint="cli.main_daily_run",
    )


def main_generate_report(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="基于数据库中的回测结果重建报告")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--run-id", default=None, help="指定要重建报告的回测 run_id；缺省时使用最近一次已完成运行")
    args = parser.parse_args(argv)
    with bootstrap_report_context(args.config) as context:
        path = context.require_report_service().rebuild_backtest_report(run_id=args.run_id)
        print({"report": str(Path(path))})
        return 0


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
        data_service = context.require_data_service()
        provider = (args.provider or context.config.data.provider).lower()
        if provider == "csv":
            if not args.csv:
                raise SystemExit("provider=csv 时必须提供 --csv")
            bundle = data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
        else:
            if not args.start_date or not args.end_date:
                raise SystemExit("在线同步时必须同时提供 --start-date 和 --end-date")
            bundle = data_service.sync_from_provider(
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
                "degradation_flags": bundle.degradation_flags,
                "warnings": bundle.warnings,
                "import_run_id": data_service.last_import_run_id,
                "status": "imported",
            }
        )
        return 0


def main_launch_ui(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动桌面原型 UI（未接交易主链）")
    parser.add_argument("--config", default="configs/app.yaml", help="配置文件路径")
    parser.add_argument(
        "--broker-client-factory",
        default=None,
        help="可选 broker client factory 路径；提供后会尝试构造真实客户端并校验方法契约",
    )
    args = parser.parse_args(argv)

    ui_check = check_ui_runtime()
    if not ui_check.ok:
        raise SystemExit(ui_check.message)

    config = ConfigLoader.load(args.config)
    token_present = bool(config.data.tushare_token or os.getenv(config.data.tushare_token_env))
    broker_client = _load_broker_client_for_cli(config, broker_client_factory=args.broker_client_factory)
    runtime_results = [
        check_data_provider_runtime(config.data.provider, token_present=token_present).to_dict(),
        check_broker_runtime(
            config.broker.provider,
            endpoint=config.broker.endpoint,
            account_id=config.broker.account_id,
            injected_client=broker_client,
            allow_shallow_client_check=True,
            strict_contract_mapping=config.broker.strict_contract_mapping,
        ).to_dict(),
    ]

    from PySide6.QtWidgets import QApplication

    from a_share_quant.ui.main_window import build_main_window

    app = QApplication([])
    window = build_main_window(config=config, runtime_results=runtime_results)
    window.show()
    return app.exec()
