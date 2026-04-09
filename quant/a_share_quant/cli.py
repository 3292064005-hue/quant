"""统一 CLI 入口。"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Callable, cast

from a_share_quant.app.bootstrap import (
    AssemblyValidationError,
    bootstrap,
    bootstrap_data_context,
    bootstrap_operator_context,
    bootstrap_report_context,
    bootstrap_storage_context,
    bootstrap_trade_operator_context,
)
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.broker_client_loader import BrokerClientFactoryError, load_broker_client
from a_share_quant.core.utils import new_id
from a_share_quant.core.runtime_checks import (
    check_broker_runtime,
    check_data_provider_runtime,
    check_ui_runtime,
    summarize_runtime_results,
)
from a_share_quant.plugins.plugin_manager import PluginManager
from a_share_quant.services.run_query_service import RunQueryService
from a_share_quant.services.ui_read_models import build_ui_snapshot_projection
from a_share_quant.workflows.research_workflow import load_research_task_specs

_DEFAULT_SAMPLE_CSV = "sample_data/daily_bars.csv"
_DEFAULT_OPERATOR_CONFIG = "configs/operator_paper_trade_demo.yaml"


def _resolve_operator_config_path(config_path: str | None) -> str:
    """解析 operator CLI 的默认配置路径。"""
    resolved = (config_path or "").strip()
    return resolved or _DEFAULT_OPERATOR_CONFIG


def _load_config_or_exit(config_path: str):
    """加载配置，并把解析失败收口为 CLI 级退出信息。"""
    try:
        return ConfigLoader.load(config_path)
    except Exception as exc:  # pragma: no cover - 透传配置解析错误文本
        raise SystemExit(f"加载配置失败: {config_path}；{exc}") from exc


def _validate_operator_cli_config(config_path: str, *, broker_client_factory: str | None) -> None:
    """在进入 operator 装配前执行显式 preflight。

    Args:
        config_path: operator 配置路径。
        broker_client_factory: CLI 层显式覆盖的 client factory。

    Raises:
        SystemExit: 当 runtime lane、broker provider 或 broker client factory 不满足 operator 命令要求时抛出。

    Boundary Behavior:
        - 不依赖后续 bootstrap 异常文本，优先给出可操作的 CLI 级错误；
        - 默认 operator 配置指向仓内自带 acceptance profile，真实 broker 场景仍可通过 ``--config`` /
          ``--broker-client-factory`` 覆盖；
        - 仅在 paper/live lane 放行，避免 research_backtest + mock 误入 operator 写路径。
    """
    config = _load_config_or_exit(config_path)
    if config.app.runtime_mode not in {"paper_trade", "live_trade"}:
        raise SystemExit(
            f"operator 命令仅支持 paper_trade/live_trade；当前 app.runtime_mode={config.app.runtime_mode}。"
            f"请改用 {_DEFAULT_OPERATOR_CONFIG} 或显式传入 paper/live 配置。"
        )
    if config.broker.provider.lower() == "mock":
        raise SystemExit(
            f"operator 命令不支持 broker.provider=mock；当前为 {config.broker.provider}。"
            f"请改用 {_DEFAULT_OPERATOR_CONFIG} 或显式传入真实 broker 配置。"
        )
    if not ((broker_client_factory or "").strip() or (config.broker.client_factory or "").strip()):
        raise SystemExit(
            "当前 operator 配置未提供 broker client factory，无法装配正式 broker 适配器。"
            f"可传 --broker-client-factory，或直接使用仓内自带 acceptance profile: {_DEFAULT_OPERATOR_CONFIG}"
        )


def _run_operator_cli_command(
    *,
    config_path: str | None,
    broker_client_factory: str | None,
    action_name: str,
    runner: Callable[[str], int],
) -> int:
    """统一执行 operator CLI，并把装配/边界异常收口为干净退出。"""
    resolved_config = _resolve_operator_config_path(config_path)
    _validate_operator_cli_config(resolved_config, broker_client_factory=broker_client_factory)
    try:
        return runner(resolved_config)
    except BrokerClientFactoryError as exc:
        raise SystemExit(f"{action_name} 失败：{exc}") from exc
    except AssemblyValidationError as exc:
        raise SystemExit(f"{action_name} 失败：{exc}") from exc
    except ValueError as exc:
        raise SystemExit(f"{action_name} 失败：{exc}") from exc
    except RuntimeError as exc:
        raise SystemExit(f"{action_name} 失败：{exc}") from exc


def _parse_symbols(symbols: str | None) -> list[str] | None:
    if symbols is None:
        return None
    items = [item.strip() for item in symbols.split(",") if item.strip()]
    return items or None


def _parse_iso_date(raw: str | None) -> date | None:
    return date.fromisoformat(raw) if raw else None


def _parse_order_side(raw: str) -> str:
    normalized = str(raw).strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise SystemExit(f"side 仅支持 BUY/SELL；收到 {raw}")
    return normalized


def _load_optional_json_payload(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("broker payload 文件根节点必须是 JSON object")
    return payload


def _resolve_import_csv(args: argparse.Namespace) -> str | None:
    if getattr(args, "csv", None):
        return str(args.csv)
    if getattr(args, "import_csv", None):
        return str(args.import_csv)
    return None


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _load_broker_client_for_cli(config, *, broker_client_factory: str | None) -> object | None:
    try:
        return load_broker_client(config, factory_path_override=broker_client_factory)
    except BrokerClientFactoryError as exc:
        raise SystemExit(str(exc)) from exc


def _build_runtime_results(
    config,
    *,
    broker_client_factory: str | None = None,
    sample_payloads: dict[str, Any] | None = None,
    include_ui: bool = False,
) -> list[dict[str, Any]]:
    """基于统一规则构造 CLI/UI 共用的运行时检查结果。"""
    token_present = bool(config.data.tushare_token or os.getenv(config.data.tushare_token_env))
    broker_client = _load_broker_client_for_cli(config, broker_client_factory=broker_client_factory)
    results = [check_data_provider_runtime(config.data.provider, token_present=token_present).to_dict()]
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
    if include_ui:
        results.append(check_ui_runtime().to_dict())
    return results


def _load_ui_operations_snapshot(config_path: str, *, runtime_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """加载桌面只读运营摘要。

    返回值同时保留原始 snapshot 与 ``ui_*`` 投影字段，供桌面层稳定消费。
    """
    snapshot: dict[str, Any] = {
        "supported_runtime_scope": {
            "official": ["research_backtest + mock"],
            "boundary_only": ["paper_trade + qmt", "paper_trade + ptrade", "live_trade + qmt", "live_trade + ptrade"],
        }
    }
    with bootstrap_data_context(config_path) as context:
        workflow_registry = context.require_workflow_registry() if context.workflow_registry is not None else None
        query_service = RunQueryService(
            backtest_run_repository=context.backtest_run_repository,
            order_repository=context.order_repository,
            audit_repository=context.audit_repository,
            data_import_repository=context.data_import_repository,
            research_run_repository=context.research_run_repository,
            execution_session_repository=context.execution_session_repository,
            account_repository=context.account_repository,
        )
        plugin_manager: PluginManager | None = context.plugin_manager
        provider_details = (
            [entry.to_summary() for entry in context.require_provider_registry().list_entries()]
            if context.provider_registry is not None
            else []
        )
        workflow_details = [entry.to_summary() for entry in workflow_registry.list_entries()] if workflow_registry is not None else []
        snapshot["config_runtime"] = {
            "runtime_mode": context.config.app.runtime_mode,
            "broker_provider": context.config.broker.provider,
        }
        snapshot.update(query_service.build_latest_snapshot())
        snapshot["available_providers"] = [item["name"] for item in provider_details]
        snapshot["available_provider_details"] = provider_details
        snapshot["available_workflows"] = [item["name"] for item in workflow_details]
        snapshot["available_workflow_details"] = workflow_details
        snapshot["installed_plugins"] = [descriptor.name for descriptor in plugin_manager.descriptors()] if plugin_manager is not None else []
        snapshot["installed_plugin_details"] = [asdict(descriptor) for descriptor in plugin_manager.descriptors()] if plugin_manager is not None else []
        snapshot["plugin_lifecycle_events"] = plugin_manager.lifecycle_events()[-20:] if plugin_manager is not None else []
        snapshot["registered_components"] = (
            context.require_component_registry().list_component_summaries() if context.component_registry is not None else []
        )
        snapshot.update(
            build_ui_snapshot_projection(
                runtime_results=runtime_results or [],
                available_provider_details=provider_details,
                available_workflow_details=workflow_details,
                recent_research_runs=snapshot.get("recent_research_runs", []),
            )
        )
    return snapshot




def _require_ui_official_runtime_scope(config) -> None:
    """限制桌面只读面板仅在官方支持的 research_backtest + mock 组合下启动。"""
    if config.app.runtime_mode != "research_backtest":
        raise SystemExit(
            f"桌面只读运营面板当前仅支持 research_backtest；收到 app.runtime_mode={config.app.runtime_mode}。"
            "paper/live 请改用 operator_snapshot 与正式 operator 命令入口。"
        )
    if config.broker.provider.lower() != "mock":
        raise SystemExit(
            f"桌面只读运营面板当前仅支持 broker.provider=mock；收到 {config.broker.provider}。"
            "真实 broker 的只读检查请使用 operator_snapshot。"
        )

def _load_operator_snapshot(config_path: str, *, broker_client_factory: str | None = None) -> dict[str, Any]:
    """加载 paper/live lane 的只读 operator snapshot。"""
    config = ConfigLoader.load(config_path)
    runtime_results = _build_runtime_results(config, broker_client_factory=broker_client_factory)
    with bootstrap_operator_context(config_path, broker_client_factory=broker_client_factory) as context:
        query_service = RunQueryService(
            backtest_run_repository=context.backtest_run_repository,
            order_repository=context.order_repository,
            audit_repository=context.audit_repository,
            data_import_repository=context.data_import_repository,
            research_run_repository=context.research_run_repository,
            execution_session_repository=context.execution_session_repository,
            account_repository=context.account_repository,
        )
        broker = context.require_broker()
        return query_service.build_operator_snapshot(
            broker=broker,
            runtime_mode=context.config.app.runtime_mode,
            broker_provider=context.config.broker.provider,
            default_account_id=context.config.broker.account_id or None,
            allowed_account_ids=context.config.broker.allowed_account_ids,
            event_source_mode=context.config.broker.event_source_mode,
            supervisor_config={
                "scan_interval_seconds": context.config.operator.supervisor_scan_interval_seconds,
                "lease_seconds": context.config.operator.supervisor_lease_seconds,
                "heartbeat_interval_seconds": context.config.operator.supervisor_heartbeat_interval_seconds,
                "idle_timeout_seconds": context.config.operator.supervisor_idle_timeout_seconds,
                "max_sessions_per_pass": context.config.operator.supervisor_max_sessions_per_pass,
            },
            runtime_checks=runtime_results,
            capability_summary=summarize_runtime_results(runtime_results),
        )


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
    import_csv_path: str | None,
    entrypoint: str = "cli.main_app",
    research_signal_run_id: str | None = None,
) -> int:
    """执行默认研究回测。

    Args:
        config_path: 主配置路径。
        import_csv_path: 显式导入的 CSV；为空时仅消费数据库现有数据，不再隐式导入。
        entrypoint: 入口标识，写入 run manifest。
        research_signal_run_id: 可选 research signal_snapshot 运行标识。

    Raises:
        SystemExit: 当 runtime lane 非 research_backtest 或 broker 非 mock 时抛出。
    """
    _require_research_backtest_mode(config_path)
    with bootstrap(config_path) as context:
        data_service = context.require_data_service()
        strategy_service = context.require_strategy_service()
        if import_csv_path:
            data_service.import_csv(import_csv_path, encoding=context.config.data.default_csv_encoding)
        strategy = strategy_service.build_default(research_signal_run_id=research_signal_run_id)
        workflow = context.require_workflow_registry().get("workflow.backtest")
        result = workflow.run_default(strategy, entrypoint=entrypoint)
        print(
            {
                "strategy_id": result.strategy_id,
                "run_id": result.run_id,
                "order_count": result.order_count,
                "fill_count": result.fill_count,
                "metrics": result.metrics,
                "report": result.report_path,
                "data_lineage": {
                    "dataset_version_id": result.data_lineage.dataset_version_id,
                    "import_run_id": result.data_lineage.import_run_id,
                    "import_run_ids": result.data_lineage.import_run_ids,
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
    parser.add_argument("--csv", default="", help="显式导入后再回测的 CSV 路径；未提供时默认导入 sample_data")
    parser.add_argument("--import-csv", default="", help="兼容旧参数名，等价于 --csv")
    parser.add_argument("--use-existing-data", action="store_true", help="跳过导入，只使用数据库中已有行情")
    parser.add_argument("--research-run-id", default=None, help="可选 research signal_snapshot 运行标识；传入后回测将消费该研究信号")
    args = parser.parse_args(argv)

    import_csv_path = _resolve_import_csv(args)
    if args.use_existing_data and import_csv_path:
        raise SystemExit("--use-existing-data 与 --csv/--import-csv 不能同时提供")
    if not args.use_existing_data and import_csv_path is None:
        import_csv_path = _DEFAULT_SAMPLE_CSV
    return _run_default_backtest(
        args.config,
        import_csv_path=import_csv_path,
        entrypoint="cli.main_app",
        research_signal_run_id=args.research_run_id,
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
    sample_payloads = _load_optional_json_payload(args.broker_sample_payload_file)
    results = _build_runtime_results(
        config,
        broker_client_factory=args.broker_client_factory,
        sample_payloads=sample_payloads,
        include_ui=args.check_ui,
    )

    capability_summary = summarize_runtime_results(results)
    payload = {
        "config": str(Path(args.config).resolve()),
        "supported_runtime_scope": {
            "official": ["research_backtest + mock"],
            "boundary_only": ["paper_trade + qmt", "paper_trade + ptrade", "live_trade + qmt", "live_trade + ptrade"],
        },
        "results": results,
        "capability_summary": capability_summary,
        "ok": all(item["ok"] for item in results),
        "operable_ok": capability_summary["operable_ok"],
    }
    payload["strict_ok"] = payload["ok"] and payload["operable_ok"]
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if (not args.strict or payload["strict_ok"]) else 2


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
        entrypoint="cli.main_daily_run",
        research_signal_run_id=args.research_run_id,
    )


def main_generate_report(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="基于数据库中的回测结果重建报告")
    parser.add_argument("--config", default="configs/app.yaml")
    parser.add_argument("--run-id", default=None, help="指定要重建报告的回测 run_id；缺省时使用最近一次可重建运行（COMPLETED / ENGINE_COMPLETED / ARTIFACT_EXPORT_FAILED）")
    args = parser.parse_args(argv)
    with bootstrap_report_context(args.config) as context:
        workflow = context.require_workflow_registry().get("workflow.report")
        path = workflow.rebuild(run_id=args.run_id)
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
    args = parser.parse_args(argv)

    _require_research_backtest_mode(args.config)
    with bootstrap_data_context(args.config) as context:
        data_service = context.require_data_service()
        if args.csv:
            data_service.import_csv(args.csv, encoding=context.config.data.default_csv_encoding)
        workflow = context.require_workflow_registry().get("workflow.research")
        common_kwargs = {
            "start_date": _parse_iso_date(args.start_date),
            "end_date": _parse_iso_date(args.end_date),
            "ts_codes": _parse_symbols(args.symbols),
        }
        if args.artifact == "dataset":
            payload = workflow.load_snapshot_summary(**common_kwargs)
        elif args.artifact == "feature":
            payload = workflow.run_feature_snapshot(feature_name=args.feature_name, lookback=args.lookback, **common_kwargs)
        elif args.artifact == "signal":
            payload = workflow.run_signal_snapshot(feature_name=args.feature_name, lookback=args.lookback, top_n=args.top_n, **common_kwargs)
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


def main_operator_snapshot(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="输出 paper/live lane 的只读 operator snapshot")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument(
        "--broker-client-factory",
        default=None,
        help="可选 broker client factory 路径；提供后会尝试构造真实客户端并校验方法契约",
    )
    args = parser.parse_args(argv)

    def _runner(resolved_config: str) -> int:
        payload = _load_operator_snapshot(resolved_config, broker_client_factory=args.broker_client_factory)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
        return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_snapshot",
        runner=_runner,
    )


def main_operator_submit_order(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="提交 paper/live lane 的正式 operator 订单批次")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument("--symbol", required=True, help="证券代码，如 600000.SH")
    parser.add_argument("--side", required=True, help="BUY 或 SELL")
    parser.add_argument("--price", type=float, required=True, help="委托价格")
    parser.add_argument("--quantity", type=int, required=True, help="委托数量")
    parser.add_argument("--reason", default="operator_submit", help="委托原因")
    parser.add_argument("--trade-date", default=None, help="交易日期，默认为今天")
    parser.add_argument("--requested-by", default=None, help="操作者标识")
    parser.add_argument("--idempotency-key", default=None, help="幂等键；重复提交将返回已有会话")
    parser.add_argument("--approved", action="store_true", help="当配置要求人工审批时显式确认")
    parser.add_argument("--account-id", default=None, help="可选账户 ID；缺省时使用 broker.account_id")
    parser.add_argument("--broker-client-factory", default=None, help="可选 broker client factory 路径")
    args = parser.parse_args(argv)

    from a_share_quant.domain.models import OrderRequest, OrderSide

    trade_date = _parse_iso_date(args.trade_date) or date.today()
    order = OrderRequest(
        order_id=(
            f"operator_{args.symbol}_{trade_date.isoformat()}_{args.side.lower()}_{int(args.quantity)}_{new_id('order')}"
        ),
        trade_date=trade_date,
        strategy_id="operator.manual",
        ts_code=args.symbol,
        side=OrderSide(_parse_order_side(args.side)),
        price=float(args.price),
        quantity=int(args.quantity),
        reason=args.reason,
    )

    def _runner(resolved_config: str) -> int:
        with bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            result = workflow.submit_orders(
                [order],
                command_source="cli.main_operator_submit_order",
                requested_by=args.requested_by,
                idempotency_key=args.idempotency_key,
                approved=args.approved,
                account_id=args.account_id,
            )
            payload = {
                "session": asdict(result.summary),
                "orders": [asdict(item) for item in result.orders],
                "fills": [asdict(item) for item in result.fills],
                "events": [asdict(item) for item in result.events],
                "replayed": result.replayed,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
            return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_submit_order",
        runner=_runner,
    )


def main_operator_reconcile_session(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="恢复 paper/live lane 中待 reconciliation 的 operator 交易会话")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument("--session-id", default=None, help="显式指定待恢复的会话 ID；不提供时恢复最近一个 RUNNING/RECOVERY_REQUIRED 会话")
    parser.add_argument("--requested-by", default=None, help="操作者标识")
    parser.add_argument("--broker-client-factory", default=None, help="可选 broker client factory 路径")
    args = parser.parse_args(argv)

    def _runner(resolved_config: str) -> int:
        with bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            orchestrator = context.require_trade_orchestrator_service()
            if args.session_id:
                result = orchestrator.reconcile_session(args.session_id, requested_by=args.requested_by)
            else:
                result = orchestrator.reconcile_latest_recovery_required(requested_by=args.requested_by)
            payload = {
                "session": asdict(result.summary),
                "orders": [asdict(item) for item in result.orders],
                "fills": [asdict(item) for item in result.fills],
                "events": [asdict(item) for item in result.events],
                "replayed": result.replayed,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
            return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_reconcile_session",
        runner=_runner,
    )





def main_operator_sync_session(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="轮询 paper/live lane 的 broker 事件并推进本地交易会话")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument("--session-id", default=None, help="显式指定待同步的会话 ID；不提供时同步最近一个 RUNNING/RECOVERY_REQUIRED 会话")
    parser.add_argument("--requested-by", default=None, help="操作者标识")
    parser.add_argument("--broker-client-factory", default=None, help="可选 broker client factory 路径")
    args = parser.parse_args(argv)

    def _runner(resolved_config: str) -> int:
        with bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            if args.session_id:
                result = workflow.sync_session_events(args.session_id, requested_by=args.requested_by)
            else:
                result = context.require_trade_orchestrator_service().sync_latest_open_session(requested_by=args.requested_by)
            payload = {
                "session": asdict(result.summary),
                "orders": [asdict(item) for item in result.orders],
                "fills": [asdict(item) for item in result.fills],
                "events": [asdict(item) for item in result.events],
                "replayed": result.replayed,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
            return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_sync_session",
        runner=_runner,
    )


def main_operator_run_supervisor(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动跨进程 operator supervisor，持续推进 open 交易会话")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument("--requested-by", default=None, help="操作者/监督者标识")
    parser.add_argument("--owner-id", default=None, help="显式指定 supervisor owner id；缺省时自动生成")
    parser.add_argument("--account-id", default=None, help="仅监督指定 account_id 的 open session")
    parser.add_argument("--session-id", default=None, help="仅监督指定 session_id")
    parser.add_argument("--max-loops", type=int, default=1, help="最多运行的 supervisor 扫描轮数；默认 1")
    parser.add_argument("--stop-when-idle", action="store_true", help="当没有 open session 可领取时立即退出")
    parser.add_argument("--broker-client-factory", default=None, help="可选 broker client factory 路径")
    args = parser.parse_args(argv)

    def _runner(resolved_config: str) -> int:
        with bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            supervisor = context.require_operator_supervisor_service()
            summary = supervisor.run_loop(
                requested_by=args.requested_by,
                owner_id=args.owner_id,
                account_id=args.account_id,
                session_id=args.session_id,
                max_loops=args.max_loops,
                stop_when_idle=args.stop_when_idle,
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, default=_json_default))
            return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_run_supervisor",
        runner=_runner,
    )


def main_launch_ui(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动桌面只读运营面板（未接交易主链写操作）")
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
    _require_ui_official_runtime_scope(config)
    runtime_results = _build_runtime_results(
        config,
        broker_client_factory=args.broker_client_factory,
        include_ui=False,
    )
    operations_snapshot = _load_ui_operations_snapshot(args.config, runtime_results=runtime_results)

    from PySide6.QtWidgets import QApplication

    from a_share_quant.ui.main_window import build_main_window

    app = QApplication([])
    window = cast(Any, build_main_window(config=config, runtime_results=runtime_results, operations_snapshot=operations_snapshot))
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main_app())
