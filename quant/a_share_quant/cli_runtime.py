"""runtime / UI lane CLI 入口。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from a_share_quant.app.bootstrap import bootstrap_data_context
from a_share_quant.cli import (
    _build_runtime_results,
    _load_operator_snapshot,
    _load_ui_operations_snapshot,
    _load_optional_json_payload,
    _require_ui_official_runtime_scope,
)
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.runtime_checks import check_market_storage_runtime, check_ui_runtime, summarize_runtime_results


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
        config_path=args.config,
    )
    try:
        with bootstrap_data_context(args.config) as context:
            results.append(
                check_market_storage_runtime(
                    context.config.data,
                    context.market_repository,
                    context.data_import_repository,
                    context.dataset_version_repository,
                ).to_dict()
            )
    except Exception as exc:
        results.append(
            {
                "name": "market_data",
                "ok": False,
                "message": f"市场数据运行检查失败: {exc}",
                "details": {"error": str(exc)},
                "capability": {
                    "config_ok": False,
                    "boundary_ok": False,
                    "client_contract_ok": False,
                    "operable_ok": False,
                    "acceptance_ok": False,
                    "recovery_ok": False,
                    "readiness_level": "config_validated",
                },
            }
        )

    capability_summary = summarize_runtime_results(results, include_extended=True)
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
    payload["strict_ok"] = payload["ok"] and capability_summary.get("required_readiness_ok", False)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if (not args.strict or payload["strict_ok"]) else 2


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
        config_path=args.config,
    )
    operations_snapshot = _load_ui_operations_snapshot(args.config, runtime_results=runtime_results)

    from PySide6.QtWidgets import QApplication

    from a_share_quant.ui.main_window import build_main_window

    app = QApplication([])
    window = build_main_window(config=config, operations_snapshot=operations_snapshot)
    window.show()
    return app.exec()
