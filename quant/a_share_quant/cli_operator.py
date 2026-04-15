"""operator lane CLI 入口。"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date

from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import OrderRequest, OrderSide
from a_share_quant.cli import (
    _DEFAULT_OPERATOR_CONFIG,
    _json_default,
    _parse_iso_date,
    _parse_order_side,
    _resolve_operator_config_path,
    _run_operator_cli_command,
)


def main_operator_snapshot(argv: list[str] | None = None) -> int:
    from a_share_quant.cli import _load_operator_snapshot

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
        from a_share_quant import cli as cli_module

        with cli_module.bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
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


def main_operator_submit_signal(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="提交 research signal_snapshot 到 paper/live operator 正式执行链")
    parser.add_argument("--config", default=_DEFAULT_OPERATOR_CONFIG, help="配置文件路径")
    parser.add_argument("--research-run-id", required=True, help="research signal_snapshot 运行标识；operator 写路径要求显式指定")
    parser.add_argument("--trade-date", default=None, help="目标交易日；为空时自动解析")
    parser.add_argument("--strategy-id", default=None, help="可选策略标识；为空时从 promotion package / config 推导")
    parser.add_argument("--requested-by", default=None, help="操作者标识")
    parser.add_argument("--idempotency-key", default=None, help="幂等键；重复提交将返回已有会话")
    parser.add_argument("--approved", action="store_true", help="当配置要求人工审批时显式确认")
    parser.add_argument("--account-id", default=None, help="可选账户 ID；缺省时使用 broker.account_id")
    parser.add_argument("--broker-client-factory", default=None, help="可选 broker client factory 路径")
    args = parser.parse_args(argv)
    resolved_trade_date = _parse_iso_date(args.trade_date)

    def _runner(resolved_config: str) -> int:
        from a_share_quant import cli as cli_module

        with cli_module.bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            result = workflow.submit_research_signal(
                research_run_id=args.research_run_id,
                trade_date=resolved_trade_date,
                command_source="cli.main_operator_submit_signal",
                requested_by=args.requested_by,
                idempotency_key=args.idempotency_key,
                approved=args.approved,
                account_id=args.account_id,
                strategy_id=args.strategy_id,
            )
            payload = {
                "intent": asdict(result.plan.intent),
                "deltas": [asdict(item) for item in result.plan.deltas],
                "planned_orders": [asdict(item) for item in result.plan.orders],
                "session": asdict(result.trade_session.summary),
                "orders": [asdict(item) for item in result.trade_session.orders],
                "fills": [asdict(item) for item in result.trade_session.fills],
                "events": [asdict(item) for item in result.trade_session.events],
                "replayed": result.trade_session.replayed,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
            return 0

    return _run_operator_cli_command(
        config_path=args.config,
        broker_client_factory=args.broker_client_factory,
        action_name="operator_submit_signal",
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
        from a_share_quant import cli as cli_module

        with cli_module.bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            if args.session_id:
                result = workflow.reconcile_session(args.session_id, requested_by=args.requested_by)
            else:
                result = workflow.reconcile_latest_recovery_required(requested_by=args.requested_by)
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
        from a_share_quant import cli as cli_module

        with cli_module.bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            if args.session_id:
                result = workflow.sync_session_events(args.session_id, requested_by=args.requested_by)
            else:
                result = workflow.sync_latest_open_session(requested_by=args.requested_by)
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
        from a_share_quant import cli as cli_module

        with cli_module.bootstrap_trade_operator_context(resolved_config, broker_client_factory=args.broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            summary = workflow.run_supervisor(
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
