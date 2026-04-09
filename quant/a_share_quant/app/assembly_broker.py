"""broker 与执行引擎装配。"""
from __future__ import annotations

from typing import Any

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.app.assembly_core import AssemblyValidationError
from a_share_quant.core.broker_client_loader import load_broker_client
from a_share_quant.engines.execution_engine import ExecutionEngine
from a_share_quant.engines.execution_registry import build_builtin_execution_registry


def build_broker(
    config,
    broker_clients: dict[str, Any] | None = None,
    *,
    broker_client_factory: str | None = None,
) -> BrokerBase | LiveBrokerPort:
    """按配置构建 broker 适配器。"""
    provider = config.broker.provider.lower()
    runtime_mode = config.app.runtime_mode
    clients = dict(broker_clients or {})

    if runtime_mode == "research_backtest":
        if provider != "mock":
            raise AssemblyValidationError(
                f"research_backtest 模式下 broker.provider 必须为 mock；当前为 {config.broker.provider}。"
                "真实 broker 仅用于 runtime 校验或独立 paper/live orchestration。"
            )
        return MockBroker(config.backtest.initial_cash, config.backtest.fee_bps, config.backtest.tax_bps)

    if provider in {"qmt", "ptrade"} and provider not in clients:
        loaded_client = load_broker_client(config, provider=provider, factory_path_override=broker_client_factory)
        if loaded_client is not None:
            clients[provider] = loaded_client
    if provider == "qmt":
        if not config.broker.endpoint or not config.broker.account_id:
            raise AssemblyValidationError("QMT 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("qmt")
        if client is None:
            raise AssemblyValidationError(
                "当前工程未内置 QMT 运行时；请通过 bootstrap(..., broker_clients={'qmt': client}) 注入客户端，"
                "或在 broker.client_factory / --broker-client-factory 中提供工厂路径"
            )
        return QMTAdapter(
            client,
            timeout_seconds=config.broker.operation_timeout_seconds,
            strict_contract_mapping=config.broker.strict_contract_mapping,
        )
    if provider == "ptrade":
        if not config.broker.endpoint or not config.broker.account_id:
            raise AssemblyValidationError("PTrade 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("ptrade")
        if client is None:
            raise AssemblyValidationError(
                "当前工程未内置 PTrade 运行时；请通过 bootstrap(..., broker_clients={'ptrade': client}) 注入客户端，"
                "或在 broker.client_factory / --broker-client-factory 中提供工厂路径"
            )
        return PTradeAdapter(
            client,
            timeout_seconds=config.broker.operation_timeout_seconds,
            strict_contract_mapping=config.broker.strict_contract_mapping,
        )
    if provider == "mock":
        raise AssemblyValidationError(
            f"app.runtime_mode={runtime_mode} 时不允许使用 mock broker；请切回 research_backtest 或配置真实 broker"
        )
    raise AssemblyValidationError(f"不支持的 broker.provider: {config.broker.provider}")



def build_execution_engine(config, broker: BrokerBase) -> ExecutionEngine:
    """根据配置构建正式执行引擎。"""
    execution_cfg = config.backtest.execution
    registry = build_builtin_execution_registry()
    slippage_model = registry.build("slippage", execution_cfg.slippage_model, config)
    fill_model = registry.build("fill", execution_cfg.fill_model, config)
    fee_model = registry.build("fee", execution_cfg.fee_model, config)
    tax_model = registry.build("tax", execution_cfg.tax_model, config)

    return ExecutionEngine(
        broker,
        slippage_model=slippage_model,
        fill_model=fill_model,
        fee_model=fee_model,
        tax_model=tax_model,
        slippage_bps=config.backtest.slippage_bps,
    )
