"""应用启动组装。"""
from __future__ import annotations

from typing import Any

from a_share_quant.adapters.broker.base import BrokerBase, LiveBrokerPort
from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.app.context import AppContext
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.broker_client_loader import load_broker_client
from a_share_quant.core.logging_utils import configure_logging
from a_share_quant.core.schema_loader import load_schema_sql
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.risk_service import RiskService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.storage.sqlite_store import SQLiteStore


def bootstrap(
    config_path: str,
    broker_clients: dict[str, Any] | None = None,
    *,
    broker_client_factory: str | None = None,
) -> AppContext:
    """构建完整研究回测上下文。"""
    return _build_context(
        config_path,
        include_data_service=True,
        include_strategy_service=True,
        include_report_service=True,
        include_backtest_service=True,
        include_broker=True,
        broker_clients=broker_clients,
        broker_client_factory=broker_client_factory,
    )


def bootstrap_storage_context(config_path: str) -> AppContext:
    """仅构建存储与 repository 上下文。"""
    return _build_context(config_path)


def bootstrap_data_context(config_path: str) -> AppContext:
    """构建数据导入/同步所需上下文，不注入 broker。"""
    return _build_context(config_path, include_data_service=True)


def bootstrap_report_context(config_path: str) -> AppContext:
    """构建报表重建所需上下文，不注入 broker。"""
    return _build_context(config_path, include_report_service=True)


def _build_context(
    config_path: str,
    *,
    include_data_service: bool = False,
    include_strategy_service: bool = False,
    include_report_service: bool = False,
    include_backtest_service: bool = False,
    include_broker: bool = False,
    broker_clients: dict[str, Any] | None = None,
    broker_client_factory: str | None = None,
) -> AppContext:
    """按最小依赖装配上下文。"""
    config = ConfigLoader.load(config_path)
    configure_logging(config.app.logs_dir)
    store = SQLiteStore(config.database.path)
    context: AppContext | None = None
    try:
        store.init_schema(load_schema_sql())
        context = _build_base_context(config, store)

        if include_backtest_service and config.app.runtime_mode != "research_backtest":
            raise ValueError(
                f"当前工程的 BacktestService 仅支持 research_backtest；收到 app.runtime_mode={config.app.runtime_mode}"
            )

        if include_data_service or include_backtest_service:
            context.data_service = DataService(
                context.market_repository,
                config.data,
                data_import_repository=context.data_import_repository,
            )

        if include_strategy_service or include_backtest_service:
            context.strategy_service = StrategyService(config, context.strategy_repository)

        if include_report_service or include_backtest_service:
            context.report_service = ReportService(
                config.data.reports_dir,
                config.backtest.report_name_template,
                account_repository=context.account_repository,
                order_repository=context.order_repository,
                run_repository=context.backtest_run_repository,
                market_repository=context.market_repository,
                annual_trading_days=config.backtest.metrics.annual_trading_days,
                risk_free_rate=config.backtest.metrics.risk_free_rate,
            )

        if include_broker:
            context.broker = _build_broker(
                config,
                broker_clients=broker_clients,
                broker_client_factory=broker_client_factory,
            )
            context.broker.connect()

        if include_backtest_service:
            if context.data_service is None or context.strategy_service is None or context.report_service is None or context.broker is None:
                raise RuntimeError("回测上下文装配失败：缺少 DataService/StrategyService/ReportService/Broker")
            risk_engine = RiskService(config.risk, config.backtest).build_engine()
            portfolio_engine = PortfolioEngine(
                enforce_lot_size=config.risk.rules.enforce_lot_size,
                rebalance_mode=config.backtest.rebalance_mode,
            )
            backtest_engine = BacktestEngine(
                context.broker,
                risk_engine,
                portfolio_engine,
                context.order_repository,
                context.account_repository,
                context.audit_repository,
                context.backtest_run_repository,
                store=store,
                initial_cash=config.backtest.initial_cash,
                annual_trading_days=config.backtest.metrics.annual_trading_days,
                risk_free_rate=config.backtest.metrics.risk_free_rate,
                slippage_bps=config.backtest.slippage_bps,
                missing_price_policy=config.backtest.valuation.missing_price_policy,
            )
            context.backtest_service = BacktestService(
                config,
                backtest_engine,
                context.report_service,
                context.backtest_run_repository,
                data_service=context.data_service,
            )
        return context
    except Exception:
        if context is not None:
            context.close()
        else:
            store.close()
        raise


def _build_base_context(config, store: SQLiteStore) -> AppContext:
    """构建所有命令共享的存储/repository 基础层。"""
    market_repository = MarketRepository(store)
    order_repository = OrderRepository(store)
    account_repository = AccountRepository(store)
    audit_repository = AuditRepository(store)
    strategy_repository = StrategyRepository(store)
    backtest_run_repository = BacktestRunRepository(store)
    data_import_repository = DataImportRepository(store)
    return AppContext(
        config=config,
        market_repository=market_repository,
        order_repository=order_repository,
        account_repository=account_repository,
        audit_repository=audit_repository,
        strategy_repository=strategy_repository,
        backtest_run_repository=backtest_run_repository,
        data_import_repository=data_import_repository,
        store=store,
    )


def _build_broker(
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
            raise ValueError(
                f"research_backtest 模式下 broker.provider 必须为 mock；当前为 {config.broker.provider}。"
                "真实 broker 仅用于 runtime 校验或未来独立 paper/live orchestration。"
            )
        return MockBroker(config.backtest.initial_cash, config.backtest.fee_bps, config.backtest.tax_bps)

    if provider in {"qmt", "ptrade"} and provider not in clients:
        loaded_client = load_broker_client(config, provider=provider, factory_path_override=broker_client_factory)
        if loaded_client is not None:
            clients[provider] = loaded_client
    if provider == "qmt":
        if not config.broker.endpoint or not config.broker.account_id:
            raise ValueError("QMT 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("qmt")
        if client is None:
            raise ValueError(
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
            raise ValueError("PTrade 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("ptrade")
        if client is None:
            raise ValueError(
                "当前工程未内置 PTrade 运行时；请通过 bootstrap(..., broker_clients={'ptrade': client}) 注入客户端，"
                "或在 broker.client_factory / --broker-client-factory 中提供工厂路径"
            )
        return PTradeAdapter(
            client,
            timeout_seconds=config.broker.operation_timeout_seconds,
            strict_contract_mapping=config.broker.strict_contract_mapping,
        )
    if provider == "mock":
        raise ValueError(
            f"app.runtime_mode={runtime_mode} 时不允许使用 mock broker；请切回 research_backtest 或配置真实 broker"
        )
    raise ValueError(f"不支持的 broker.provider: {config.broker.provider}")
