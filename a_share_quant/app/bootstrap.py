"""应用启动组装。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from a_share_quant.adapters.broker.base import BrokerBase
from a_share_quant.adapters.broker.mock_broker import MockBroker
from a_share_quant.adapters.broker.ptrade_adapter import PTradeAdapter
from a_share_quant.adapters.broker.qmt_adapter import QMTAdapter
from a_share_quant.app.context import AppContext
from a_share_quant.config.loader import ConfigLoader
from a_share_quant.core.logging_utils import configure_logging
from a_share_quant.engines.backtest_engine import BacktestEngine
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.audit_repository import AuditRepository
from a_share_quant.repositories.backtest_run_repository import BacktestRunRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.order_repository import OrderRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.services.backtest_service import BacktestService
from a_share_quant.services.data_service import DataService
from a_share_quant.services.report_service import ReportService
from a_share_quant.services.risk_service import RiskService
from a_share_quant.services.strategy_service import StrategyService
from a_share_quant.storage.sqlite_store import SQLiteStore


def bootstrap(config_path: str, broker_clients: dict[str, Any] | None = None) -> AppContext:
    """根据配置构建应用上下文。"""
    config = ConfigLoader.load(config_path)
    configure_logging(config.app.logs_dir)
    schema_sql = Path(__file__).resolve().parents[2].joinpath("schema.sql").read_text(encoding="utf-8")
    store = SQLiteStore(config.database.path)
    store.init_schema(schema_sql)
    market_repository = MarketRepository(store)
    order_repository = OrderRepository(store)
    account_repository = AccountRepository(store)
    audit_repository = AuditRepository(store)
    strategy_repository = StrategyRepository(store)
    backtest_run_repository = BacktestRunRepository(store)

    data_service = DataService(market_repository, config.data)
    strategy_service = StrategyService(config, strategy_repository)
    risk_engine = RiskService(config.risk, config.backtest).build_engine()
    broker = _build_broker(config, broker_clients=broker_clients)
    broker.connect()
    portfolio_engine = PortfolioEngine(
        enforce_lot_size=config.risk.rules.enforce_lot_size,
        rebalance_mode=config.backtest.rebalance_mode,
    )
    backtest_engine = BacktestEngine(
        broker,
        risk_engine,
        portfolio_engine,
        order_repository,
        account_repository,
        audit_repository,
        backtest_run_repository,
        annual_trading_days=config.backtest.metrics.annual_trading_days,
        risk_free_rate=config.backtest.metrics.risk_free_rate,
        slippage_bps=config.backtest.slippage_bps,
    )
    report_service = ReportService(config.data.reports_dir, config.backtest.report_name_template)
    backtest_service = BacktestService(config, backtest_engine, report_service, backtest_run_repository)
    return AppContext(
        config=config,
        data_service=data_service,
        strategy_service=strategy_service,
        backtest_service=backtest_service,
        market_repository=market_repository,
        order_repository=order_repository,
        account_repository=account_repository,
        audit_repository=audit_repository,
        strategy_repository=strategy_repository,
        backtest_run_repository=backtest_run_repository,
        store=store,
    )


def _build_broker(config, broker_clients: dict[str, Any] | None = None) -> BrokerBase:
    provider = config.broker.provider.lower()
    clients = broker_clients or {}
    if provider == "mock":
        return MockBroker(config.backtest.initial_cash, config.backtest.fee_bps, config.backtest.tax_bps)
    if provider == "qmt":
        if not config.broker.endpoint or not config.broker.account_id:
            raise ValueError("QMT 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("qmt")
        if client is None:
            raise ValueError("当前工程未内置 QMT 运行时，请通过 bootstrap(..., broker_clients={'qmt': client}) 注入客户端")
        return QMTAdapter(client, timeout_seconds=config.broker.operation_timeout_seconds)
    if provider == "ptrade":
        if not config.broker.endpoint or not config.broker.account_id:
            raise ValueError("PTrade 模式下必须提供 broker.endpoint 与 broker.account_id")
        client = clients.get("ptrade")
        if client is None:
            raise ValueError("当前工程未内置 PTrade 运行时，请通过 bootstrap(..., broker_clients={'ptrade': client}) 注入客户端")
        return PTradeAdapter(client, timeout_seconds=config.broker.operation_timeout_seconds)
    raise ValueError(f"不支持的 broker.provider: {config.broker.provider}")
