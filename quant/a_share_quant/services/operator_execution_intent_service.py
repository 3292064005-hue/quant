"""paper/live operator 执行意图编排服务。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from a_share_quant.adapters.broker.base import LiveBrokerPort
from a_share_quant.config.models import AppConfig
from a_share_quant.contracts.versioned_contracts import parse_execution_intent_envelope
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import (
    ExecutionIntent,
    ExecutionIntentPlan,
    OrderSide,
    PortfolioDelta,
    PositionSnapshot,
    TargetPosition,
)
from a_share_quant.engines.portfolio_engine import PortfolioContext, PortfolioEngine
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.services.research_promotion import validate_signal_promotion_package
from a_share_quant.strategies.runtime_components import ResearchSignalSnapshotComponent


@dataclass(slots=True)
class ExecutionIntentResolution:
    """执行意图解析结果。"""

    payload: dict[str, Any]
    trade_date: date
    account_id: str | None


class OperatorExecutionIntentService:
    """把 research signal_snapshot 转换为 operator 正式订单计划。

    Notes:
        - 该服务只负责把 research 产物解析为 ``ExecutionIntent`` / ``ExecutionIntentPlan``；
        - 正式风控、幂等、审计与提交仍由 ``TradeOrchestratorService`` 负责；
        - 通过独立服务把“研究信号转换”和“真实下单编排”从 orchestrator 中分离，避免继续加重主服务。
    """

    def __init__(
        self,
        *,
        config: AppConfig,
        broker: LiveBrokerPort,
        market_repository: MarketRepository,
        research_run_repository: ResearchRunRepository,
        portfolio_engine: PortfolioEngine,
    ) -> None:
        self.config = config
        self.broker = broker
        self.market_repository = market_repository
        self.research_run_repository = research_run_repository
        self.portfolio_engine = portfolio_engine
        self.signal_component = ResearchSignalSnapshotComponent()

    def build_research_signal_plan(
        self,
        *,
        research_run_id: str,
        trade_date: date | None = None,
        account_id: str | None = None,
        strategy_id: str | None = None,
    ) -> ExecutionIntentPlan:
        """把 research signal_snapshot 转换为 operator lane 可提交的正式订单计划。

        Args:
            research_run_id: 指定 research ``signal_snapshot`` 运行标识；operator 写路径要求显式提供。
            trade_date: 目标交易日；为空时自动回落到可用的最新行情日期。
            account_id: 账户作用域；为空时回退到 broker 默认账户。
            strategy_id: 可选 strategy_id；为空时优先取 promotion/research payload，再回退配置项。

        Returns:
            ``ExecutionIntentPlan``，其中包含统一执行意图、差额快照与最终订单列表。

        Raises:
            ValueError: 当 research 载荷、行情、证券或账户上下文不足以生成正式订单时抛出。
        """
        if not str(research_run_id).strip():
            raise ValueError("operator signal 提交必须显式提供 research_run_id，禁止隐式消费最近一次 signal_snapshot")
        resolved = self._resolve_signal_payload(research_run_id=research_run_id, trade_date=trade_date, account_id=account_id)
        payload = resolved.payload
        selected_symbols = list(payload.get("selected_symbols") or [])
        target_symbols = [str(item.get("ts_code", "")).strip() for item in selected_symbols if isinstance(item, dict)]
        target_symbols = [item for item in target_symbols if item]
        if not target_symbols:
            raise ValueError("research signal_snapshot 不包含可执行证券，不能进入 operator lane")

        effective_account_id = resolved.account_id
        effective_trade_date = resolved.trade_date
        current_positions = self._load_positions(account_id=effective_account_id)
        position_symbols = sorted({code for code in current_positions})
        symbols = sorted(set(target_symbols) | set(position_symbols))
        securities = self.market_repository.load_securities(symbols, as_of_date=effective_trade_date, active_only=True)
        missing_securities = sorted(set(symbols) - set(securities))
        if missing_securities:
            raise ValueError(f"缺少可交易证券主数据，不能生成正式执行意图: {missing_securities}")

        bars_by_symbol = self.market_repository.load_bars_grouped(
            start_date=effective_trade_date,
            end_date=effective_trade_date,
            ts_codes=symbols,
        )
        bars = {ts_code: items[-1] for ts_code, items in bars_by_symbol.items() if items}
        missing_bars = sorted(set(symbols) - set(bars))
        if missing_bars:
            raise ValueError(f"交易日 {effective_trade_date.isoformat()} 缺少行情，不能生成正式执行意图: {missing_bars}")

        account = self.broker.get_account_snapshot(
            account_id=effective_account_id,
            last_prices={ts_code: bar.close for ts_code, bar in bars.items()},
        )
        targets = self.signal_component.build_targets(payload, active_securities=securities)
        if not targets:
            raise ValueError("research signal_snapshot 在当前交易日没有可执行 target_positions")
        effective_strategy_id = (strategy_id or self._resolve_strategy_id(payload)).strip() or self.config.strategy.strategy_id
        context = PortfolioContext(
            strategy_id=effective_strategy_id,
            trade_date=effective_trade_date,
            account=account,
            positions=current_positions,
            bars=bars,
            securities=securities,
        )
        orders = self.portfolio_engine.generate_orders(list(targets), context)
        deltas = self._build_deltas(targets=targets, positions=current_positions, bars=bars, account_total_assets=float(account.total_assets))
        intent_metadata = {
            "signal_type": payload.get("signal_type"),
            "dataset_version_id": payload.get("dataset_version_id"),
            "dataset_digest": payload.get("dataset_digest"),
            "root_research_run_id": payload.get("root_research_run_id"),
            "research_session_id": payload.get("research_session_id"),
        }
        parse_execution_intent_envelope(
            {
                "intent_contract_version": 1,
                "intent_type": "research.signal_snapshot",
                "strategy_id": effective_strategy_id,
                "trade_date": effective_trade_date.isoformat(),
                "runtime_mode": self.config.app.runtime_mode,
                "source_run_id": payload.get("research_run_id"),
                "account_id": effective_account_id,
                "promotion_package": dict(payload.get("promotion_package") or {}),
                "metadata": intent_metadata,
            }
        )
        intent = ExecutionIntent(
            intent_id=new_id("intent"),
            intent_type="research.signal_snapshot",
            strategy_id=effective_strategy_id,
            trade_date=effective_trade_date,
            runtime_mode=self.config.app.runtime_mode,
            source_run_id=payload.get("research_run_id"),
            account_id=effective_account_id,
            target_positions=list(targets),
            promotion_package=dict(payload.get("promotion_package") or {}),
            metadata=intent_metadata,
        )
        for order in orders:
            order.strategy_id = effective_strategy_id
            order.run_id = intent.source_run_id
            order.account_id = effective_account_id
        return ExecutionIntentPlan(intent=intent, deltas=deltas, orders=orders, source_payload=payload)

    def _resolve_signal_payload(
        self,
        *,
        research_run_id: str,
        trade_date: date | None,
        account_id: str | None,
    ) -> ExecutionIntentResolution:
        payload = self.research_run_repository.load_signal_snapshot(research_run_id)
        promotion_package = validate_signal_promotion_package(payload.get("promotion_package"), config=self.config)
        payload["promotion_package"] = promotion_package
        resolved_trade_date = trade_date or self._resolve_trade_date(payload)
        resolved_account_id = (account_id or self.config.broker.account_id or "").strip() or None
        return ExecutionIntentResolution(payload=payload, trade_date=resolved_trade_date, account_id=resolved_account_id)

    def _resolve_trade_date(self, payload: dict[str, Any]) -> date:
        dataset_summary = dict(payload.get("dataset_summary") or {})
        dataset_end = str(dataset_summary.get("end_date") or "").strip()
        if dataset_end:
            return date.fromisoformat(dataset_end)
        selected_symbols = [
            str(item.get("ts_code", "")).strip()
            for item in payload.get("selected_symbols") or []
            if isinstance(item, dict) and str(item.get("ts_code", "")).strip()
        ]
        trade_dates = self.market_repository.load_bar_trade_dates(ts_codes=selected_symbols)
        if not trade_dates:
            raise ValueError("research signal_snapshot 缺少可用 trade_date，且数据库中不存在对应行情")
        return trade_dates[-1]

    def _load_positions(self, *, account_id: str | None) -> dict[str, PositionSnapshot]:
        positions = self.broker.get_position_snapshots(account_id=account_id)
        return {item.ts_code: item for item in positions}

    def _resolve_strategy_id(self, payload: dict[str, Any]) -> str:
        promotion_package = dict(payload.get("promotion_package") or {})
        blueprint = dict(promotion_package.get("strategy_blueprint") or {})
        signal_name = str(blueprint.get("signal") or payload.get("signal_type") or "research.signal_snapshot")
        return f"research.promoted::{signal_name}"

    @staticmethod
    def _build_deltas(
        *,
        targets: list[TargetPosition],
        positions: dict[str, PositionSnapshot],
        bars: dict[str, Any],
        account_total_assets: float,
    ) -> list[PortfolioDelta]:
        target_map = {item.ts_code: item for item in targets}
        total_assets = max(float(account_total_assets), 1.0)
        deltas: list[PortfolioDelta] = []
        for ts_code in sorted(set(target_map) | set(positions)):
            target = target_map.get(ts_code)
            current = positions.get(ts_code)
            bar = bars.get(ts_code)
            current_quantity = int(current.quantity) if current is not None else 0
            target_weight = float(target.target_weight) if target is not None else 0.0
            target_value = total_assets * target_weight
            target_quantity = int(target_value / float(bar.close)) if bar is not None and float(bar.close) > 0 else 0
            delta_quantity = target_quantity - current_quantity
            if delta_quantity > 0:
                side = OrderSide.BUY
            elif delta_quantity < 0:
                side = OrderSide.SELL
            else:
                side = None
            deltas.append(
                PortfolioDelta(
                    ts_code=ts_code,
                    current_quantity=current_quantity,
                    target_quantity=target_quantity,
                    delta_quantity=delta_quantity,
                    target_weight=target_weight,
                    score=float(target.score) if target is not None else 0.0,
                    price=(float(bar.close) if bar is not None else None),
                    reason=(target.reason if target is not None else "target_weight=0 清仓"),
                    side=side,
                )
            )
        return deltas
