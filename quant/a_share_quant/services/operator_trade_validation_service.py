"""operator pre-trade 校验与订单批次规格服务。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date
from typing import Any

from a_share_quant.domain.models import Bar, OrderRequest, OrderStatus, RiskResult


class OperatorTradeValidationService:
    """封装 operator 批次交易日解析、账户绑定与 pre-trade 风控校验。"""

    def __init__(
        self,
        *,
        config,
        broker,
        risk_engine,
        market_repository,
        execution_contract_service,
    ) -> None:
        self.config = config
        self.broker = broker
        self.risk_engine = risk_engine
        self.market_repository = market_repository
        self.execution_contract_service = execution_contract_service

    @staticmethod
    def resolve_trade_date(orders: list[OrderRequest]) -> date:
        trade_dates = {item.trade_date for item in orders}
        if len(trade_dates) != 1:
            raise ValueError("operator trade 当前要求同一批次订单的 trade_date 完全一致")
        return next(iter(trade_dates))

    @staticmethod
    def bind_orders_account_id(orders: list[OrderRequest], account_id: str) -> None:
        for order in orders:
            order.account_id = account_id

    def pre_trade_validate(self, orders: list[OrderRequest]) -> tuple[dict[str, Any], list[OrderRequest], list[OrderRequest]]:
        positions = {item.ts_code: item for item in self.broker.get_positions(last_prices=None)}
        account = self.broker.get_account(last_prices=None)
        trade_date = self.resolve_trade_date(orders)
        ts_codes = sorted({order.ts_code for order in orders})
        securities = self.market_repository.load_securities(ts_codes=ts_codes, as_of_date=trade_date, active_only=False)
        bars = self._load_trade_date_bars(trade_date=trade_date, ts_codes=ts_codes)
        accepted: list[OrderRequest] = []
        validation = self.execution_contract_service.validate_basic_order_inputs(
            orders,
            trade_date=trade_date,
            securities=securities,
            bars=bars,
        )
        rejected = list(validation.rejected_orders)
        reasons_by_symbol: dict[str, list[str]] = defaultdict(list, validation.reasons_by_symbol)
        audit_results: dict[str, list[RiskResult]] = dict(validation.audit_results)
        candidate_orders = list(validation.candidate_orders)
        for order in rejected:
            rejection_reason = self.resolve_rejection_reason(audit_results.get(order.order_id, [])) or "未知输入拒绝"
            order.mark_rejected(OrderStatus.PRE_TRADE_REJECTED, rejection_reason)

        if candidate_orders:
            target_weights = self.execution_contract_service.build_projected_target_weights(candidate_orders, positions=positions, account=account)
            engine_accepted, engine_audit = self.risk_engine.validate_orders(
                candidate_orders,
                securities={code: securities[code] for code in {order.ts_code for order in candidate_orders}},
                bars={code: bars[code] for code in {order.ts_code for order in candidate_orders}},
                positions=positions,
                account=account,
                target_weights=target_weights,
            )
            accepted_ids = {item.order_id for item in engine_accepted}
            accepted.extend(engine_accepted)
            for order in candidate_orders:
                results = engine_audit.get(order.order_id, [])
                audit_results[order.order_id] = list(results)
                if order.order_id in accepted_ids:
                    order.last_error = None
                    continue
                rejection_reason = self.resolve_rejection_reason(results) or "未知风控拒绝"
                order.mark_rejected(OrderStatus.PRE_TRADE_REJECTED, rejection_reason)
                rejected.append(order)
                reasons_by_symbol[order.ts_code].append(rejection_reason)

        total_assets = float(account.total_assets)
        max_weight_value = max(total_assets * float(self.config.risk.max_position_weight), 0.0)
        risk_summary: dict[str, Any] = {
            "trade_date": trade_date.isoformat(),
            "accepted_order_count": len(accepted),
            "rejected_order_count": len(rejected),
            "reasons_by_symbol": dict(reasons_by_symbol),
            "available_cash": float(account.available_cash),
            "total_assets": total_assets,
            "max_order_value": float(self.config.risk.max_order_value),
            "max_weight_value": max_weight_value,
            "results_by_order": {
                order_id: [asdict(result) for result in results]
                for order_id, results in audit_results.items()
                if results
            },
        }
        if len(rejected) == len(orders):
            risk_summary["terminal_error"] = "全部订单在 pre-trade 阶段被拒绝"
        return risk_summary, accepted, rejected

    def _load_trade_date_bars(self, *, trade_date: date, ts_codes: list[str]) -> dict[str, Bar]:
        grouped = self.market_repository.load_bars_grouped(start_date=trade_date, end_date=trade_date, ts_codes=ts_codes)
        return {code: bars[-1] for code, bars in grouped.items() if bars}

    @staticmethod
    def resolve_rejection_reason(results: list[RiskResult]) -> str | None:
        for result in results:
            if not result.passed:
                return result.reason
        return None
