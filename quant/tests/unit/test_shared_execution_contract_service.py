from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from a_share_quant.domain.models import AccountSnapshot, Bar, OrderRequest, OrderSide, PositionSnapshot, Security, TargetPosition
from a_share_quant.execution.shared_contract_service import SharedExecutionContractService


class _RuntimeOverride:
    def required_history_bars(self, strategy) -> int:
        return 4

    def should_rebalance(self, strategy, eligible_trade_index: int) -> bool:
        return eligible_trade_index % 2 == 0

    def generate_targets(self, strategy, frame) -> list[TargetPosition]:
        return [TargetPosition(ts_code="600000.SH", target_weight=1.0, score=1.0, reason="runtime")]


class _Strategy:
    strategy_id = "demo"

    def __init__(self) -> None:
        self._execution_runtime = _RuntimeOverride()

    def required_history_bars(self) -> int:
        return 1

    def should_rebalance(self, eligible_trade_index: int) -> bool:
        return False

    def generate_targets(self, history_by_symbol, current_date, securities) -> list[TargetPosition]:
        return [TargetPosition(ts_code="000001.SZ", target_weight=1.0, score=0.0, reason="legacy")]


@dataclass(slots=True)
class _Frame:
    trade_date: date
    active_history: dict[str, list[Bar]]
    active_securities: dict[str, Security]



def _build_order(order_id: str, *, ts_code: str = "600000.SH", side: OrderSide = OrderSide.BUY, quantity: int = 100, price: float = 10.0) -> OrderRequest:
    return OrderRequest(
        order_id=order_id,
        trade_date=date(2026, 1, 5),
        strategy_id="operator.manual",
        ts_code=ts_code,
        side=side,
        price=price,
        quantity=quantity,
        reason="test",
    )



def test_shared_execution_contract_prefers_runtime_binding() -> None:
    service = SharedExecutionContractService()
    strategy = _Strategy()
    frame = _Frame(
        trade_date=date(2026, 1, 5),
        active_history={"600000.SH": []},
        active_securities={"600000.SH": Security(ts_code="600000.SH", name="浦发银行", exchange="SSE", board="主板")},
    )

    assert service.required_history_bars(strategy) == 4
    assert service.should_rebalance(strategy, eligible_trade_index=2) is True
    targets = service.generate_targets(strategy, frame)
    assert [item.reason for item in targets] == ["runtime"]



def test_validate_basic_order_inputs_and_projected_weights() -> None:
    service = SharedExecutionContractService()
    securities = {
        "600000.SH": Security(ts_code="600000.SH", name="浦发银行", exchange="SSE", board="主板"),
    }
    bars = {
        "600000.SH": Bar(
            ts_code="600000.SH",
            trade_date=date(2026, 1, 5),
            open=10.0,
            high=10.8,
            low=9.8,
            close=10.5,
            volume=1000,
            amount=10000,
            pre_close=10.0,
        )
    }
    accepted = _build_order("order_accepted")
    rejected = _build_order("order_rejected", ts_code="000001.SZ")
    outcome = service.validate_basic_order_inputs(
        [accepted, rejected],
        trade_date=date(2026, 1, 5),
        securities=securities,
        bars=bars,
    )
    assert [item.order_id for item in outcome.candidate_orders] == ["order_accepted"]
    assert [item.order_id for item in outcome.rejected_orders] == ["order_rejected"]
    assert outcome.reasons_by_symbol["000001.SZ"][0].startswith("证券不存在")

    weights = service.build_projected_target_weights(
        [accepted, _build_order("sell", side=OrderSide.SELL, quantity=50, price=10.0)],
        positions={
            "600000.SH": PositionSnapshot(
                ts_code="600000.SH",
                quantity=100,
                available_quantity=100,
                avg_cost=9.5,
                market_value=1000.0,
                unrealized_pnl=50.0,
            )
        },
        account=AccountSnapshot(cash=9000.0, available_cash=9000.0, market_value=1000.0, total_assets=10000.0, pnl=0.0),
    )
    assert weights["600000.SH"] > 0
