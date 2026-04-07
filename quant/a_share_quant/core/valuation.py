"""组合估值与 EOD 快照构建。"""
from __future__ import annotations

from dataclasses import dataclass, field

from a_share_quant.domain.models import AccountSnapshot, PositionSnapshot


@dataclass(slots=True)
class PriceMark:
    """估值价格标记。

    Attributes:
        price: 实际用于估值的价格。
        source: 价格来源，取值为 ``current`` / ``last_known`` / ``avg_cost``。
    """

    price: float
    source: str


@dataclass(slots=True)
class ValuationResult:
    """一次估值计算的输出。"""

    account: AccountSnapshot
    positions: list[PositionSnapshot]
    price_marks: dict[str, PriceMark] = field(default_factory=dict)
    stale_quotes: list[str] = field(default_factory=list)
    fallback_quotes: list[str] = field(default_factory=list)
    missing_quotes: list[str] = field(default_factory=list)


class PortfolioValuator:
    """负责将账户现金 + 原始持仓状态估值为统一快照。

    设计原则：
    1. 估值是纯计算，不依赖 broker 内部可变状态；
    2. ``daily_pnl`` 仅基于 EOD 对比计算；
    3. 当当日无报价时，优先使用最近一次有效价格；若历史价格也缺失，则回退到持仓成本；
       若配置为 ``reject``，则直接抛出异常，阻止生成不可信的 EOD 快照。
    """

    def __init__(self, initial_cash: float, missing_price_policy: str = "last_known") -> None:
        self.initial_cash = initial_cash
        self.missing_price_policy = missing_price_policy
        allowed = {"last_known", "avg_cost", "reject"}
        if self.missing_price_policy not in allowed:
            raise ValueError(f"不支持的 missing_price_policy: {missing_price_policy}")

    def value(
        self,
        raw_account: AccountSnapshot,
        raw_positions: list[PositionSnapshot],
        current_prices: dict[str, float],
        last_known_prices: dict[str, float],
        previous_eod_total_assets: float | None,
        peak_total_assets: float,
        *,
        include_daily_pnl: bool,
    ) -> ValuationResult:
        """对账户和持仓做一次无副作用估值。

        Args:
            raw_account: 券商返回的原始账户状态，当前仅消费现金字段。
            raw_positions: 券商返回的原始持仓状态，当前仅消费数量与成本字段。
            current_prices: 当前交易日有效收盘价映射。
            last_known_prices: 历史最近有效价格映射。
            previous_eod_total_assets: 上一交易日 EOD 总资产；仅在 ``include_daily_pnl=True`` 时参与计算。
            peak_total_assets: 截至上一交易日的峰值资产，用于计算本次估值后的回撤。
            include_daily_pnl: 是否按 EOD 语义计算 ``daily_pnl``。

        Returns:
            ``ValuationResult``。

        Raises:
            ValueError: 当 ``missing_price_policy='reject'`` 且存在无法估值持仓时抛出。
        """
        price_marks: dict[str, PriceMark] = {}
        stale_quotes: list[str] = []
        fallback_quotes: list[str] = []
        missing_quotes: list[str] = []
        valued_positions: list[PositionSnapshot] = []
        market_value = 0.0

        for snapshot in raw_positions:
            price_mark = self._resolve_price(snapshot, current_prices, last_known_prices)
            if price_mark is None:
                missing_quotes.append(snapshot.ts_code)
                if self.missing_price_policy == "reject":
                    raise ValueError(f"持仓无法估值，缺少价格: {snapshot.ts_code}")
                mark_price = snapshot.avg_cost
                price_mark = PriceMark(price=mark_price, source="avg_cost")
                fallback_quotes.append(snapshot.ts_code)
            else:
                if price_mark.source == "last_known":
                    stale_quotes.append(snapshot.ts_code)
                if price_mark.source == "avg_cost":
                    fallback_quotes.append(snapshot.ts_code)
                mark_price = price_mark.price
            price_marks[snapshot.ts_code] = price_mark
            position_market_value = snapshot.quantity * mark_price
            unrealized = (mark_price - snapshot.avg_cost) * snapshot.quantity
            market_value += position_market_value
            valued_positions.append(
                PositionSnapshot(
                    ts_code=snapshot.ts_code,
                    quantity=snapshot.quantity,
                    available_quantity=snapshot.available_quantity,
                    avg_cost=snapshot.avg_cost,
                    market_value=position_market_value,
                    unrealized_pnl=unrealized,
                )
            )

        total_assets = raw_account.cash + market_value
        cumulative_pnl = total_assets - self.initial_cash
        daily_pnl = None
        if include_daily_pnl:
            previous_total = previous_eod_total_assets if previous_eod_total_assets is not None else total_assets
            daily_pnl = total_assets - previous_total
        updated_peak = max(peak_total_assets, total_assets)
        drawdown = 0.0 if updated_peak <= 0 else total_assets / updated_peak - 1.0
        account = AccountSnapshot(
            cash=raw_account.cash,
            available_cash=raw_account.available_cash,
            market_value=market_value,
            total_assets=total_assets,
            pnl=cumulative_pnl,
            cum_pnl=cumulative_pnl,
            daily_pnl=daily_pnl,
            drawdown=drawdown,
        )
        return ValuationResult(
            account=account,
            positions=valued_positions,
            price_marks=price_marks,
            stale_quotes=stale_quotes,
            fallback_quotes=fallback_quotes,
            missing_quotes=missing_quotes,
        )

    def _resolve_price(
        self,
        snapshot: PositionSnapshot,
        current_prices: dict[str, float],
        last_known_prices: dict[str, float],
    ) -> PriceMark | None:
        """解析单个持仓的估值价格。"""
        current_price = current_prices.get(snapshot.ts_code)
        if current_price is not None and current_price > 0:
            return PriceMark(price=current_price, source="current")
        last_known = last_known_prices.get(snapshot.ts_code)
        if last_known is not None and last_known > 0 and self.missing_price_policy in {"last_known", "avg_cost"}:
            return PriceMark(price=last_known, source="last_known")
        if self.missing_price_policy in {"last_known", "avg_cost"} and snapshot.avg_cost > 0:
            return PriceMark(price=snapshot.avg_cost, source="avg_cost")
        return None
