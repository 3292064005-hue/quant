"""特征/因子提供器。"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from a_share_quant.domain.models import Bar
from a_share_quant.engines.factor_engine import FactorEngine


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """研究特征定义。"""

    name: str
    feature_type: str
    required_history_bars: int
    params: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, str] = field(default_factory=dict)
    tags: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FeatureProvider:
    """提供正式 research 特征。

    Boundary Behavior:
        - 仅提供研究态的确定性特征计算，不承担策略调仓编排；
        - 输入 bar 不足时显式抛 ``ValueError``，避免静默生成伪特征；
        - 当前仅内置动量族特征，但接口已经支持批量描述与批量产出。
    """

    def describe_features(self) -> list[FeatureSpec]:
        """返回当前内置特征目录。"""
        return [
            FeatureSpec(
                name="momentum",
                feature_type="cross_sectional_scalar",
                required_history_bars=2,
                params={"lookback": "int>=1"},
                output_schema={"value": "float"},
                tags=("builtin", "daily_bar", "return_based"),
            )
        ]

    def momentum_spec(self, lookback: int) -> FeatureSpec:
        """返回带实际参数的动量特征描述。"""
        if lookback < 1:
            raise ValueError("lookback 必须 >= 1")
        return FeatureSpec(
            name="momentum",
            feature_type="cross_sectional_scalar",
            required_history_bars=lookback + 1,
            params={"lookback": lookback},
            output_schema={"value": "float"},
            tags=("builtin", "daily_bar", "return_based"),
        )

    def momentum(self, bars: list[Bar], lookback: int) -> float:
        """计算动量特征。"""
        return FactorEngine.momentum(bars, lookback)

    def compute_feature(self, feature_name: str, *, bars: list[Bar], **params: Any) -> float:
        """按正式特征名计算单个特征。"""
        normalized = feature_name.strip().lower()
        if normalized == "momentum":
            lookback = int(params.get("lookback", 1))
            return self.momentum(bars, lookback)
        raise ValueError(f"不支持的 feature_name: {feature_name}")

    def compute_feature_batch(
        self,
        feature_name: str,
        histories_by_symbol: dict[str, list[Bar]],
        **params: Any,
    ) -> dict[str, float]:
        """批量计算横截面特征值。"""
        values: dict[str, float] = {}
        for ts_code, bars in histories_by_symbol.items():
            try:
                values[ts_code] = self.compute_feature(feature_name, bars=bars, **params)
            except ValueError:
                continue
        return values
