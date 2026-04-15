"""特征/因子提供器。"""
from __future__ import annotations

from collections.abc import Callable
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


@dataclass(frozen=True, slots=True)
class RegisteredFeature:
    """正式注册的特征定义。"""

    name: str
    feature_type: str
    params_contract: dict[str, Any]
    output_schema: dict[str, str]
    tags: tuple[str, ...]
    required_history_resolver: Callable[[dict[str, Any]], int]
    compute: Callable[[list[Bar], dict[str, Any]], float]

    def describe(self, *, params: dict[str, Any] | None = None) -> FeatureSpec:
        effective_params = dict(params or {})
        return FeatureSpec(
            name=self.name,
            feature_type=self.feature_type,
            required_history_bars=self.required_history_resolver(effective_params),
            params=effective_params or dict(self.params_contract),
            output_schema=dict(self.output_schema),
            tags=tuple(self.tags),
        )


class FeatureProvider:
    """提供正式 research 特征。

    Boundary Behavior:
        - 特征目录由 registry 驱动，而不是 if/else 硬编码；
        - 输入 bar 不足时显式抛 ``ValueError``，避免静默生成伪特征；
        - ``compute_feature_batch`` 对单只证券失败做隔离，但不会吞掉整个 batch；
        - 注册的特征必须提供正式元数据与 required_history 解析规则。
    """

    def __init__(self) -> None:
        self._registry: dict[str, RegisteredFeature] = {}
        self._register_builtin_features()

    def _register_builtin_features(self) -> None:
        self.register_feature(
            RegisteredFeature(
                name="momentum",
                feature_type="cross_sectional_scalar",
                params_contract={"lookback": "int>=1"},
                output_schema={"value": "float"},
                tags=("builtin", "daily_bar", "return_based"),
                required_history_resolver=lambda params: int(params.get("lookback", 1)) + 1,
                compute=lambda bars, params: FactorEngine.momentum(bars, int(params.get("lookback", 1))),
            )
        )
        self.register_feature(
            RegisteredFeature(
                name="daily_return",
                feature_type="cross_sectional_scalar",
                params_contract={},
                output_schema={"value": "float"},
                tags=("builtin", "daily_bar", "return_based", "one_day"),
                required_history_resolver=lambda params: 2,
                compute=self._compute_daily_return,
            )
        )

    @staticmethod
    def _compute_daily_return(bars: list[Bar], params: dict[str, Any]) -> float:
        if len(bars) < 2:
            raise ValueError("daily_return 至少需要 2 根 bar")
        previous_close = float(bars[-2].close)
        if previous_close == 0:
            raise ValueError("daily_return 遇到 pre_close=0，无法计算")
        return (float(bars[-1].close) - previous_close) / previous_close

    def register_feature(self, feature: RegisteredFeature) -> None:
        name = feature.name.strip().lower()
        if not name:
            raise ValueError("feature.name 不能为空")
        self._registry[name] = feature

    def has_feature(self, feature_name: str) -> bool:
        return feature_name.strip().lower() in self._registry

    def describe_features(self) -> list[FeatureSpec]:
        """返回当前内置特征目录。"""
        return [self.describe_feature(name) for name in sorted(self._registry)]

    def describe_feature(self, feature_name: str, **params: Any) -> FeatureSpec:
        normalized = feature_name.strip().lower()
        feature = self._registry.get(normalized)
        if feature is None:
            raise ValueError(f"不支持的 feature_name: {feature_name}")
        return feature.describe(params=params)

    def momentum_spec(self, lookback: int) -> FeatureSpec:
        """返回带实际参数的动量特征描述。"""
        return self.describe_feature("momentum", lookback=lookback)

    def momentum(self, bars: list[Bar], lookback: int) -> float:
        """计算动量特征。"""
        return self.compute_feature("momentum", bars=bars, lookback=lookback)

    def compute_feature(self, feature_name: str, *, bars: list[Bar], **params: Any) -> float:
        """按正式特征名计算单个特征。"""
        normalized = feature_name.strip().lower()
        feature = self._registry.get(normalized)
        if feature is None:
            raise ValueError(f"不支持的 feature_name: {feature_name}")
        spec = feature.describe(params=params)
        if spec.required_history_bars < 1:
            raise ValueError(f"feature_name={feature_name} required_history_bars 非法: {spec.required_history_bars}")
        return float(feature.compute(list(bars), dict(params)))

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
