"""执行模型注册中心。"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ExecutionModelDescriptor:
    """执行模型描述。

    Attributes:
        name: 配置层使用的正式模型名。
        model_type: 模型类别，如 ``fill`` / ``slippage`` / ``fee`` / ``tax``。
        market_scope: 适用市场范围。
        config_fields: 依赖的配置字段列表，仅用于观测和校验提示。
        metadata: 其他说明信息。
    """

    name: str
    model_type: str
    market_scope: str = "a_share"
    config_fields: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RegisteredExecutionModel(Generic[T]):
    """执行模型注册项。"""

    descriptor: ExecutionModelDescriptor
    factory: Callable[[Any], T]


class ExecutionModelRegistry:
    """按模型类别管理执行模型工厂。

    Boundary Behavior:
        - 注册表只负责根据配置名称解析正式工厂，不处理 broker 生命周期；
        - 未注册模型会显式抛错，禁止静默回退到其他模型；
        - 同名重复注册视为配置错误，避免 bootstrap 顺序覆盖。
    """

    def __init__(self) -> None:
        self._items: dict[str, dict[str, RegisteredExecutionModel[Any]]] = {}

    def register(self, descriptor: ExecutionModelDescriptor, factory: Callable[[Any], T]) -> None:
        """注册执行模型工厂。"""
        if not descriptor.name:
            raise ValueError("execution model name 不能为空")
        bucket = self._items.setdefault(descriptor.model_type, {})
        if descriptor.name in bucket:
            raise ValueError(f"重复注册 execution model: {descriptor.model_type}:{descriptor.name}")
        bucket[descriptor.name] = RegisteredExecutionModel(descriptor=descriptor, factory=factory)

    def build(self, model_type: str, model_name: str, config: Any) -> Any:
        """根据正式名称实例化执行模型。"""
        entry = self.get_entry(model_type, model_name)
        return entry.factory(config)

    def get_entry(self, model_type: str, model_name: str) -> RegisteredExecutionModel[Any]:
        """获取带描述信息的注册项。"""
        bucket = self._items.get(model_type, {})
        entry = bucket.get(model_name)
        if entry is None:
            supported = sorted(bucket)
            raise ValueError(
                f"暂不支持的 {model_type}_model: {model_name}；"
                f"允许值={supported if supported else '[]'}"
            )
        return entry

    def list_entries(self, model_type: str | None = None) -> list[RegisteredExecutionModel[Any]]:
        """列出注册项。"""
        if model_type is not None:
            return list(self._items.get(model_type, {}).values())
        items: list[RegisteredExecutionModel[Any]] = []
        for bucket in self._items.values():
            items.extend(bucket.values())
        return items


def build_builtin_execution_registry() -> ExecutionModelRegistry:
    """构建内建执行模型注册表。

    Returns:
        已注册当前内建 fill/slippage/fee/tax 模型的注册表。

    Boundary Behavior:
        - 内建模型目录定义在 execution_registry 模块，避免新增模型时必须修改 bootstrap；
        - 若后续扩展为插件式注入，可在外部基于该注册表继续追加注册。
    """
    from a_share_quant.engines.execution_models import AShareSellTaxModel, BpsFeeModel, BpsSlippageModel, VolumeShareFillModel

    registry = ExecutionModelRegistry()
    registry.register(
        ExecutionModelDescriptor(name='bps', model_type='slippage', config_fields=('backtest.slippage_bps',)),
        lambda config: BpsSlippageModel(config.backtest.slippage_bps),
    )
    registry.register(
        ExecutionModelDescriptor(
            name='volume_share',
            model_type='fill',
            config_fields=(
                'backtest.execution.max_volume_share',
                'backtest.execution.min_trade_lot',
                'backtest.execution.allow_partial_fill',
            ),
        ),
        lambda config: VolumeShareFillModel(
            max_volume_share=config.backtest.execution.max_volume_share,
            lot_size=config.backtest.execution.min_trade_lot,
            allow_partial_fill=config.backtest.execution.allow_partial_fill,
        ),
    )
    registry.register(
        ExecutionModelDescriptor(name='broker_bps', model_type='fee', config_fields=('backtest.fee_bps',)),
        lambda config: BpsFeeModel(config.backtest.fee_bps),
    )
    registry.register(
        ExecutionModelDescriptor(name='a_share_sell_tax', model_type='tax', config_fields=('backtest.tax_bps',)),
        lambda config: AShareSellTaxModel(config.backtest.tax_bps),
    )
    return registry
