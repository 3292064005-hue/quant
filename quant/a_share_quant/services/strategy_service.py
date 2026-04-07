"""策略服务。"""
from __future__ import annotations

import importlib
import inspect
from typing import Any

from a_share_quant.config.models import AppConfig
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.strategies.momentum import TopNMomentumStrategy


class StrategyService:
    """策略注册、装载与持久化服务。"""

    _BUILTIN_REGISTRY: dict[str, type] = {
        "builtin.top_n_momentum": TopNMomentumStrategy,
    }

    def __init__(self, config: AppConfig, strategy_repository: StrategyRepository) -> None:
        self.config = config
        self.strategy_repository = strategy_repository

    def build_default(self):
        """兼容旧入口：构建当前配置指定的策略。"""
        return self.load_configured_strategy()

    def load_configured_strategy(self):
        """根据配置装载策略并持久化策略定义。

        Returns:
            已实例化的策略对象。

        Raises:
            ValueError: 策略路径不可解析，或构造参数与策略签名不匹配。
        """
        strategy_cls = self._resolve_strategy_class(self.config.strategy.class_path)
        init_params = self._build_init_params(strategy_cls)
        strategy = strategy_cls(**init_params)
        self.strategy_repository.save(
            strategy_id=self.config.strategy.strategy_id,
            strategy_type=type(strategy).__name__,
            class_path=self._resolved_class_path(self.config.strategy.class_path),
            params=init_params,
            version=self.config.strategy.version,
            enabled=True,
        )
        return strategy

    def get_saved_strategy_definition(self, strategy_id: str) -> dict | None:
        """读取指定策略定义。"""
        return self.strategy_repository.get(strategy_id)

    def list_enabled_strategy_definitions(self) -> list[dict]:
        """列出当前已启用策略定义。"""
        return self.strategy_repository.list_enabled()

    def _resolve_strategy_class(self, class_path: str | None):
        resolved_path = self._resolved_class_path(class_path)
        if resolved_path in self._BUILTIN_REGISTRY:
            return self._BUILTIN_REGISTRY[resolved_path]
        if ":" not in resolved_path:
            raise ValueError(f"策略 class_path 格式错误: {resolved_path}；期望 package.module:ClassName")
        module_name, attr_name = resolved_path.split(":", 1)
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - 防御性保护
            raise ValueError(f"无法导入策略模块: {module_name}") from exc
        try:
            strategy_cls = getattr(module, attr_name)
        except AttributeError as exc:  # pragma: no cover - 防御性保护
            raise ValueError(f"策略模块中不存在属性: {resolved_path}") from exc
        if not callable(strategy_cls):
            raise ValueError(f"策略 class_path 不可调用: {resolved_path}")
        return strategy_cls

    def _resolved_class_path(self, class_path: str | None) -> str:
        return (class_path or "builtin.top_n_momentum").strip()

    def _build_init_params(self, strategy_cls: type) -> dict[str, Any]:
        """构造策略初始化参数。

        参数来源优先级：
            1. ``strategy.params`` 显式提供的通用参数
            2. 兼容旧配置的 ``lookback/top_n/holding_days`` 默认参数

        Boundary Behavior:
            - 若策略签名不接受某些参数，且策略未声明 ``**kwargs``，这些参数会被过滤；
            - 若策略声明了 ``**kwargs``，则保留全部显式参数，便于外部策略扩展；
            - 缺少必填参数时抛出 ``ValueError``，避免悄悄以半配置运行。
        """
        signature = inspect.signature(strategy_cls)
        legacy_defaults: dict[str, Any] = {
            "strategy_id": self.config.strategy.strategy_id,
            "lookback": self.config.strategy.lookback,
            "top_n": self.config.strategy.top_n,
            "holding_days": self.config.strategy.holding_days,
        }
        merged_params: dict[str, Any] = {**legacy_defaults, **self.config.strategy.params}
        parameters = signature.parameters
        accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
        if accepts_kwargs:
            filtered = dict(merged_params)
        else:
            allowed = set(parameters)
            filtered = {key: value for key, value in merged_params.items() if key in allowed}
        required = {
            name
            for name, parameter in parameters.items()
            if parameter.default is inspect._empty and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        }
        missing = sorted(required - set(filtered))
        if missing:
            raise ValueError(f"策略 {strategy_cls.__name__} 缺少必要构造参数: {missing}")
        return filtered
