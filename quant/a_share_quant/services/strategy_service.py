"""策略服务。"""
from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from typing import Any

from a_share_quant.config.models import AppConfig
from a_share_quant.core.component_registry import ComponentDescriptor, ComponentRegistry
from a_share_quant.engines.portfolio_engine import PortfolioEngine
from a_share_quant.repositories.research_run_repository import ResearchRunRepository
from a_share_quant.repositories.strategy_repository import StrategyRepository
from a_share_quant.services.research_promotion import validate_signal_promotion_package
from a_share_quant.strategies.base import StrategyBase, StrategyComponentManifest
from a_share_quant.strategies.momentum import TopNMomentumStrategy
from a_share_quant.strategies.runtime_components import (
    AllActiveAShareUniverse,
    BypassedPortfolioComponent,
    DirectTargetsSignal,
    EqualWeightTopNPortfolio,
    MomentumFactorComponent,
    NoFactorComponent,
    ResearchSignalSnapshotComponent,
    StrategyExecutionRuntime,
    TopNSelectionSignal,
)


@dataclass(frozen=True, slots=True)
class StrategyRegistration:
    """策略注册表项。"""

    strategy_cls: type
    component_manifest: StrategyComponentManifest


class StrategyService:
    """策略注册、装载、组件装配与持久化服务。"""

    _BUILTIN_REGISTRY: dict[str, StrategyRegistration] = {
        "builtin.top_n_momentum": StrategyRegistration(
            strategy_cls=TopNMomentumStrategy,
            component_manifest=TopNMomentumStrategy.component_manifest(),
        ),
    }

    def __init__(
        self,
        config: AppConfig,
        strategy_repository: StrategyRepository,
        component_registry: ComponentRegistry | None = None,
        research_run_repository: ResearchRunRepository | None = None,
        plugin_manager=None,
        plugin_context=None,
    ) -> None:
        self.config = config
        self.strategy_repository = strategy_repository
        self.component_registry = component_registry or self._build_default_component_registry()
        self.research_run_repository = research_run_repository
        self.plugin_manager = plugin_manager
        self.plugin_context = plugin_context
        self._last_loaded_strategy = None

    def bind_plugin_manager(self, plugin_manager, plugin_context=None) -> None:
        """在正式装配完成后回填 plugin manager。

        Args:
            plugin_manager: 已完成注册、后续会在上下文 ready 阶段统一 configure 的插件管理器。
            plugin_context: 可选上下文；为空时保持现有上下文引用。

        Boundary Behavior:
            - 允许在 service 已构造后执行回填，避免 runtime assembly 因装配顺序漂移导致 plugin hook 失效；
            - 若当前 service 已经装载过策略，则会同步把 plugin manager 回填到已绑定的 execution runtime。
        """
        self.plugin_manager = plugin_manager
        if plugin_context is not None:
            self.plugin_context = plugin_context
        loaded_strategy = self._last_loaded_strategy
        if loaded_strategy is None:
            return
        execution_runtime = getattr(loaded_strategy, "_execution_runtime", None)
        if execution_runtime is None:
            return
        execution_runtime.plugin_manager = plugin_manager
        execution_runtime.plugin_context = self.plugin_context

    def build_default(self, *, research_signal_run_id: str | None = None):
        """兼容旧入口：构建当前配置指定的策略。"""
        return self.load_configured_strategy(research_signal_run_id=research_signal_run_id)

    def load_configured_strategy(self, *, research_signal_run_id: str | None = None):
        """根据配置装载策略、绑定组件执行合同并持久化策略定义。

        Args:
            research_signal_run_id: 可选 research signal_snapshot 运行标识。传入后会覆盖配置中的
                ``strategy.research_signal_run_id``，并把策略切到 ``research.signal_snapshot`` 输入模式。

        Returns:
            已实例化且已绑定正式执行运行时的策略对象。

        Raises:
            ValueError: 策略路径不可解析，构造参数不匹配，组件未注册，或 research signal 绑定非法时抛出。
        """
        registration = self._resolve_strategy_registration(self.config.strategy.class_path)
        strategy_cls = registration.strategy_cls
        init_params = self._build_init_params(strategy_cls)
        strategy = strategy_cls(**init_params)
        manifest = self._resolve_component_manifest(strategy_cls, registration.component_manifest)
        resolved_signal_run_id = (research_signal_run_id or self.config.strategy.research_signal_run_id or "").strip() or None
        manifest = self._apply_research_signal_binding(manifest, resolved_signal_run_id)
        strategy_blueprint = self._build_strategy_blueprint(manifest, research_signal_run_id=resolved_signal_run_id)
        runtime, promotion_package = self._build_execution_runtime(manifest, resolved_signal_run_id)
        strategy._component_manifest = manifest.to_dict()
        strategy._strategy_blueprint = strategy_blueprint
        strategy._promotion_package = promotion_package
        strategy._strategy_init_params = dict(init_params)
        strategy._execution_runtime = runtime
        strategy._bound_research_signal_run_id = resolved_signal_run_id
        self._last_loaded_strategy = strategy
        self.strategy_repository.save(
            strategy_id=self.config.strategy.strategy_id,
            strategy_type=type(strategy).__name__,
            class_path=self._resolved_class_path(self.config.strategy.class_path),
            params=init_params,
            version=self.config.strategy.version,
            enabled=True,
            component_manifest=manifest.to_dict(),
            capability_tags=list(manifest.capability_tags),
            strategy_blueprint=strategy_blueprint,
        )
        return strategy

    def get_saved_strategy_definition(self, strategy_id: str) -> dict | None:
        """读取指定策略定义。"""
        return self.strategy_repository.get(strategy_id)

    def list_enabled_strategy_definitions(self) -> list[dict]:
        """列出当前已启用策略定义。"""
        return self.strategy_repository.list_enabled()

    def _build_default_component_registry(self) -> ComponentRegistry:
        """为存储态/单测等无上下文装配场景提供最小组件注册表。"""
        registry = ComponentRegistry()
        registry.register(
            "builtin.portfolio_engine",
            PortfolioEngine,
            metadata={"component_type": "portfolio_engine_cls", "contract_kind": "executable"},
            descriptor=ComponentDescriptor(
                name="builtin.portfolio_engine",
                component_type="portfolio_engine_cls",
                contract_kind="executable",
                input_contract="target_positions + holdings",
                output_contract="order_requests",
                callable_path="a_share_quant.engines.portfolio_engine:PortfolioEngine.generate_orders",
                tags=("strategy", "portfolio"),
            ),
        )
        self._register_default_component(
            registry,
            name="builtin.all_active_a_share",
            component=AllActiveAShareUniverse(),
            component_type="universe",
            input_contract="trade_day_frame",
            output_contract="eligible_histories + eligible_securities",
            contract_kind="runtime_instance",
            tags=("strategy", "universe"),
            callable_path="a_share_quant.strategies.runtime_components:AllActiveAShareUniverse.select",
        )
        self._register_default_component(
            registry,
            name="builtin.top_n_selection",
            component=TopNSelectionSignal(),
            component_type="signal",
            input_contract="factor_values",
            output_contract="ranked_symbols",
            contract_kind="runtime_instance",
            tags=("strategy", "signal"),
            callable_path="a_share_quant.strategies.runtime_components:TopNSelectionSignal.select",
        )
        self._register_default_component(
            registry,
            name="builtin.equal_weight_top_n",
            component=EqualWeightTopNPortfolio(),
            component_type="portfolio_construction",
            input_contract="selected_symbols",
            output_contract="target_positions",
            contract_kind="runtime_instance",
            tags=("strategy", "portfolio"),
            callable_path="a_share_quant.strategies.runtime_components:EqualWeightTopNPortfolio.build_targets",
        )
        self._register_default_component(
            registry,
            name="builtin.bypassed_portfolio",
            component=BypassedPortfolioComponent(),
            component_type="portfolio_construction",
            input_contract="target_positions",
            output_contract="target_positions",
            contract_kind="runtime_instance",
            tags=("strategy", "portfolio", "bypass"),
            callable_path="a_share_quant.strategies.runtime_components:BypassedPortfolioComponent.build_targets",
        )
        self._register_default_component(
            registry,
            name="builtin.momentum",
            component=MomentumFactorComponent(),
            component_type="factor",
            input_contract="bars_by_symbol + lookback",
            output_contract="factor_values",
            contract_kind="runtime_instance",
            tags=("factor", "momentum"),
            callable_path="a_share_quant.strategies.runtime_components:MomentumFactorComponent.compute",
        )
        self._register_default_component(
            registry,
            name="builtin.none",
            component=NoFactorComponent(),
            component_type="factor",
            input_contract="none",
            output_contract="none",
            contract_kind="runtime_instance",
            tags=("factor", "none"),
            callable_path="a_share_quant.strategies.runtime_components:NoFactorComponent.compute",
        )
        self._register_default_component(
            registry,
            name="builtin.direct_targets",
            component=DirectTargetsSignal(),
            component_type="signal",
            input_contract="strategy_targets",
            output_contract="target_positions",
            contract_kind="runtime_instance",
            tags=("strategy", "signal"),
            callable_path="a_share_quant.strategies.runtime_components:DirectTargetsSignal.build_targets",
        )
        self._register_default_component(
            registry,
            name="research.signal_snapshot",
            component=ResearchSignalSnapshotComponent(),
            component_type="signal",
            input_contract="research_run.signal_snapshot",
            output_contract="target_positions",
            contract_kind="runtime_instance",
            tags=("strategy", "signal", "research"),
            callable_path="a_share_quant.strategies.runtime_components:ResearchSignalSnapshotComponent.build_targets",
        )
        return registry

    def _register_default_component(
        self,
        registry: ComponentRegistry,
        *,
        name: str,
        component: Any,
        component_type: str,
        input_contract: str,
        output_contract: str,
        contract_kind: str,
        tags: tuple[str, ...],
        callable_path: str | None = None,
    ) -> None:
        registry.register(
            name,
            component,
            metadata={"component_type": component_type, "contract_kind": contract_kind},
            descriptor=ComponentDescriptor(
                name=name,
                component_type=component_type,
                contract_kind=contract_kind,
                input_contract=input_contract,
                output_contract=output_contract,
                callable_path=callable_path,
                tags=tags,
            ),
        )

    def _resolve_strategy_registration(self, class_path: str | None) -> StrategyRegistration:
        resolved_path = self._resolved_class_path(class_path)
        builtin = self._BUILTIN_REGISTRY.get(resolved_path)
        if builtin is not None:
            return builtin
        strategy_cls = self._import_strategy_class(resolved_path)
        return StrategyRegistration(
            strategy_cls=strategy_cls,
            component_manifest=self._resolve_component_manifest(strategy_cls),
        )

    def _import_strategy_class(self, resolved_path: str):
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

    def _resolve_component_manifest(
        self,
        strategy_cls: type,
        fallback: StrategyComponentManifest | None = None,
    ) -> StrategyComponentManifest:
        """解析策略组件声明。"""
        if fallback is not None:
            return StrategyComponentManifest(**fallback.to_dict())
        manifest_provider = getattr(strategy_cls, "component_manifest", None)
        if callable(manifest_provider):
            manifest_value = manifest_provider()
            if isinstance(manifest_value, StrategyComponentManifest):
                return manifest_value
            if isinstance(manifest_value, dict):
                return StrategyComponentManifest(**manifest_value)
            raise ValueError(f"策略 {strategy_cls.__name__} 的 component_manifest 返回值非法")
        if inspect.isclass(strategy_cls) and issubclass(strategy_cls, StrategyBase):
            return strategy_cls.component_manifest()
        return StrategyComponentManifest(capability_tags=["research", "external_strategy", "single_strategy"])

    def _build_init_params(self, strategy_cls: type) -> dict[str, Any]:
        """构造策略初始化参数。"""
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

    def _apply_research_signal_binding(
        self,
        manifest: StrategyComponentManifest,
        research_signal_run_id: str | None,
    ) -> StrategyComponentManifest:
        """在声明层把 research signal 输入模式显式写入 manifest。"""
        payload = manifest.to_dict()
        capability_tags = list(payload.pop("capability_tags", []))
        if research_signal_run_id:
            payload["signal_component"] = "research.signal_snapshot"
            payload["factor_component"] = "builtin.none"
            payload["portfolio_construction_component"] = "builtin.bypassed_portfolio"
            if "research_signal_binding" not in capability_tags:
                capability_tags.append("research_signal_binding")
        payload["capability_tags"] = capability_tags
        return StrategyComponentManifest(**payload)

    def _build_strategy_blueprint(
        self,
        manifest: StrategyComponentManifest,
        *,
        research_signal_run_id: str | None,
    ) -> dict[str, Any]:
        """构造组件化策略蓝图。"""
        return {
            "universe": manifest.universe_component,
            "factor": manifest.factor_component,
            "signal": manifest.signal_component,
            "portfolio_construction": manifest.portfolio_construction_component,
            "execution_policy": manifest.execution_policy_component,
            "risk_gate": manifest.risk_gate_component,
            "benchmark": manifest.benchmark_component,
            "capability_tags": list(manifest.capability_tags),
            "execution_contract": "component_runtime",
            "target_intent_contract": "target_intent.v1",
            "signal_source_run_id": research_signal_run_id,
            "promotion_contract_required": bool(research_signal_run_id),
        }

    def _build_execution_runtime(
        self,
        manifest: StrategyComponentManifest,
        research_signal_run_id: str | None,
    ) -> tuple[StrategyExecutionRuntime, dict[str, Any] | None]:
        """解析并构建正式组件执行运行时。"""
        universe_component = self._require_component(manifest.universe_component)
        factor_component = self._require_component(manifest.factor_component)
        signal_component = self._require_component(manifest.signal_component)
        portfolio_component = self._require_component(manifest.portfolio_construction_component)
        research_signal_payload = None
        promotion_package: dict[str, Any] | None = None
        if manifest.signal_component == "research.signal_snapshot":
            if self.research_run_repository is None:
                raise ValueError("绑定 research.signal_snapshot 时必须注入 ResearchRunRepository")
            research_signal_payload = self.research_run_repository.load_signal_snapshot(research_signal_run_id)
            promotion_package = validate_signal_promotion_package(
                research_signal_payload.get("promotion_package"),
                config=self.config,
            )
        runtime = StrategyExecutionRuntime(
            manifest=manifest,
            universe_component=universe_component,
            factor_component=factor_component,
            signal_component=signal_component,
            portfolio_component=portfolio_component,
            research_signal_payload=research_signal_payload,
            plugin_manager=self.plugin_manager,
            plugin_context=self.plugin_context,
        )
        return runtime, promotion_package

    def _require_component(self, name: str):
        try:
            return self.component_registry.get(name)
        except KeyError as exc:
            raise ValueError(f"策略组件未注册: {name}") from exc
