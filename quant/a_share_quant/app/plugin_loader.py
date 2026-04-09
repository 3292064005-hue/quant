"""插件发现、启停与装载。"""
from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass

from a_share_quant.plugins import AnalyserPlugin, BrokerPlugin, DatasetPlugin, RiskPlugin, SchedulerPlugin
from a_share_quant.plugins.base import AppPlugin
from a_share_quant.plugins.plugin_manager import PluginManager


@dataclass(slots=True, frozen=True)
class BuiltinPluginDefinition:
    """内建插件定义。"""

    name: str
    factory: Callable[[], AppPlugin]


_BUILTIN_PLUGIN_DEFINITIONS: dict[str, BuiltinPluginDefinition] = {
    "builtin.risk": BuiltinPluginDefinition(name="builtin.risk", factory=RiskPlugin),
    "builtin.analyser": BuiltinPluginDefinition(name="builtin.analyser", factory=AnalyserPlugin),
    "builtin.scheduler": BuiltinPluginDefinition(name="builtin.scheduler", factory=SchedulerPlugin),
    "builtin.broker": BuiltinPluginDefinition(name="builtin.broker", factory=BrokerPlugin),
    "builtin.dataset": BuiltinPluginDefinition(name="builtin.dataset", factory=DatasetPlugin),
}


class PluginLoadError(RuntimeError):
    """插件装载失败。"""



def builtin_plugin_names() -> list[str]:
    """返回已知内建插件名。"""
    return sorted(_BUILTIN_PLUGIN_DEFINITIONS)



def build_plugin_manager(config) -> PluginManager:
    """按配置构造并注册插件管理器。"""
    plugin_manager = PluginManager()
    for plugin in resolve_plugins(config):
        plugin_manager.register(plugin)
    return plugin_manager



def resolve_plugins(config) -> list[AppPlugin]:
    """按配置解析最终插件集合。

    Rules:
        - ``plugins.enabled_builtin`` 为空时，默认启用全部内建插件；
        - ``plugins.disabled`` 会在 builtin/external 合并后统一移除；
        - ``plugins.external`` 支持 ``module:attr`` / ``module.attr`` 路径，目标可为
          ``AppPlugin`` 子类、零参工厂函数或已构造实例；
        - 重复插件名会显式报错，不允许 silent override。
    """
    plugin_cfg = config.plugins
    enabled_builtin = list(plugin_cfg.enabled_builtin)
    disabled = set(plugin_cfg.disabled)
    external_paths = list(plugin_cfg.external)

    unknown_builtin = set(enabled_builtin) - set(_BUILTIN_PLUGIN_DEFINITIONS)
    if unknown_builtin:
        raise PluginLoadError(f"配置引用了未知内建插件: {sorted(unknown_builtin)}")

    builtin_names = enabled_builtin or builtin_plugin_names()
    plugins: list[AppPlugin] = []
    for name in builtin_names:
        if name in disabled:
            continue
        plugin = _BUILTIN_PLUGIN_DEFINITIONS[name].factory()
        plugins.append(plugin)

    for path in external_paths:
        plugin = _load_external_plugin(path)
        if plugin.descriptor.name in disabled:
            continue
        plugins.append(plugin)

    names: set[str] = set()
    duplicates: list[str] = []
    for plugin in plugins:
        name = plugin.descriptor.name
        if name in names:
            duplicates.append(name)
            continue
        names.add(name)
    if duplicates:
        raise PluginLoadError(f"插件装载结果存在重复 descriptor.name: {sorted(set(duplicates))}")
    return plugins



def _load_external_plugin(path: str) -> AppPlugin:
    target = _load_object_from_path(path)
    if isinstance(target, AppPlugin):
        return target
    if inspect.isclass(target) and issubclass(target, AppPlugin):
        return target()
    if callable(target):
        instance = target()
        if isinstance(instance, AppPlugin):
            return instance
    raise PluginLoadError(
        f"外部插件路径 {path} 必须指向 AppPlugin 实例、AppPlugin 子类或返回 AppPlugin 的零参工厂"
    )



def _load_object_from_path(path: str):
    module_name: str
    attr_name: str
    if ":" in path:
        module_name, attr_name = path.split(":", 1)
    else:
        module_name, _, attr_name = path.rpartition(".")
    if not module_name or not attr_name:
        raise PluginLoadError(f"插件路径格式非法: {path}")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise PluginLoadError(f"无法导入插件模块: {module_name}") from exc
    try:
        return getattr(module, attr_name)
    except AttributeError as exc:
        raise PluginLoadError(f"插件模块 {module_name} 中不存在属性 {attr_name}") from exc
