"""插件管理器。"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


@dataclass(slots=True, frozen=True)
class PluginLifecycleHookError(RuntimeError):
    """插件生命周期 hook 执行失败。

    Attributes:
        hook_name: 失败的生命周期 hook 名称。
        plugin_name: 失败插件名。
        workflow_name: 关联 workflow；仅 workflow hook 有值。
    """

    hook_name: str
    plugin_name: str
    workflow_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "args",
            (
                f"插件生命周期 hook 执行失败: hook={self.hook_name}, plugin={self.plugin_name}, workflow={self.workflow_name}",
            ),
        )


class PluginManager:
    """管理应用插件的注册、生命周期与执行留痕。"""

    def __init__(self) -> None:
        self._plugins: dict[str, AppPlugin] = {}
        self._lifecycle_events: list[dict[str, Any]] = []
        self._shutdown_executed = False

    def register(self, plugin: AppPlugin) -> None:
        """注册插件实例。"""
        name = plugin.descriptor.name
        if not name:
            raise ValueError("plugin.descriptor.name 不能为空")
        if name in self._plugins:
            raise ValueError(f"重复注册插件: {name}")
        self._plugins[name] = plugin

    def configure_all(self, context) -> None:
        """按依赖顺序配置全部插件，并执行 context_ready hook。"""
        configured: set[str] = set()
        remaining = dict(self._plugins)
        while remaining:
            progressed = False
            for name, plugin in list(remaining.items()):
                if set(plugin.descriptor.depends_on) - configured:
                    continue
                plugin.configure(context)
                configured.add(name)
                del remaining[name]
                self._record("configure", plugin_name=name)
                progressed = True
            if not progressed:
                unresolved = {name: plugin.descriptor.depends_on for name, plugin in remaining.items()}
                raise ValueError(f"插件依赖无法解析: {unresolved}")
        for name, plugin in self._plugins.items():
            plugin.on_context_ready(context)
            self._record("context_ready", plugin_name=name)

    def emit_before_workflow_run(self, context, workflow_name: str, payload: dict[str, Any]) -> None:
        """向全部插件广播 workflow 执行前事件。

        Boundary Behavior:
            - before hook 属于主链准入校验，任一插件失败都会阻断 workflow 真正执行。
            - 失败会以 PluginLifecycleHookError 抛出，并记录错误事件。
        """
        for name, plugin in self._plugins.items():
            try:
                plugin.before_workflow_run(context, workflow_name, dict(payload))
            except Exception as exc:
                self._record(
                    "before_workflow_run_error",
                    plugin_name=name,
                    workflow_name=workflow_name,
                    payload=dict(payload),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                raise PluginLifecycleHookError("before_workflow_run", name, workflow_name) from exc
            self._record("before_workflow_run", plugin_name=name, workflow_name=workflow_name, payload=dict(payload))

    def emit_after_workflow_run(
        self,
        context,
        workflow_name: str,
        payload: dict[str, Any],
        *,
        result: Any | None,
        error: Exception | None,
    ) -> None:
        """向全部插件广播 workflow 执行后事件。

        Boundary Behavior:
            - after hook 必须尽量通知全部插件，避免前序插件失败导致后序插件丢失收尾机会。
            - 若存在异常，会记录全部错误并在广播结束后抛出首个 PluginLifecycleHookError。
        """
        first_error: PluginLifecycleHookError | None = None
        for name, plugin in self._plugins.items():
            try:
                plugin.after_workflow_run(context, workflow_name, dict(payload), result, error)
            except Exception as exc:
                self._record(
                    "after_workflow_run_error",
                    plugin_name=name,
                    workflow_name=workflow_name,
                    payload=dict(payload),
                    result_type=(type(result).__name__ if result is not None else None),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    upstream_error=(type(error).__name__ if error is not None else None),
                )
                if first_error is None:
                    first_error = PluginLifecycleHookError("after_workflow_run", name, workflow_name)
                continue
            self._record(
                "after_workflow_run",
                plugin_name=name,
                workflow_name=workflow_name,
                payload=dict(payload),
                result_type=(type(result).__name__ if result is not None else None),
                error=(type(error).__name__ if error is not None else None),
            )
        if first_error is not None:
            raise first_error

    def shutdown(self, context) -> None:
        """关闭全部插件，重复调用幂等。

        Boundary Behavior:
            - shutdown 会尽量通知全部插件。
            - 若存在异常，会记录全部错误并在结束后抛出首个 PluginLifecycleHookError。
        """
        if self._shutdown_executed:
            return
        shutdown_error: PluginLifecycleHookError | None = None
        for name, plugin in reversed(list(self._plugins.items())):
            try:
                plugin.shutdown(context)
                self._record("shutdown", plugin_name=name)
            except Exception as exc:  # pragma: no cover - 防御性保护
                self._record(
                    "shutdown_error",
                    plugin_name=name,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                if shutdown_error is None:
                    shutdown_error = PluginLifecycleHookError("shutdown", name)
        self._shutdown_executed = True
        if shutdown_error is not None:
            raise shutdown_error

    def descriptors(self) -> list[PluginDescriptor]:
        """返回插件描述列表。"""
        return [plugin.descriptor for plugin in self._plugins.values()]

    def names(self) -> list[str]:
        """返回已注册插件名。"""
        return list(self._plugins)

    def lifecycle_events(self) -> list[dict[str, Any]]:
        """返回插件生命周期执行记录。"""
        return [dict(item) for item in self._lifecycle_events]

    def extend(self, plugins: Iterable[AppPlugin]) -> None:
        """批量注册插件。"""
        for plugin in plugins:
            self.register(plugin)

    def _record(self, event: str, **payload: Any) -> None:
        self._lifecycle_events.append({"event": event, **payload})
