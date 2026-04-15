"""插件管理器。"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from a_share_quant.core.utils import now_iso
from a_share_quant.domain.models import ExecutionReport, TargetIntent, TradeCommandEvent
from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


@dataclass(slots=True, frozen=True)
class PluginLifecycleHookError(RuntimeError):
    """插件生命周期 hook 执行失败。"""

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
        name = plugin.descriptor.name
        if not name:
            raise ValueError("plugin.descriptor.name 不能为空")
        if name in self._plugins:
            raise ValueError(f"重复注册插件: {name}")
        self._plugins[name] = plugin

    def configure_all(self, context) -> None:
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

    def emit_after_workflow_run(self, context, workflow_name: str, payload: dict[str, Any], *, result: Any | None, error: Exception | None) -> None:
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
            self._record("after_workflow_run", plugin_name=name, workflow_name=workflow_name, payload=dict(payload), result_type=(type(result).__name__ if result is not None else None), error=(type(error).__name__ if error is not None else None))
        if first_error is not None:
            raise first_error

    def emit_target_intents_generated(self, context, strategy_id: str, intents: list[TargetIntent], payload: dict[str, Any]) -> None:
        for name, plugin in self._plugins.items():
            try:
                plugin.on_target_intents_generated(context, strategy_id, list(intents), dict(payload))
            except Exception as exc:
                self._record("target_intents_generated_error", plugin_name=name, strategy_id=strategy_id, payload=dict(payload), error_type=type(exc).__name__, error_message=str(exc), level="WARN")
                continue
            self._record("target_intents_generated", plugin_name=name, strategy_id=strategy_id, payload=dict(payload))

    def emit_risk_decision(self, context, order_id: str, payload: dict[str, Any]) -> None:
        for name, plugin in self._plugins.items():
            try:
                plugin.on_risk_decision(context, order_id, dict(payload))
            except Exception as exc:
                self._record("risk_decision_error", plugin_name=name, order_id=order_id, payload=dict(payload), error_type=type(exc).__name__, error_message=str(exc), level="WARN")
                continue
            self._record("risk_decision", plugin_name=name, order_id=order_id, payload=dict(payload))

    def transform_submission_order(self, context, order_payload: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(order_payload)
        for name, plugin in self._plugins.items():
            try:
                transformed = dict(plugin.transform_submission_order(context, transformed))
            except Exception as exc:
                self._record("transform_submission_order_error", plugin_name=name, payload=dict(transformed), error_type=type(exc).__name__, error_message=str(exc), level="WARN")
                continue
            self._record("transform_submission_order", plugin_name=name, payload=dict(transformed))
        return transformed

    def normalize_execution_report(self, context, report: ExecutionReport) -> ExecutionReport:
        normalized = report
        for name, plugin in self._plugins.items():
            try:
                normalized = plugin.normalize_execution_report(context, normalized)
            except Exception as exc:
                self._record("normalize_execution_report_error", plugin_name=name, order_id=normalized.order_id, error_type=type(exc).__name__, error_message=str(exc), level="WARN")
                continue
            self._record("normalize_execution_report", plugin_name=name, order_id=normalized.order_id)
        return normalized

    def enrich_lifecycle_event(self, context, event: TradeCommandEvent) -> TradeCommandEvent:
        enriched = event
        for name, plugin in self._plugins.items():
            try:
                enriched = plugin.enrich_lifecycle_event(context, enriched)
            except Exception as exc:
                self._record("enrich_lifecycle_event_error", plugin_name=name, event_type=enriched.event_type, error_type=type(exc).__name__, error_message=str(exc), level="WARN")
                continue
            self._record("enrich_lifecycle_event", plugin_name=name, event_type=enriched.event_type)
        return enriched

    def shutdown(self, context) -> None:
        if self._shutdown_executed:
            return
        shutdown_error: PluginLifecycleHookError | None = None
        for name, plugin in reversed(list(self._plugins.items())):
            try:
                plugin.shutdown(context)
                self._record("shutdown", plugin_name=name)
            except Exception as exc:
                self._record("shutdown_error", plugin_name=name, error_type=type(exc).__name__, error_message=str(exc))
                if shutdown_error is None:
                    shutdown_error = PluginLifecycleHookError("shutdown", name)
        self._shutdown_executed = True
        if shutdown_error is not None:
            raise shutdown_error

    def descriptors(self) -> list[PluginDescriptor]:
        return [plugin.descriptor for plugin in self._plugins.values()]

    def names(self) -> list[str]:
        return list(self._plugins)

    def lifecycle_events(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._lifecycle_events]

    def extend(self, plugins: Iterable[AppPlugin]) -> None:
        for plugin in plugins:
            self.register(plugin)

    def _record(self, event: str, **payload: Any) -> None:
        level = str(payload.get("level") or ("ERROR" if event.endswith("_error") else "INFO"))
        self._lifecycle_events.append(
            {
                "schema_version": 1,
                "event": event,
                "event_type": event,
                "source": "plugin_manager",
                "level": level,
                "created_at": now_iso(),
                "payload": dict(payload),
                **payload,
            }
        )
