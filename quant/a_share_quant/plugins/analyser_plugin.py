"""分析插件。"""
from __future__ import annotations

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


class AnalyserPlugin(AppPlugin):
    descriptor = PluginDescriptor(
        name="builtin.analyser",
        plugin_type="analyser",
        provides=("report_service",),
        capability_tags=("report_generation", "workflow_summary"),
        hook_contracts=("after_workflow_run",),
        metadata={"component": "builtin.report_service"},
    )

    def configure(self, context) -> None:
        """登记报表组件元数据。"""
        if context.report_service is not None:
            context.component_registry.register(
                self.descriptor.name,
                context.report_service,
                metadata={
                    "plugin_type": self.descriptor.plugin_type,
                    "provides": list(self.descriptor.provides),
                },
            )

    def after_workflow_run(self, context, workflow_name: str, payload: dict[str, object], result, error: Exception | None) -> None:
        """在报表/回测/研究后沉淀最近一次分析摘要。"""
        if context.component_registry is None:
            return
        analysis_summary = {
            "workflow_name": workflow_name,
            "payload": payload,
            "result_type": type(result).__name__ if result is not None else None,
            "error": str(error) if error is not None else None,
        }
        entry_name = "builtin.analyser.latest_summary"
        if context.component_registry.contains(entry_name):
            context.component_registry.get_entry(entry_name).component.update(analysis_summary)
            return
        context.component_registry.register(
            entry_name,
            analysis_summary,
            metadata={"plugin_type": self.descriptor.plugin_type, "ephemeral": True},
        )
