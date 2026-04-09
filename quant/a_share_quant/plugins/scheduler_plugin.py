"""调度插件。"""
from __future__ import annotations

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


class SchedulerPlugin(AppPlugin):
    descriptor = PluginDescriptor(
        name="builtin.scheduler",
        plugin_type="scheduler",
        provides=("sync_cli_scheduler",),
        capability_tags=("synchronous_dispatch",),
        hook_contracts=(),
        metadata={"mode": "sync_cli"},
    )

    def configure(self, context) -> None:
        """登记当前调度方式。"""
        context.component_registry.register(
            self.descriptor.name,
            {"entrypoint_mode": "sync_cli", "runtime_mode": context.config.app.runtime_mode},
            metadata={
                "plugin_type": self.descriptor.plugin_type,
                "provides": list(self.descriptor.provides),
            },
        )
