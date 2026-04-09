"""Broker 插件。"""
from __future__ import annotations

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


class BrokerPlugin(AppPlugin):
    descriptor = PluginDescriptor(
        name="builtin.broker",
        plugin_type="broker",
        provides=("broker_adapter",),
        capability_tags=("runtime_boundary", "broker_metadata"),
        hook_contracts=(),
        metadata={},
    )

    def configure(self, context) -> None:
        """登记 broker 组件元数据。"""
        if context.broker is None:
            return
        context.component_registry.register(
            self.descriptor.name,
            context.broker,
            metadata={
                "plugin_type": self.descriptor.plugin_type,
                "provider": context.config.broker.provider,
                "provides": list(self.descriptor.provides),
            },
        )
