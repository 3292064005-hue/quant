"""数据集插件。"""
from __future__ import annotations

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


class DatasetPlugin(AppPlugin):
    descriptor = PluginDescriptor(
        name="builtin.dataset",
        plugin_type="dataset",
        provides=("provider.dataset", "provider.feature"),
        capability_tags=("dataset_snapshot", "feature_provider", "workflow_gate"),
        hook_contracts=("on_context_ready", "before_workflow_run"),
        metadata={"component": "provider.dataset"},
    )

    def configure(self, context) -> None:
        """登记 dataset provider。"""
        if context.provider_registry is None:
            return
        context.component_registry.register(
            self.descriptor.name,
            [entry.name for entry in context.provider_registry.list_entries()],
            metadata={
                "plugin_type": self.descriptor.plugin_type,
                "provides": list(self.descriptor.provides),
            },
        )

    def on_context_ready(self, context) -> None:
        """验证研究链所需 provider 在可用时已安装。"""
        if context.provider_registry is None:
            return
        installed_names = {entry.name for entry in context.provider_registry.list_entries()}
        if not installed_names:
            return
        for required_name in ("provider.dataset", "provider.feature"):
            if required_name not in installed_names:
                raise RuntimeError(f"DatasetPlugin 缺少必需 provider: {required_name}")

    def before_workflow_run(self, context, workflow_name: str, payload: dict[str, object]) -> None:
        """在 research/backtest 执行前保证 provider 仍可用。"""
        if workflow_name not in {"workflow.research", "workflow.backtest"}:
            return
        if context.provider_registry is None:
            raise RuntimeError("DatasetPlugin 需要 provider_registry 才能执行研究/回测工作流")
