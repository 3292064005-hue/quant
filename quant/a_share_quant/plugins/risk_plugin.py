"""风险插件。"""
from __future__ import annotations

from a_share_quant.plugins.base import AppPlugin, PluginDescriptor


class RiskPlugin(AppPlugin):
    descriptor = PluginDescriptor(
        name="builtin.risk",
        plugin_type="risk",
        provides=("risk_config",),
        capability_tags=("risk_gate", "workflow_guard"),
        hook_contracts=("before_workflow_run",),
        metadata={"component": "builtin.risk_engine"},
    )

    def configure(self, context) -> None:
        """登记风控组件元数据。"""
        context.component_registry.register(
            self.descriptor.name,
            context.config.risk.model_dump(mode="json"),
            metadata={
                "plugin_type": self.descriptor.plugin_type,
                "provides": list(self.descriptor.provides),
            },
        )

    def before_workflow_run(self, context, workflow_name: str, payload: dict[str, object]) -> None:
        """在主 workflow 执行前校验风险配置已就绪。"""
        if workflow_name not in {"workflow.backtest", "workflow.research"}:
            return
        if context.component_registry is None or not context.component_registry.contains(self.descriptor.name):
            raise RuntimeError("RiskPlugin 尚未完成注册，不能执行研究/回测工作流")
