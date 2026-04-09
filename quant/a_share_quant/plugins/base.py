"""插件基础接口。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PluginDescriptor:
    """插件描述。"""

    name: str
    plugin_type: str
    provides: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    enabled_by_default: bool = True
    capability_tags: tuple[str, ...] = ()
    hook_contracts: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


class AppPlugin(ABC):
    """应用插件接口。"""

    descriptor: PluginDescriptor

    @abstractmethod
    def configure(self, context) -> None:
        """在 bootstrap 阶段配置上下文。"""

    def on_context_ready(self, context) -> None:
        """在所有插件 configure 完成后执行最终检查。"""
        return None

    def before_workflow_run(self, context, workflow_name: str, payload: dict[str, Any]) -> None:
        """在 workflow 真正执行前接收只读输入摘要。"""
        return None

    def after_workflow_run(
        self,
        context,
        workflow_name: str,
        payload: dict[str, Any],
        result: Any | None,
        error: Exception | None,
    ) -> None:
        """在 workflow 完成后接收结果或异常。"""
        return None

    def shutdown(self, context) -> None:
        """在上下文关闭前执行插件清理。"""
        return None
