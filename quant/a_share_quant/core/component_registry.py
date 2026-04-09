"""通用组件注册表。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class ComponentDescriptor:
    """组件契约描述。

    该描述与实际 component 对象分离，避免仅靠 dict 声明注册时无法区分
    “声明型组件”“可执行组件”“运行时实例组件”的问题。
    """

    name: str
    component_type: str
    contract_kind: str
    input_contract: str | None = None
    output_contract: str | None = None
    callable_path: str | None = None
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """序列化组件契约，供 UI/审计输出使用。"""
        return {
            "name": self.name,
            "component_type": self.component_type,
            "contract_kind": self.contract_kind,
            "input_contract": self.input_contract,
            "output_contract": self.output_contract,
            "callable_path": self.callable_path,
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class RegisteredComponent:
    """已注册组件。"""

    name: str
    component: Any
    metadata: dict[str, Any] = field(default_factory=dict)
    descriptor: ComponentDescriptor | None = None

    def to_summary(self) -> dict[str, Any]:
        """返回适合输出到 operator plane 的轻量摘要。"""
        return {
            "name": self.name,
            "metadata": dict(self.metadata),
            "descriptor": self.descriptor.to_dict() if self.descriptor is not None else None,
        }


class ComponentRegistry:
    """轻量组件注册中心。"""

    def __init__(self) -> None:
        self._items: dict[str, RegisteredComponent] = {}

    def register(
        self,
        name: str,
        component: Any,
        *,
        metadata: dict[str, Any] | None = None,
        descriptor: ComponentDescriptor | None = None,
    ) -> None:
        """注册组件。

        Args:
            name: 组件名。
            component: 组件对象、实例、描述载荷或可执行对象。
            metadata: 兼容旧路径的附加元数据。
            descriptor: 显式组件契约。传入后会与 metadata 并存。

        Raises:
            ValueError: 名称为空、重复注册、或 descriptor.name 与注册名不一致时抛出。
        """
        if not name:
            raise ValueError("component name 不能为空")
        if name in self._items:
            raise ValueError(f"重复注册组件: {name}")
        if descriptor is not None and descriptor.name != name:
            raise ValueError(f"descriptor.name 与注册名不一致: {descriptor.name} != {name}")
        self._items[name] = RegisteredComponent(
            name=name,
            component=component,
            metadata=dict(metadata or {}),
            descriptor=descriptor,
        )

    def get(self, name: str) -> Any:
        """按名称获取组件。"""
        item = self._items.get(name)
        if item is None:
            raise KeyError(f"未注册组件: {name}")
        return item.component

    def get_entry(self, name: str) -> RegisteredComponent:
        """返回带元数据的注册项。"""
        item = self._items.get(name)
        if item is None:
            raise KeyError(f"未注册组件: {name}")
        return item

    def list_entries(self) -> list[RegisteredComponent]:
        """列出所有注册项。"""
        return list(self._items.values())

    def list_component_summaries(self) -> list[dict[str, Any]]:
        """列出组件摘要。"""
        return [item.to_summary() for item in self._items.values()]

    def contains(self, name: str) -> bool:
        """判断组件是否已存在。"""
        return name in self._items
