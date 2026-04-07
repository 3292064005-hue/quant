"""事件总线。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, DefaultDict


@dataclass(slots=True)
class Event:
    """系统事件。"""

    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)


class EventBus:
    """进程内同步事件总线。"""

    def __init__(self) -> None:
        self._handlers: DefaultDict[str, list[Callable[[Event], None]]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """订阅事件。

        Args:
            event_type: 事件类型。
            handler: 事件处理函数。

        Returns:
            None。

        Raises:
            ValueError: 当事件类型为空时抛出。
        """
        if not event_type:
            raise ValueError("event_type 不能为空")
        self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        """发布事件并同步调用订阅者。

        Args:
            event: 待发布事件。

        Returns:
            None。

        Boundary Behavior:
            当没有订阅者时静默返回；单个处理器异常不会被吞掉。
        """
        for handler in self._handlers.get(event.event_type, []):
            handler(event)
