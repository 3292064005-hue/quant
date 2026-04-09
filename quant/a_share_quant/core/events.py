"""事件总线与正式事件类型。"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from a_share_quant.core.utils import new_id, now_iso


class EventType:
    """正式事件类型常量。"""

    DAY_CLOSED = "DAY_CLOSED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_PARTIALLY_FILLED = "ORDER_PARTIALLY_FILLED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_REJECTED = "ORDER_REJECTED"
    EXECUTION_REPORT = "EXECUTION_REPORT"
    PORTFOLIO_SNAPSHOT = "PORTFOLIO_SNAPSHOT"
    RISK_DECISION = "RISK_DECISION"


@dataclass(slots=True)
class Event:
    """系统事件。"""

    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: new_id("evt"))
    occurred_at: str = field(default_factory=now_iso)


class EventJournal:
    """追加式事件日志。

    用于在保留同步 ``EventBus`` 语义的同时，为回放、快照与审计摘要提供正式事件源。
    当前仍以进程内日志为主，同时允许把事件旁路写入统一 runtime event substrate。
    """

    def __init__(self, *, sink: Callable[[Event], None] | None = None) -> None:
        self._events: list[Event] = []
        self._sink = sink

    def append(self, event: Event) -> None:
        """追加一条事件。"""
        self._events.append(event)
        if self._sink is not None:
            self._sink(Event(event_type=event.event_type, payload=dict(event.payload), event_id=event.event_id, occurred_at=event.occurred_at))

    def snapshot(self) -> list[Event]:
        """返回当前事件快照。"""
        return [Event(event_type=item.event_type, payload=dict(item.payload), event_id=item.event_id, occurred_at=item.occurred_at) for item in self._events]

    def replay(
        self,
        handler: Callable[[Event], None],
        *,
        event_types: Iterable[str] | None = None,
        start_index: int = 0,
    ) -> int:
        """按日志顺序重放事件。

        Returns:
            实际重放的事件数量。
        """
        allowed = set(event_types or [])
        replayed = 0
        for event in self._events[start_index:]:
            if allowed and event.event_type not in allowed:
                continue
            handler(Event(event_type=event.event_type, payload=dict(event.payload), event_id=event.event_id, occurred_at=event.occurred_at))
            replayed += 1
        return replayed

    def clear(self) -> None:
        """清空日志。"""
        self._events.clear()

    def __len__(self) -> int:
        return len(self._events)


class EventBus:
    """进程内同步事件总线。"""

    def __init__(self, *, journal: EventJournal | None = None, record_history: bool = True) -> None:
        self._handlers: defaultdict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._journal = journal if journal is not None else EventJournal()
        self._record_history = record_history

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """订阅单个事件类型。

        Args:
            event_type: 事件类型。
            handler: 事件处理函数。

        Raises:
            ValueError: 当事件类型为空时抛出。
        """
        if not event_type:
            raise ValueError("event_type 不能为空")
        self._handlers[event_type].append(handler)

    def subscribe_many(self, event_types: Iterable[str], handler: Callable[[Event], None]) -> None:
        """一次性订阅多个事件类型。"""
        for event_type in event_types:
            self.subscribe(event_type, handler)

    def publish(self, event: Event) -> None:
        """发布事件并同步调用订阅者。

        Args:
            event: 待发布事件。

        Boundary Behavior:
            - 当没有订阅者时静默返回；
            - 处理器异常不吞掉，保持调用方可观测；
            - handler 收到的是同一个 ``Event`` 实例，不做隐式复制；
            - 当 ``record_history=True`` 时，事件会先进入 ``EventJournal``，再广播给订阅者。
        """
        if self._record_history:
            self._journal.append(Event(event_type=event.event_type, payload=dict(event.payload), event_id=event.event_id, occurred_at=event.occurred_at))
        for handler in self._handlers.get(event.event_type, []):
            handler(event)

    def publish_type(self, event_type: str, payload: dict[str, Any] | None = None) -> Event:
        """按事件类型直接发布。"""
        event = Event(event_type=event_type, payload=dict(payload or {}))
        self.publish(event)
        return event

    def list_subscribers(self, event_type: str) -> list[Callable[[Event], None]]:
        """返回指定事件类型的订阅者快照。"""
        return list(self._handlers.get(event_type, []))

    def history_snapshot(self) -> list[Event]:
        """返回当前已记录的事件历史。"""
        return self._journal.snapshot()

    def replay_history(
        self,
        handler: Callable[[Event], None],
        *,
        event_types: Iterable[str] | None = None,
        start_index: int = 0,
    ) -> int:
        """重放已记录事件。"""
        return self._journal.replay(handler, event_types=event_types, start_index=start_index)

    def clear_history(self) -> None:
        """清空事件历史。"""
        self._journal.clear()
