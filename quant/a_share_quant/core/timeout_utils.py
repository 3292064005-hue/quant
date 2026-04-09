"""外部调用 timeout 包装器。"""
from __future__ import annotations

import atexit
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from threading import Lock
from typing import Any, TypeVar

from a_share_quant.core.exceptions import ExternalServiceTimeoutError

T = TypeVar("T")
_EXECUTOR_LOCK = Lock()
_EXECUTOR: ThreadPoolExecutor | None = None


def _get_executor() -> ThreadPoolExecutor:
    """获取共享线程池。

    Notes:
        该模块级线程池用于减少高频 timeout 包装调用的线程创建/销毁开销。
        线程池本身并不能保证真正中断底层阻塞调用，因此 timeout 仍属于 best-effort 语义。
    """
    global _EXECUTOR
    if _EXECUTOR is None:
        with _EXECUTOR_LOCK:
            if _EXECUTOR is None:
                _EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="external-timeout")
    return _EXECUTOR


def _shutdown_executor() -> None:
    global _EXECUTOR
    executor = _EXECUTOR
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)
        _EXECUTOR = None


atexit.register(_shutdown_executor)


def call_with_timeout(
    operation: Callable[..., T],
    *args: Any,
    timeout_seconds: float | None = None,
    operation_name: str = "external_operation",
    **kwargs: Any,
) -> T:
    """以 best-effort 方式为外部调用施加时限。

    Args:
        operation: 待执行调用。
        *args: 位置参数。
        timeout_seconds: 超时时间；当为 ``None``、0 或负数时，直接调用。
        operation_name: 写入异常信息的操作名称。
        **kwargs: 关键字参数。

    Returns:
        调用返回值。

    Raises:
        ExternalServiceTimeoutError: 当调用在约定时限内未返回时抛出。
        Exception: 原始调用抛出的异常原样上抛。

    Boundary Behavior:
        该实现使用共享线程池等待结果，属于跨依赖、跨运行时的 best-effort timeout。
        对不支持原生取消的第三方调用，超时后会立即向上抛出异常并请求取消 future，
        但底层阻塞调用是否能被真正终止取决于对应依赖实现。
    """
    if timeout_seconds is None or timeout_seconds <= 0:
        return operation(*args, **kwargs)
    future = _get_executor().submit(operation, *args, **kwargs)
    try:
        return future.result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise ExternalServiceTimeoutError(f"{operation_name} 超时，timeout_seconds={timeout_seconds}") from exc
