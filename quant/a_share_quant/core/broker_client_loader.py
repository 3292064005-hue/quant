"""真实 broker 客户端工厂加载。"""
from __future__ import annotations

import importlib
import inspect
from typing import Any

from a_share_quant.config.models import AppConfig


class BrokerClientFactoryError(RuntimeError):
    """加载 broker client factory 失败。"""


def _load_object(import_path: str) -> object:
    path = import_path.strip()
    if not path:
        raise BrokerClientFactoryError("broker.client_factory 不能为空")
    module_name: str
    attr_name: str
    if ":" in path:
        module_name, attr_name = path.split(":", 1)
    elif "." in path:
        module_name, attr_name = path.rsplit(".", 1)
    else:
        raise BrokerClientFactoryError(
            f"broker client factory 路径非法: {import_path!r}；请使用 package.module:callable 或 package.module.callable"
        )
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - importlib message passthrough
        raise BrokerClientFactoryError(f"无法导入 broker client factory 模块 {module_name!r}: {exc}") from exc
    try:
        target = getattr(module, attr_name)
    except AttributeError as exc:
        raise BrokerClientFactoryError(f"broker client factory 未找到属性 {attr_name!r}: {import_path!r}") from exc
    return target


def _invoke_factory(factory: object, *, config: AppConfig, provider: str) -> object:
    if not callable(factory):
        raise BrokerClientFactoryError(f"broker client factory 不是可调用对象: {factory!r}")
    signature = inspect.signature(factory)
    kwargs: dict[str, Any] = {}
    supported_keywords = {"config": config, "provider": provider}
    for name, parameter in signature.parameters.items():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            kwargs.update(supported_keywords)
            break
        if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY) and name in supported_keywords:
            kwargs[name] = supported_keywords[name]
    try:
        return factory(**kwargs)
    except TypeError as exc:
        if kwargs:
            raise BrokerClientFactoryError(
                f"broker client factory 调用失败: {factory!r}；当前仅支持可选参数 config/provider，原始错误: {exc}"
            ) from exc
        try:
            return factory()
        except TypeError as second_exc:
            raise BrokerClientFactoryError(
                f"broker client factory 调用失败: {factory!r}；请使用零参数或支持 config/provider 的签名，原始错误: {second_exc}"
            ) from second_exc


def load_broker_client(
    config: AppConfig,
    *,
    provider: str | None = None,
    factory_path_override: str | None = None,
) -> object | None:
    """按配置加载真实 broker 客户端对象。

    Args:
        config: 聚合配置对象。
        provider: 要加载的 provider；缺省时使用配置中的 provider。
        factory_path_override: CLI 级覆盖路径；优先级高于配置中的 `broker.client_factory`。

    Returns:
        已构造的客户端对象；若 provider=mock 或未配置 factory，则返回 ``None``。

    Raises:
        BrokerClientFactoryError: factory 路径非法、导入失败、返回值不符合约定时抛出。
    """
    resolved_provider = (provider or config.broker.provider).strip().lower()
    if resolved_provider == "mock":
        return None
    factory_path = (factory_path_override or config.broker.client_factory or "").strip()
    if not factory_path:
        return None
    factory = _load_object(factory_path)
    product = _invoke_factory(factory, config=config, provider=resolved_provider)
    if isinstance(product, dict):
        if resolved_provider not in product:
            raise BrokerClientFactoryError(
                f"broker client factory 返回了映射，但未包含 provider={resolved_provider!r} 的客户端"
            )
        client = product[resolved_provider]
    else:
        client = product
    if client is None:
        raise BrokerClientFactoryError(f"broker client factory 为 provider={resolved_provider!r} 返回了空客户端")
    return client
