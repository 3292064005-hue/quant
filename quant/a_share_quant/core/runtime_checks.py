"""运行前环境与适配器健康检查。"""
from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from datetime import date
from importlib.util import find_spec
from typing import Any, Callable, Iterable

from a_share_quant.adapters.broker.mappers import map_account_snapshot, map_fill, map_position_snapshots
from a_share_quant.domain.models import AccountSnapshot, Fill, OrderRequest, OrderSide, PositionSnapshot


@dataclass(slots=True)
class RuntimeCheckResult:
    """单项运行前检查结果。"""

    name: str
    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _check_python_module(module_name: str, install_hint: str, *, name: str | None = None) -> RuntimeCheckResult:
    if find_spec(module_name) is None:
        return RuntimeCheckResult(
            name=name or module_name,
            ok=False,
            message=f"缺少可选依赖 {module_name}；{install_hint}",
            details={"module": module_name, "install_hint": install_hint},
        )
    return RuntimeCheckResult(
        name=name or module_name,
        ok=True,
        message=f"依赖 {module_name} 已安装",
        details={"module": module_name},
    )


def check_ui_runtime() -> RuntimeCheckResult:
    """检查桌面 UI 运行依赖是否齐备。"""
    return _check_python_module("PySide6", "请执行 pip install '.[ui]' 或 pip install PySide6", name="ui")


def check_data_provider_runtime(provider: str, *, token_present: bool = False) -> RuntimeCheckResult:
    """检查指定数据源在当前环境中是否具备运行条件。"""
    normalized = provider.strip().lower()
    if normalized == "csv":
        return RuntimeCheckResult(name="data_provider", ok=True, message="CSV 数据源无需额外运行时")
    if normalized == "tushare":
        dependency = _check_python_module("tushare", "请执行 pip install '.[tushare]' 或 pip install tushare", name="data_provider")
        if not dependency.ok:
            return dependency
        if not token_present:
            return RuntimeCheckResult(
                name="data_provider",
                ok=False,
                message="Tushare 运行时缺少 token；请配置 data.tushare_token 或环境变量 TUSHARE_TOKEN",
                details={"provider": "tushare", "token_required": True},
            )
        return RuntimeCheckResult(name="data_provider", ok=True, message="Tushare 运行时检查通过", details={"provider": "tushare"})
    if normalized == "akshare":
        dependency = _check_python_module("akshare", "请执行 pip install '.[akshare]' 或 pip install akshare", name="data_provider")
        if not dependency.ok:
            return dependency
        return RuntimeCheckResult(name="data_provider", ok=True, message="AKShare 运行时检查通过", details={"provider": "akshare"})
    return RuntimeCheckResult(name="data_provider", ok=False, message=f"未知数据源 provider={provider}", details={"provider": provider})


_REQUIRED_BROKER_CLIENT_METHODS: dict[str, int] = {
    "get_account": 1,
    "get_positions": 1,
    "submit_order": 3,
    "cancel_order": 1,
    "query_orders": 0,
    "query_trades": 0,
    "heartbeat": 0,
}


def _supports_positional_arity(candidate: Callable[..., Any], required_args: int) -> bool:
    signature = inspect.signature(candidate)
    positional_params = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if any(parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in signature.parameters.values()):
        return True
    max_args = len(positional_params)
    if max_args < required_args:
        return False
    mandatory_args = sum(parameter.default is inspect._empty for parameter in positional_params)
    return mandatory_args <= required_args <= max_args or required_args >= mandatory_args


def _read_field(payload: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(payload, dict) and name in payload:
            return payload[name]
        if hasattr(payload, name):
            return getattr(payload, name)
    return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_date(value: Any, *, default: date) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        return default
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}")
    try:
        return date.fromisoformat(text)
    except ValueError:
        return default


def _coerce_side(value: Any, *, default: OrderSide) -> OrderSide:
    if isinstance(value, OrderSide):
        return value
    mapping = {
        "BUY": OrderSide.BUY,
        "B": OrderSide.BUY,
        "1": OrderSide.BUY,
        "LONG": OrderSide.BUY,
        "SELL": OrderSide.SELL,
        "S": OrderSide.SELL,
        "2": OrderSide.SELL,
        "SHORT": OrderSide.SELL,
    }
    return mapping.get(str(value).strip().upper(), default)


def _validate_runtime_mode_provider(runtime_mode: str | None, provider: str) -> RuntimeCheckResult | None:
    """校验运行模式与 broker.provider 的组合是否合法。"""
    if runtime_mode is None:
        return None
    normalized_mode = str(runtime_mode).strip().lower()
    normalized_provider = provider.strip().lower()
    if normalized_mode == "research_backtest" and normalized_provider != "mock":
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=(
                f"research_backtest 模式下 broker.provider 必须为 mock；当前为 {provider}。"
                "真实 broker 仅用于 runtime 校验或未来独立 paper/live orchestration。"
            ),
            details={"runtime_mode": normalized_mode, "provider": normalized_provider, "expected_provider": "mock"},
        )
    if normalized_mode in {"paper_trade", "live_trade"} and normalized_provider == "mock":
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"app.runtime_mode={normalized_mode} 时不允许使用 mock broker；请切回 research_backtest 或配置真实 broker",
            details={"runtime_mode": normalized_mode, "provider": normalized_provider, "mock_allowed": False},
        )
    return None


def _validate_broker_sample_payloads_lenient(sample_payloads: dict[str, Any]) -> RuntimeCheckResult | None:
    try:
        if "account" in sample_payloads:
            payload = sample_payloads["account"]
            cash = _coerce_float(_read_field(payload, "cash", "available_cash", "available", "availableAmount", default=0.0), default=0.0)
            available_cash = _coerce_float(_read_field(payload, "available_cash", "available", "availableAmount", default=cash), default=cash)
            market_value = _coerce_float(_read_field(payload, "market_value", "marketValue", "market_val", "value", default=0.0), default=0.0)
            total_assets = _coerce_float(_read_field(payload, "total_assets", "totalAssets", "asset", "assets", default=cash + market_value), default=cash + market_value)
            pnl = _coerce_float(_read_field(payload, "pnl", "profit", "floating_pnl", default=0.0), default=0.0)
            AccountSnapshot(
                cash=cash,
                available_cash=available_cash,
                market_value=market_value,
                total_assets=total_assets,
                pnl=pnl,
                cum_pnl=None,
                daily_pnl=None,
                drawdown=_coerce_float(_read_field(payload, "drawdown", default=0.0), default=0.0),
            )
        if "positions" in sample_payloads:
            positions_payload = sample_payloads["positions"]
            if positions_payload is not None and (not isinstance(positions_payload, Iterable) or isinstance(positions_payload, (str, bytes, dict))):
                raise ValueError("positions 样本必须是可迭代对象")
            for item in positions_payload or []:
                ts_code = str(_read_field(item, "ts_code", "symbol", "security_code", "stock_code", default="")).strip()
                if not ts_code:
                    continue
                quantity = _coerce_int(_read_field(item, "quantity", "qty", "current_qty", "total_quantity", default=0), default=0)
                available_quantity = _coerce_int(_read_field(item, "available_quantity", "available_qty", "available", default=quantity), default=quantity)
                PositionSnapshot(
                    ts_code=ts_code,
                    quantity=quantity,
                    available_quantity=available_quantity,
                    avg_cost=_coerce_float(_read_field(item, "avg_cost", "cost_price", "avgPrice", "cost", default=0.0), default=0.0),
                    market_value=_coerce_float(_read_field(item, "market_value", "marketValue", "value", default=0.0), default=0.0),
                    unrealized_pnl=_coerce_float(_read_field(item, "unrealized_pnl", "floating_pnl", "profit", default=0.0), default=0.0),
                )
        if "fill" in sample_payloads:
            payload = sample_payloads["fill"]
            trade_date = _coerce_date(_read_field(payload, "trade_date", "date", "trade_day", default=date.today()), default=date.today())
            ts_code = str(_read_field(payload, "ts_code", "symbol", "security_code", "stock_code", default="UNKNOWN")).strip() or "UNKNOWN"
            side = _coerce_side(_read_field(payload, "side", "direction", "bs_type", default=OrderSide.BUY), default=OrderSide.BUY)
            fallback_order = OrderRequest(
                order_id=str(_read_field(payload, "order_id", "broker_order_id", "entrust_no", default="runtime_fill_order")).strip() or "runtime_fill_order",
                trade_date=trade_date,
                strategy_id="runtime_check",
                ts_code=ts_code,
                side=side,
                price=_coerce_float(_read_field(payload, "fill_price", "price", "deal_price", default=0.0), default=0.0),
                quantity=max(1, _coerce_int(_read_field(payload, "fill_quantity", "quantity", "qty", "deal_qty", default=1), default=1)),
                reason="runtime_check",
            )
            Fill(
                fill_id=str(_read_field(payload, "fill_id", "trade_id", "deal_id", default="runtime_fill")).strip() or "runtime_fill",
                order_id=str(_read_field(payload, "order_id", "broker_order_id", "entrust_no", default=fallback_order.order_id)).strip() or fallback_order.order_id,
                trade_date=trade_date,
                ts_code=ts_code,
                side=side,
                fill_price=_coerce_float(_read_field(payload, "fill_price", "price", "deal_price", default=fallback_order.price), default=fallback_order.price),
                fill_quantity=max(0, _coerce_int(_read_field(payload, "fill_quantity", "quantity", "qty", "deal_qty", default=fallback_order.quantity), default=fallback_order.quantity)),
                fee=_coerce_float(_read_field(payload, "fee", "commission", default=0.0), default=0.0),
                tax=_coerce_float(_read_field(payload, "tax", "stamp_tax", default=0.0), default=0.0),
                run_id="runtime_check",
            )
    except Exception as exc:
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"券商样本载荷在兼容映射下仍不可用: {exc}",
            details={"sample_keys": sorted(sample_payloads.keys()), "mapping_mode": "lenient"},
        )
    return None


def _validate_broker_sample_payloads(sample_payloads: dict[str, Any], *, strict_contract_mapping: bool) -> RuntimeCheckResult | None:
    if not strict_contract_mapping:
        return _validate_broker_sample_payloads_lenient(sample_payloads)
    try:
        if "account" in sample_payloads:
            map_account_snapshot(sample_payloads["account"])
        if "positions" in sample_payloads:
            map_position_snapshots(sample_payloads["positions"])
        if "fill" in sample_payloads:
            map_fill(sample_payloads["fill"])
    except Exception as exc:
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"券商样本载荷不满足领域映射契约: {exc}",
            details={"sample_keys": sorted(sample_payloads.keys()), "mapping_mode": "strict"},
        )
    return None


def check_broker_runtime(
    provider: str,
    *,
    endpoint: str = "",
    account_id: str = "",
    injected_client: object | None = None,
    sample_payloads: dict[str, Any] | None = None,
    allow_shallow_client_check: bool = False,
    strict_contract_mapping: bool = True,
    runtime_mode: str | None = None,
) -> RuntimeCheckResult:
    """检查券商适配器运行前条件。

    Args:
        provider: broker provider。
        endpoint: 券商 endpoint。
        account_id: 账户 ID。
        injected_client: 真实客户端对象；用于严格方法契约检查。
        sample_payloads: 可选样本载荷；用于领域映射契约检查。
        allow_shallow_client_check: 为 ``True`` 时，若未注入客户端，则退化为“基础配置 + 可选样本载荷”检查，
            不再把“缺少真实客户端”视为失败。适合 CLI 的运行前浅检查。
        strict_contract_mapping: 与适配器一致的契约模式；``False`` 时，样本载荷按 best-effort 兼容模式校验。
        runtime_mode: 可选运行模式；提供后会同时校验 ``runtime_mode`` 与 ``broker.provider`` 的组合合法性。
    """
    normalized = provider.strip().lower()
    runtime_check = _validate_runtime_mode_provider(runtime_mode, normalized)
    if runtime_check is not None:
        return runtime_check
    if normalized == "mock":
        return RuntimeCheckResult(name="broker", ok=True, message="MockBroker 无需额外运行时", details={"provider": normalized, "runtime_mode": runtime_mode})
    if normalized not in {"qmt", "ptrade"}:
        return RuntimeCheckResult(name="broker", ok=False, message=f"未知 broker.provider={provider}", details={"provider": provider})
    if not endpoint or not account_id:
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"{normalized.upper()} 运行时缺少 endpoint/account_id",
            details={"provider": normalized, "runtime_mode": runtime_mode, "endpoint_present": bool(endpoint), "account_id_present": bool(account_id)},
        )
    if sample_payloads:
        sample_check = _validate_broker_sample_payloads(sample_payloads, strict_contract_mapping=strict_contract_mapping)
        if sample_check is not None:
            return sample_check
    if injected_client is None:
        if allow_shallow_client_check:
            details = {
                "provider": normalized,
                "client_checked": False,
                "mode": "shallow",
                "mapping_mode": "strict" if strict_contract_mapping else "lenient",
                "runtime_mode": runtime_mode,
            }
            if sample_payloads:
                details["sample_keys"] = sorted(sample_payloads.keys())
                message = (
                    f"{normalized.upper()} 基础配置检查通过，样本载荷映射通过；"
                    f"当前为{'严格' if strict_contract_mapping else '兼容'}映射模式；未执行客户端方法契约检查"
                )
            else:
                message = (
                    f"{normalized.upper()} 基础配置检查通过；"
                    f"当前为{'严格' if strict_contract_mapping else '兼容'}映射模式；未执行客户端方法契约检查"
                )
            return RuntimeCheckResult(name="broker", ok=True, message=message, details=details)
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"{normalized.upper()} 运行时缺少注入客户端；请通过 bootstrap(..., broker_clients={{'{normalized}': client}}) 提供",
            details={"provider": normalized, "runtime_mode": runtime_mode, "client_required": True},
        )
    missing = [method for method in _REQUIRED_BROKER_CLIENT_METHODS if not callable(getattr(injected_client, method, None))]
    if missing:
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"{normalized.upper()} 客户端缺少必要方法: {missing}",
            details={"provider": normalized, "runtime_mode": runtime_mode, "missing_methods": missing},
        )
    incompatible = [
        method
        for method, required_args in _REQUIRED_BROKER_CLIENT_METHODS.items()
        if not _supports_positional_arity(getattr(injected_client, method), required_args)
    ]
    if incompatible:
        return RuntimeCheckResult(
            name="broker",
            ok=False,
            message=f"{normalized.upper()} 客户端方法签名与工程契约不兼容: {incompatible}",
            details={"provider": normalized, "runtime_mode": runtime_mode, "incompatible_methods": incompatible},
        )
    if sample_payloads:
        sample_check = _validate_broker_sample_payloads(sample_payloads, strict_contract_mapping=strict_contract_mapping)
        if sample_check is not None:
            return sample_check
    return RuntimeCheckResult(
        name="broker",
        ok=True,
        message=(
            f"{normalized.upper()} 运行时检查通过；"
            f"当前为{'严格' if strict_contract_mapping else '兼容'}映射模式"
        ),
        details={
            "provider": normalized,
            "runtime_mode": runtime_mode,
            "client_checked": True,
            "mapping_mode": "strict" if strict_contract_mapping else "lenient",
        },
    )
