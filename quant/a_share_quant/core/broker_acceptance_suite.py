"""可执行 broker acceptance suite。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from a_share_quant.core.broker_acceptance import BrokerAcceptanceEvidence
from a_share_quant.core.utils import new_id
from a_share_quant.domain.models import OrderRequest, OrderSide, TradeSessionStatus


@dataclass(slots=True)
class AcceptanceScenarioResult:
    name: str
    ok: bool
    message: str
    session_id: str | None = None
    status: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class AcceptanceSuiteResult:
    provider: str
    runtime_mode: str
    suite_name: str
    environment: str
    readiness_level: str
    manifest_path: str | None
    scenarios: list[AcceptanceScenarioResult]

    @property
    def ok(self) -> bool:
        return all(item.ok for item in self.scenarios)

    def to_evidence(self) -> BrokerAcceptanceEvidence:
        checks = [item.name for item in self.scenarios if item.ok]
        metadata = {
            "suite_result": {
                "ok": self.ok,
                "scenarios": [asdict(item) for item in self.scenarios],
            }
        }
        return BrokerAcceptanceEvidence(
            provider=self.provider,
            readiness_level=self.readiness_level,
            verified_at=datetime.now(timezone.utc).isoformat(),
            scenario_checks=checks,
            suite_name=self.suite_name,
            environment=self.environment,
            expires_at=(datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
            capabilities={
                "submit": "submit" in checks,
                "sync": "sync" in checks,
                "reconcile": "reconcile" in checks,
                "restart_recovery": "restart_recovery" in checks,
            },
            metadata=metadata,
        )


def run_operator_acceptance_suite(
    *,
    config_path: str,
    broker_client_factory: str | None,
    provider: str,
    runtime_mode: str,
    manifest_path: str | None,
) -> AcceptanceSuiteResult:
    """执行正式 operator 主链 acceptance 场景。

    Notes:
        - 真正调用 submit -> restart(sync latest open session) -> sync -> reconcile；
        - 不再接受纯静态 manifest 自声明作为默认 demo operator 的唯一证据；
        - 场景结果可回写到 ``manifest_path``，供 release/runtime gate 复用。
    """
    from a_share_quant.app.bootstrap import bootstrap_trade_operator_context

    suite_name = f"{provider}_operator_acceptance_suite"
    environment = "demo_operator_runtime"
    requested_by = "system.acceptance_suite"
    trade_date = _resolve_trade_date(config_path, broker_client_factory=broker_client_factory)
    _ensure_sample_market_data(config_path, broker_client_factory=broker_client_factory)

    scenarios: list[AcceptanceScenarioResult] = []
    session_id: str | None = None

    try:
        with bootstrap_trade_operator_context(config_path, broker_client_factory=broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            order = OrderRequest(
                order_id=f"acceptance_{new_id('order')}",
                trade_date=trade_date,
                strategy_id="operator.acceptance",
                ts_code="600000.SH",
                side=OrderSide.BUY,
                price=10.0,
                quantity=100,
                reason="broker_acceptance_suite",
            )
            submit_result = workflow.submit_orders(
                [order],
                command_source="acceptance_suite.submit",
                requested_by=requested_by,
                idempotency_key=f"acceptance_submit_{trade_date.isoformat()}",
                approved=True,
            )
            session_id = submit_result.summary.session_id
            scenarios.append(
                AcceptanceScenarioResult(
                    name="submit",
                    ok=submit_result.summary.status in {TradeSessionStatus.RUNNING, TradeSessionStatus.RECOVERY_REQUIRED, TradeSessionStatus.COMPLETED},
                    message="operator submit 主链执行完成",
                    session_id=session_id,
                    status=submit_result.summary.status.value,
                    metadata={"order_count": len(submit_result.orders)},
                )
            )
    except Exception as exc:
        scenarios.append(AcceptanceScenarioResult(name="submit", ok=False, message=str(exc), session_id=session_id))
        return _finalize_suite_result(
            provider=provider,
            runtime_mode=runtime_mode,
            suite_name=suite_name,
            environment=environment,
            manifest_path=manifest_path,
            scenarios=scenarios,
        )

    try:
        with bootstrap_trade_operator_context(config_path, broker_client_factory=broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            restart_result = workflow.sync_latest_open_session(requested_by=requested_by)
            session_id = restart_result.summary.session_id
            scenarios.append(
                AcceptanceScenarioResult(
                    name="restart_recovery",
                    ok=restart_result.summary.status in {TradeSessionStatus.RUNNING, TradeSessionStatus.RECOVERY_REQUIRED, TradeSessionStatus.COMPLETED},
                    message="restart 后成功恢复并推进 open session",
                    session_id=session_id,
                    status=restart_result.summary.status.value,
                )
            )
    except Exception as exc:
        scenarios.append(AcceptanceScenarioResult(name="restart_recovery", ok=False, message=str(exc), session_id=session_id))
        return _finalize_suite_result(
            provider=provider,
            runtime_mode=runtime_mode,
            suite_name=suite_name,
            environment=environment,
            manifest_path=manifest_path,
            scenarios=scenarios,
        )

    try:
        with bootstrap_trade_operator_context(config_path, broker_client_factory=broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            sync_result = workflow.sync_session_events(session_id, requested_by=requested_by)
            scenarios.append(
                AcceptanceScenarioResult(
                    name="sync",
                    ok=sync_result.summary.status in {TradeSessionStatus.RUNNING, TradeSessionStatus.RECOVERY_REQUIRED, TradeSessionStatus.COMPLETED},
                    message="sync 主链执行完成",
                    session_id=sync_result.summary.session_id,
                    status=sync_result.summary.status.value,
                )
            )
    except Exception as exc:
        scenarios.append(AcceptanceScenarioResult(name="sync", ok=False, message=str(exc), session_id=session_id))
        return _finalize_suite_result(
            provider=provider,
            runtime_mode=runtime_mode,
            suite_name=suite_name,
            environment=environment,
            manifest_path=manifest_path,
            scenarios=scenarios,
        )

    try:
        with bootstrap_trade_operator_context(config_path, broker_client_factory=broker_client_factory) as context:
            workflow = context.require_workflow_registry().get("workflow.operator_trade")
            reconcile_result = workflow.reconcile_session(session_id, requested_by=requested_by)
            scenarios.append(
                AcceptanceScenarioResult(
                    name="reconcile",
                    ok=reconcile_result.summary.status == TradeSessionStatus.COMPLETED,
                    message="reconcile 主链执行完成",
                    session_id=reconcile_result.summary.session_id,
                    status=reconcile_result.summary.status.value,
                )
            )
    except Exception as exc:
        scenarios.append(AcceptanceScenarioResult(name="reconcile", ok=False, message=str(exc), session_id=session_id))

    return _finalize_suite_result(
        provider=provider,
        runtime_mode=runtime_mode,
        suite_name=suite_name,
        environment=environment,
        manifest_path=manifest_path,
        scenarios=scenarios,
    )


def _finalize_suite_result(
    *,
    provider: str,
    runtime_mode: str,
    suite_name: str,
    environment: str,
    manifest_path: str | None,
    scenarios: list[AcceptanceScenarioResult],
) -> AcceptanceSuiteResult:
    result = AcceptanceSuiteResult(
        provider=provider,
        runtime_mode=runtime_mode,
        suite_name=suite_name,
        environment=environment,
        readiness_level="staging_accepted" if all(item.ok for item in scenarios) else "operable",
        manifest_path=manifest_path,
        scenarios=scenarios,
    )
    if manifest_path:
        path = Path(manifest_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result.to_evidence().to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _ensure_sample_market_data(config_path: str, *, broker_client_factory: str | None) -> None:
    from a_share_quant.app.bootstrap import bootstrap_data_context

    sample_csv = Path(__file__).resolve().parents[1] / "sample_data" / "daily_bars.csv"
    with bootstrap_data_context(config_path) as context:
        dates = context.market_repository.load_bar_trade_dates()
        if dates:
            return
        context.require_data_service().import_csv(str(sample_csv), encoding=context.config.data.default_csv_encoding)


def _resolve_trade_date(config_path: str, *, broker_client_factory: str | None) -> date:
    from a_share_quant.app.bootstrap import bootstrap_data_context

    _ensure_sample_market_data(config_path, broker_client_factory=broker_client_factory)
    with bootstrap_data_context(config_path) as context:
        dates = context.market_repository.load_bar_trade_dates(ts_codes=["600000.SH"])
        if dates:
            return dates[-1]
    return date.today()
