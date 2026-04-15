"""operator 账户快照采集服务。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from a_share_quant.domain.models import AccountSnapshot, PositionSnapshot
from a_share_quant.repositories.account_repository import AccountRepository
from a_share_quant.repositories.execution_session_repository import ExecutionSessionRepository


@dataclass(frozen=True, slots=True)
class OperatorAccountCapturePlan:
    """一次 operator 账户快照采集计划。"""

    session_id: str
    trade_date: date
    account_id: str | None
    source: str
    captured_at: str
    account: AccountSnapshot | None = None
    positions: tuple[PositionSnapshot, ...] = field(default_factory=tuple)
    error_message: str | None = None

    @property
    def enabled(self) -> bool:
        return self.account is not None or self.error_message is not None

    @property
    def succeeded(self) -> bool:
        return self.account is not None and self.error_message is None


class OperatorAccountCaptureService:
    """在主事务外采样账户状态，并在事务内持久化。"""

    def __init__(self, *, account_repository: AccountRepository | None = None) -> None:
        self.account_repository = account_repository

    def is_enabled(self) -> bool:
        return self.account_repository is not None

    def disabled_plan(self, *, session_id: str, trade_date: date, account_id: str | None, source: str, captured_at: str) -> OperatorAccountCapturePlan:
        return OperatorAccountCapturePlan(
            session_id=session_id,
            trade_date=trade_date,
            account_id=account_id,
            source=source,
            captured_at=captured_at,
        )

    def persist_plan(self, plan: OperatorAccountCapturePlan, execution_session_repository: ExecutionSessionRepository) -> str | None:
        """把预采样结果写入数据库与 session event。"""
        if self.account_repository is None or not plan.enabled:
            return None
        if plan.succeeded and plan.account is not None:
            capture_id = self.account_repository.save_operator_account_snapshot(
                plan.session_id,
                plan.trade_date,
                plan.account,
                account_id=plan.account_id,
                source=plan.source,
                captured_at=plan.captured_at,
            )
            self.account_repository.save_operator_position_snapshots(
                plan.session_id,
                plan.trade_date,
                list(plan.positions),
                account_id=plan.account_id,
                source=plan.source,
                capture_id=capture_id,
                captured_at=plan.captured_at,
            )
            execution_session_repository.append_event(
                plan.session_id,
                event_type="ACCOUNT_SNAPSHOT_CAPTURED",
                level="INFO",
                payload={
                    "account_id": plan.account_id,
                    "source": plan.source,
                    "capture_id": capture_id,
                    "position_count": len(plan.positions),
                },
            )
            return capture_id
        execution_session_repository.append_event(
            plan.session_id,
            event_type="ACCOUNT_SNAPSHOT_CAPTURE_FAILED",
            level="ERROR",
            payload={
                "account_id": plan.account_id,
                "source": plan.source,
                "error": plan.error_message,
            },
        )
        return None
