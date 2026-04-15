"""订单监控面板。"""
from __future__ import annotations

from typing import Any

from a_share_quant.ui.panels.common import build_key_value_group, build_page, build_table_group


def build_order_monitor_panel(operations_snapshot: dict[str, Any]) -> object:
    """展示回测执行、operator 会话与可观测性摘要。"""
    latest_execution = operations_snapshot.get("ui_latest_execution_summary") or {}
    latest_operator = operations_snapshot.get("ui_latest_operator_session") or {}
    observability = latest_operator.get("observability") or {}
    return build_page(
        "订单、成交与执行摘要",
        [
            build_key_value_group(
                "最近回测执行",
                {
                    "run_id": latest_execution.get("run_id"),
                    "order_count": latest_execution.get("order_count"),
                    "fill_count": latest_execution.get("fill_count"),
                    "fill_notional": latest_execution.get("fill_notional"),
                    "order_status_counts": latest_execution.get("order_status_counts"),
                },
            ),
            build_table_group(
                "最近回测订单",
                latest_execution.get("recent_orders", []),
                [("订单ID", "order_id"), ("日期", "trade_date"), ("标的", "ts_code"), ("方向", "side"), ("价格", "price"), ("数量", "quantity"), ("状态", "status")],
            ),
            build_table_group(
                "最近回测成交",
                latest_execution.get("recent_fills", []),
                [("成交ID", "fill_id"), ("订单ID", "order_id"), ("日期", "trade_date"), ("标的", "ts_code"), ("方向", "side"), ("成交价", "fill_price"), ("数量", "fill_quantity")],
            ),
            build_key_value_group(
                "最近 Operator 会话",
                {
                    "session_id": latest_operator.get("session_id"),
                    "runtime_mode": latest_operator.get("runtime_mode"),
                    "broker_provider": latest_operator.get("broker_provider"),
                    "status": latest_operator.get("status"),
                    "requested_by": latest_operator.get("requested_by"),
                    "requested_trade_date": latest_operator.get("requested_trade_date"),
                    "order_count": latest_operator.get("order_count"),
                    "submitted_count": latest_operator.get("submitted_count"),
                    "rejected_count": latest_operator.get("rejected_count"),
                    "risk_summary": latest_operator.get("risk_summary"),
                    "error_message": latest_operator.get("error_message"),
                    "account_id": latest_operator.get("account_id"),
                    "last_synced_at": latest_operator.get("last_synced_at"),
                    "supervisor_owner": latest_operator.get("supervisor_owner"),
                    "supervisor_mode": latest_operator.get("supervisor_mode"),
                    "last_supervised_at": latest_operator.get("last_supervised_at"),
                },
            ),
            build_key_value_group(
                "Operator 可观测性",
                {
                    "total_event_count": observability.get("total_event_count"),
                    "degraded_event_count": observability.get("degraded_event_count"),
                    "audit_write_failure_count": observability.get("audit_write_failure_count"),
                    "recovery_retry_failure_count": observability.get("recovery_retry_failure_count"),
                    "supervisor_event_count": observability.get("supervisor_event_count"),
                    "reconcile_event_count": observability.get("reconcile_event_count"),
                    "event_type_counts": observability.get("event_type_counts"),
                    "level_counts": observability.get("level_counts"),
                },
            ),
            build_table_group(
                "Operator 订单",
                latest_operator.get("recent_orders", []),
                [("订单ID", "order_id"), ("标的", "ts_code"), ("方向", "side"), ("价格", "price"), ("数量", "quantity"), ("状态", "status"), ("Broker单号", "broker_order_id")],
            ),
            build_table_group(
                "Operator 成交",
                latest_operator.get("recent_fills", []),
                [("成交ID", "fill_id"), ("订单ID", "order_id"), ("标的", "ts_code"), ("方向", "side"), ("成交价", "fill_price"), ("数量", "fill_quantity")],
            ),
        ],
    )
