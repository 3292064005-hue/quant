from __future__ import annotations

from datetime import date

from a_share_quant.domain.models import ExecutionReport, OrderStatus, TradeCommandEvent
from a_share_quant.plugins import AppPlugin, PluginDescriptor
from a_share_quant.plugins.plugin_manager import PluginManager


class _DomainPlugin(AppPlugin):
    descriptor = PluginDescriptor(name="test.domain", plugin_type="test")

    def configure(self, context) -> None:
        return None

    def transform_submission_order(self, context, order_payload: dict[str, object]) -> dict[str, object]:
        payload = dict(order_payload)
        payload["reason"] = "transformed"
        return payload

    def normalize_execution_report(self, context, report: ExecutionReport) -> ExecutionReport:
        report.message = "normalized"
        return report

    def enrich_lifecycle_event(self, context, event: TradeCommandEvent) -> TradeCommandEvent:
        event.payload["enriched"] = True
        return event


def test_plugin_manager_domain_hooks_are_isolated_and_effective() -> None:
    manager = PluginManager()
    manager.register(_DomainPlugin())
    payload = manager.transform_submission_order(object(), {"reason": "raw"})
    assert payload["reason"] == "transformed"
    report = manager.normalize_execution_report(
        object(),
        ExecutionReport(
            report_id="r1",
            order_id="o1",
            trade_date=date(2024, 1, 1),
            status=OrderStatus.ACCEPTED,
            requested_quantity=100,
            filled_quantity=0,
            remaining_quantity=100,
            message="raw",
        ),
    )
    assert report.message == "normalized"
    enriched = manager.enrich_lifecycle_event(object(), TradeCommandEvent(event_id="e1", session_id="s1", event_type="ORDER_ACCEPTED", level="INFO", payload={}))
    assert enriched.payload["enriched"] is True
