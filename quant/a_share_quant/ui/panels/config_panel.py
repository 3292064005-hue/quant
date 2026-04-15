"""配置摘要面板。"""
from __future__ import annotations

from a_share_quant.config.models import AppConfig
from a_share_quant.ui.panels.common import build_key_value_group, build_page


def build_config_panel(config: AppConfig) -> object:
    """构建配置摘要面板。"""
    return build_page(
        "配置摘要",
        [
            build_key_value_group(
                "应用",
                {
                    "name": config.app.name,
                    "environment": config.app.environment,
                    "timezone": config.app.timezone,
                    "logs_dir": config.app.logs_dir,
                    "runtime_mode": config.app.runtime_mode,
                    "distribution_profile": config.app.distribution_profile,
                    "path_resolution_mode": config.app.path_resolution_mode,
                },
            ),
            build_key_value_group(
                "Profile 能力",
                config.distribution_capabilities(),
            ),
            build_key_value_group(
                "数据",
                {
                    "provider": config.data.provider,
                    "storage_dir": config.data.storage_dir,
                    "reports_dir": config.data.reports_dir,
                    "default_exchange": config.data.default_exchange,
                    "calendar_policy": config.data.calendar_policy,
                    "allow_degraded_data": config.data.allow_degraded_data,
                    "fail_on_degraded_data": config.data.fail_on_degraded_data,
                },
            ),
            build_key_value_group("数据库", {"path": config.database.path}),
            build_key_value_group("Research", {"enable_cache": config.research.enable_cache, "cache_namespace": config.research.cache_namespace, "cache_schema_version": config.research.cache_schema_version, "max_cached_entries": config.research.max_cached_entries, "record_query_runs": config.research.record_query_runs}),
            build_key_value_group(
                "回测",
                {
                    "data_access_mode": config.backtest.data_access_mode,
                    "report_name_template": config.backtest.report_name_template,
                    "benchmark_symbol": config.backtest.benchmark_symbol,
                    "initial_cash": config.backtest.initial_cash,
                    "rebalance_mode": config.backtest.rebalance_mode,
                },
            ),
            build_key_value_group("执行模型", config.backtest.execution.model_dump(mode="json")),
            build_key_value_group(
                "Broker",
                {
                    "provider": config.broker.provider,
                    "endpoint": config.broker.endpoint,
                    "account_id": config.broker.account_id,
                    "strict_contract_mapping": config.broker.strict_contract_mapping,
                    "client_factory": config.broker.client_factory,
                    "event_source_mode": config.broker.event_source_mode,
                },
            ),
            build_key_value_group(
                "Operator 命令",
                {
                    "require_approval": config.operator.require_approval,
                    "max_batch_orders": config.operator.max_batch_orders,
                    "default_requested_by": config.operator.default_requested_by,
                    "fail_fast": config.operator.fail_fast,
                    "supervisor_scan_interval_seconds": config.operator.supervisor_scan_interval_seconds,
                    "supervisor_lease_seconds": config.operator.supervisor_lease_seconds,
                    "supervisor_heartbeat_interval_seconds": config.operator.supervisor_heartbeat_interval_seconds,
                    "supervisor_idle_timeout_seconds": config.operator.supervisor_idle_timeout_seconds,
                    "supervisor_max_sessions_per_pass": config.operator.supervisor_max_sessions_per_pass,
                },
            ),
        ],
    )
