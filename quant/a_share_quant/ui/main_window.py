"""桌面只读运营面板主窗口。"""
from __future__ import annotations

from typing import Any

from a_share_quant.config.models import AppConfig
from a_share_quant.ui.panels import (
    build_boundary_panel,
    build_config_panel,
    build_import_audit_panel,
    build_order_monitor_panel,
    build_report_replay_panel,
    build_risk_alert_panel,
    build_runtime_health_panel,
    build_strategy_lifecycle_panel,
)


def build_main_window(
    *,
    config: AppConfig,
    runtime_results: list[dict[str, Any]] | None = None,
    operations_snapshot: dict[str, Any] | None = None,
) -> object:
    """构建桌面只读运营面板主窗口。

    功能:
        展示配置摘要、运行时检查结果、Provider/Workflow/Plugin 状态、导入审计、
        最近回测与当前桌面层边界说明。

    Args:
        config: 已解析的应用配置。
        runtime_results: 运行时检查结果列表，可为空。
        operations_snapshot: 只读运维摘要，可为空。

    Returns:
        `QMainWindow` 实例。

    Raises:
        ImportError: 当前环境未安装 PySide6 时抛出。
    """
    from PySide6.QtWidgets import QMainWindow, QTabWidget

    runtime_results = runtime_results or []
    operations_snapshot = operations_snapshot or {}

    window = QMainWindow()
    window.setWindowTitle("A 股量化研究与交易工作站 - 桌面只读运营面板")
    tabs = QTabWidget()
    tabs.addTab(build_boundary_panel(), "边界说明")
    tabs.addTab(build_config_panel(config), "配置摘要")
    tabs.addTab(build_runtime_health_panel(runtime_results), "运行时健康")
    tabs.addTab(build_strategy_lifecycle_panel(operations_snapshot), "策略生命周期")
    tabs.addTab(build_order_monitor_panel(operations_snapshot), "订单执行")
    tabs.addTab(build_risk_alert_panel(operations_snapshot), "风险告警")
    tabs.addTab(build_import_audit_panel(operations_snapshot), "导入审计")
    tabs.addTab(build_report_replay_panel(operations_snapshot), "报告与回放")

    window.setCentralWidget(tabs)
    window.resize(1440, 900)
    return window
