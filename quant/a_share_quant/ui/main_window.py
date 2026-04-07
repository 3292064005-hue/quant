"""桌面原型主窗口。"""
from __future__ import annotations

import json
from typing import Any

from a_share_quant.config.models import AppConfig


def build_main_window(*, config: AppConfig, runtime_results: list[dict[str, Any]] | None = None) -> object:
    """构建桌面原型主窗口。

    功能:
        展示配置摘要、运行时检查结果以及当前桌面层边界说明。

    Args:
        config: 已解析的应用配置。
        runtime_results: 运行时检查结果列表，可为空。

    Returns:
        `QMainWindow` 实例。

    Raises:
        ImportError: 当前环境未安装 PySide6 时抛出。
    """
    from PySide6.QtWidgets import QLabel, QMainWindow, QPlainTextEdit, QTabWidget, QVBoxLayout, QWidget

    runtime_results = runtime_results or []
    window = QMainWindow()
    window.setWindowTitle("A 股量化研究与交易工作站 - 桌面原型（未接交易主链）")
    tabs = QTabWidget()

    config_summary = {
        "app": {
            "name": config.app.name,
            "environment": config.app.environment,
            "timezone": config.app.timezone,
            "logs_dir": config.app.logs_dir,
        },
        "data": {
            "provider": config.data.provider,
            "storage_dir": config.data.storage_dir,
            "reports_dir": config.data.reports_dir,
        },
        "database": {"path": config.database.path},
        "backtest": {
            "data_access_mode": config.backtest.data_access_mode,
            "report_name_template": config.backtest.report_name_template,
            "benchmark_symbol": config.backtest.benchmark_symbol,
        },
        "broker": {
            "provider": config.broker.provider,
            "endpoint": config.broker.endpoint,
            "account_id": config.broker.account_id,
            "strict_contract_mapping": config.broker.strict_contract_mapping,
        },
    }

    tabs.addTab(
        _build_readonly_page(
            title="原型边界",
            body=(
                "当前窗口是桌面原型，不承载回测/下单/报表重建业务操作。\n\n"
                "它只做三件事：\n"
                "1. 展示已解析配置\n"
                "2. 展示运行时健康检查结果\n"
                "3. 明确告知桌面层尚未接入交易主链，避免误判为可操作工作台\n"
            ),
        ),
        "边界说明",
    )
    tabs.addTab(
        _build_readonly_page(
            title="配置摘要",
            body=json.dumps(config_summary, ensure_ascii=False, indent=2),
        ),
        "配置摘要",
    )
    tabs.addTab(
        _build_readonly_page(
            title="运行时检查",
            body=json.dumps(runtime_results, ensure_ascii=False, indent=2),
        ),
        "运行时检查",
    )

    window.setCentralWidget(tabs)
    window.resize(1280, 800)
    return window


def _build_readonly_page(*, title: str, body: str) -> object:
    from PySide6.QtWidgets import QLabel, QPlainTextEdit, QVBoxLayout, QWidget

    page = QWidget()
    layout = QVBoxLayout(page)
    label = QLabel(title)
    editor = QPlainTextEdit()
    editor.setReadOnly(True)
    editor.setPlainText(body)
    layout.addWidget(label)
    layout.addWidget(editor)
    return page
