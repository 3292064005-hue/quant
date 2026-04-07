"""桌面主窗口。"""
from __future__ import annotations


def build_main_window() -> object:
    """构建主窗口。

    Returns:
        `QMainWindow` 实例。

    Raises:
        ImportError: 当前环境未安装 PySide6 时抛出。
    """
    from PySide6.QtWidgets import QLabel, QMainWindow, QTabWidget, QTextEdit, QVBoxLayout, QWidget

    window = QMainWindow()
    window.setWindowTitle("A 股量化研究与交易工作站")
    tabs = QTabWidget()
    for name in ["数据中心", "策略中心", "回测中心", "风控中心", "执行中心", "账户与审计"]:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(name))
        layout.addWidget(QTextEdit())
        tabs.addTab(page, name)
    window.setCentralWidget(tabs)
    window.resize(1280, 800)
    return window
