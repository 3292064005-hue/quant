"""PySide6 面板通用构建工具。"""
from __future__ import annotations

from typing import Any


def _stringify(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_stringify(item) for item in value) or "-"
    if isinstance(value, dict):
        return ", ".join(f"{key}={_stringify(val)}" for key, val in value.items()) or "-"
    return str(value)


def build_page(title: str, sections: list[object]) -> object:
    from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.addWidget(QLabel(title))
    for section in sections:
        layout.addWidget(section)
    layout.addStretch(1)
    return page


def build_key_value_group(title: str, payload: dict[str, Any]) -> object:
    from PySide6.QtWidgets import QGridLayout, QGroupBox, QLabel

    box = QGroupBox(title)
    grid = QGridLayout(box)
    for row_index, (key, value) in enumerate(payload.items()):
        grid.addWidget(QLabel(str(key)), row_index, 0)
        value_label = QLabel(_stringify(value))
        value_label.setWordWrap(True)
        grid.addWidget(value_label, row_index, 1)
    return box


def build_table_group(title: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> object:
    from PySide6.QtWidgets import QGroupBox, QTableWidget, QTableWidgetItem, QVBoxLayout

    box = QGroupBox(title)
    layout = QVBoxLayout(box)
    table = QTableWidget(len(rows), len(columns))
    table.setHorizontalHeaderLabels([header for header, _ in columns])
    for row_index, row in enumerate(rows):
        for column_index, (_, key) in enumerate(columns):
            table.setItem(row_index, column_index, QTableWidgetItem(_stringify(row.get(key))))
    table.resizeColumnsToContents()
    table.setEditTriggers(table.EditTrigger.NoEditTriggers)
    layout.addWidget(table)
    return box


def build_text_group(title: str, text: str) -> object:
    from PySide6.QtWidgets import QGroupBox, QLabel, QVBoxLayout

    box = QGroupBox(title)
    layout = QVBoxLayout(box)
    label = QLabel(text)
    label.setWordWrap(True)
    layout.addWidget(label)
    return box
