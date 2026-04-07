"""日志配置。"""
from __future__ import annotations

import json
import logging
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """JSON 日志格式化器。"""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
        }
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(log_dir: str) -> None:
    """配置根日志器。

    Args:
        log_dir: 日志目录。

    Returns:
        None。

    Raises:
        OSError: 当日志目录不可创建时抛出。
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    file_handler = logging.FileHandler(Path(log_dir) / "app.log", encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())

    root.addHandler(stream_handler)
    root.addHandler(file_handler)
