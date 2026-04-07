"""CSV 数据适配器。"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from a_share_quant.adapters.data.base import MarketDataBundle
from a_share_quant.core.exceptions import DataValidationError
from a_share_quant.core.utils import parse_yyyymmdd
from a_share_quant.domain.models import Bar, Security


REQUIRED_COLUMNS = {
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "suspended",
    "limit_up",
    "limit_down",
    "adj_type",
    "name",
    "exchange",
    "board",
    "is_st",
    "status",
}


class CSVDataAdapter:
    """从标准化 CSV 导入 A 股行情数据。"""

    def load(self, csv_path: str | Path, encoding: str = "utf-8") -> MarketDataBundle:
        """读取 CSV 并转换为领域模型。

        Args:
            csv_path: CSV 文件路径。
            encoding: 文件编码。

        Returns:
            `MarketDataBundle`。

        Raises:
            DataValidationError: 当缺少必要字段、日期非法、价格为空时抛出。
        """
        path = Path(csv_path)
        if not path.exists():
            raise DataValidationError(f"CSV 文件不存在: {path}")
        frame = pd.read_csv(path, encoding=encoding)
        missing = REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            raise DataValidationError(f"CSV 缺少必要字段: {sorted(missing)}")
        bars: list[Bar] = []
        securities: dict[str, Security] = {}
        for _, row in frame.sort_values(["trade_date", "ts_code"]).iterrows():
            trade_date = pd.to_datetime(row["trade_date"]).date()
            ts_code = str(row["ts_code"])
            security = securities.get(ts_code)
            if security is None:
                securities[ts_code] = Security(
                    ts_code=ts_code,
                    name=str(row["name"]),
                    exchange=str(row["exchange"]),
                    board=str(row["board"]),
                    is_st=bool(row["is_st"]),
                    status=str(row["status"]),
                    list_date=parse_yyyymmdd(row.get("list_date")),
                    delist_date=parse_yyyymmdd(row.get("delist_date")),
                )
            pre_close = row.get("pre_close")
            bars.append(
                Bar(
                    ts_code=ts_code,
                    trade_date=date(trade_date.year, trade_date.month, trade_date.day),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                    amount=float(row["amount"]),
                    pre_close=None if pd.isna(pre_close) else float(pre_close),
                    suspended=bool(row["suspended"]),
                    limit_up=bool(row["limit_up"]),
                    limit_down=bool(row["limit_down"]),
                    adj_type=str(row["adj_type"]),
                )
            )
        return MarketDataBundle(bars=bars, securities=securities)
