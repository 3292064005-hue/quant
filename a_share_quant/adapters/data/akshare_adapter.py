"""AKShare 数据适配器。"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from a_share_quant.adapters.data.base import MarketDataBundle
from a_share_quant.core.exceptions import DataSourceError, ExternalDependencyError, ExternalServiceTimeoutError
from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.core.timeout_utils import call_with_timeout
from a_share_quant.core.utils import parse_yyyymmdd
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry


class AKShareDataAdapter:
    """从 AKShare 同步 A 股日线。

    Notes:
        AKShare 的历史日线接口可直接获取日线，但 ST 历史、涨跌停价格和
        交易日历并不像 Tushare 那样统一、稳定地通过单一接口暴露。因此：
        - ST 标记采用名称前缀 best-effort 推断；
        - 日涨跌停状态优先使用 `pre_close + 常规板块规则` 推断；
        - 交易日历由抓回的 bar 日期集合反推生成，仅作为研究辅助使用。
    """

    def __init__(self, client: Any | None = None, adj_type: str = "", timeout_seconds: float | None = None) -> None:
        self._client = client
        self._adj_type = adj_type
        self._timeout_seconds = timeout_seconds

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import akshare as ak
        except ImportError as exc:
            raise ExternalDependencyError("未安装 akshare；请执行 pip install '.[akshare]' 或 pip install akshare") from exc
        self._client = ak
        return self._client

    def fetch_bundle(
        self,
        start_date: str,
        end_date: str,
        ts_codes: list[str] | None = None,
        exchange: str = "SSE",
    ) -> MarketDataBundle:
        client = self._get_client()
        securities = self._fetch_securities(client)
        if ts_codes is not None:
            target_codes = set(ts_codes)
            securities = {code: item for code, item in securities.items() if code in target_codes}
        bars = self._fetch_bars(client, start_date=start_date, end_date=end_date, securities=securities)
        calendar = self._build_calendar_from_bars(exchange=exchange, bars=bars)
        return MarketDataBundle(securities=securities, bars=bars, calendar=calendar)

    def _fetch_securities(self, client: Any) -> dict[str, Security]:
        frame = None
        if hasattr(client, "stock_info_a_code_name"):
            frame = call_with_timeout(client.stock_info_a_code_name, timeout_seconds=self._timeout_seconds, operation_name="akshare.stock_info_a_code_name")
        elif hasattr(client, "stock_zh_a_spot_em"):
            spot = call_with_timeout(client.stock_zh_a_spot_em, timeout_seconds=self._timeout_seconds, operation_name="akshare.stock_zh_a_spot_em")
            if spot is not None and not spot.empty:
                frame = spot[["代码", "名称"]].copy()
        if frame is None or frame.empty:
            raise DataSourceError("AKShare 未返回可用的股票基础列表")
        securities: dict[str, Security] = {}
        for _, row in frame.iterrows():
            code = str(row.get("code", row.get("代码", ""))).strip()
            name = str(row.get("name", row.get("名称", ""))).strip()
            if not code:
                continue
            ts_code = self._to_ts_code(code)
            securities[ts_code] = Security(
                ts_code=ts_code,
                name=name,
                exchange=self._infer_exchange(code),
                board=self._infer_board(code),
                is_st=name.upper().startswith(("ST", "*ST")),
                status="L",
                list_date=None,
                delist_date=None,
            )
        return securities

    def _fetch_bars(self, client: Any, start_date: str, end_date: str, securities: dict[str, Security]) -> list[Bar]:
        bars: list[Bar] = []
        for ts_code, security in sorted(securities.items()):
            symbol = ts_code.split(".")[0]
            try:
                frame = call_with_timeout(
                    client.stock_zh_a_hist,
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=self._adj_type,
                    timeout_seconds=self._timeout_seconds,
                    operation_name=f"akshare.stock_zh_a_hist[{ts_code}]",
                )
            except ExternalServiceTimeoutError:
                raise
            except Exception as exc:
                raise DataSourceError(f"AKShare 获取日线失败: {ts_code}") from exc
            if frame is None or frame.empty:
                continue
            normalized = self._normalize_daily_frame(frame)
            previous_close: float | None = None
            for _, row in normalized.iterrows():
                current_pre_close = row["pre_close"]
                if current_pre_close is None and previous_close is not None:
                    current_pre_close = previous_close
                limit_up, limit_down = MarketRules.infer_limit_state(float(row["close"]), current_pre_close, security)
                bars.append(
                    Bar(
                        ts_code=ts_code,
                        trade_date=parse_yyyymmdd(row["trade_date"]) or date.today(),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                        amount=float(row["amount"]),
                        pre_close=current_pre_close,
                        suspended=False,
                        limit_up=limit_up,
                        limit_down=limit_down,
                        adj_type=self._adj_type,
                    )
                )
                previous_close = float(row["close"])
        return bars

    @staticmethod
    def _normalize_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            "日期": "trade_date",
            "date": "trade_date",
            "开盘": "open",
            "open": "open",
            "收盘": "close",
            "close": "close",
            "最高": "high",
            "high": "high",
            "最低": "low",
            "low": "low",
            "成交量": "volume",
            "volume": "volume",
            "amount": "volume",
            "成交额": "amount",
            "turnover": "amount",
            "涨跌额": "change",
            "change": "change",
        }
        normalized = frame.rename(columns=rename_map).copy()
        required = {"trade_date", "open", "close", "high", "low"}
        if not required.issubset(normalized.columns):
            raise DataSourceError(f"AKShare 日线字段不完整，实际字段: {sorted(normalized.columns)}")
        if "volume" not in normalized.columns:
            normalized["volume"] = 0.0
        if "amount" not in normalized.columns:
            normalized["amount"] = 0.0
        normalized = normalized.sort_values("trade_date")
        normalized["pre_close"] = normalized["close"].shift(1)
        if "change" in normalized.columns:
            normalized["pre_close"] = normalized.apply(
                lambda row: float(row["close"]) - float(row["change"]) if pd.notna(row["change"]) else row["pre_close"],
                axis=1,
            )
        normalized = normalized[["trade_date", "open", "close", "high", "low", "volume", "amount", "pre_close"]]
        return normalized

    @staticmethod
    def _build_calendar_from_bars(exchange: str, bars: list[Bar]) -> list[TradingCalendarEntry]:
        dates = sorted({bar.trade_date for bar in bars})
        calendar: list[TradingCalendarEntry] = []
        previous: date | None = None
        for item in dates:
            calendar.append(TradingCalendarEntry(exchange=exchange, cal_date=item, is_open=True, pretrade_date=previous))
            previous = item
        return calendar

    @staticmethod
    def _to_ts_code(code: str) -> str:
        if code.startswith(("6", "5")):
            return f"{code}.SH"
        if code.startswith(("0", "1", "2", "3")):
            return f"{code}.SZ"
        if code.startswith(("4", "8", "9")):
            return f"{code}.BJ"
        return code

    @staticmethod
    def _infer_exchange(code: str) -> str:
        if code.startswith(("6", "5")):
            return "SSE"
        if code.startswith(("0", "1", "2", "3")):
            return "SZSE"
        if code.startswith(("4", "8", "9")):
            return "BSE"
        return "UNKNOWN"

    @staticmethod
    def _infer_board(code: str) -> str:
        if code.startswith(("688", "689")):
            return "科创板"
        if code.startswith(("300", "301")):
            return "创业板"
        if code.startswith(("4", "8", "9")):
            return "北交所"
        return "主板"
