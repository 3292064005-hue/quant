"""Tushare 数据适配器。"""
from __future__ import annotations

import os
from datetime import date
from typing import Any

import pandas as pd

from a_share_quant.adapters.data.base import MarketDataBundle
from a_share_quant.core.exceptions import DataSourceError, ExternalDependencyError, ExternalServiceTimeoutError
from a_share_quant.core.rules.market_rules import MarketRules
from a_share_quant.core.timeout_utils import call_with_timeout
from a_share_quant.core.utils import parse_yyyymmdd
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry


class TushareDataAdapter:
    """从 Tushare Pro 同步 A 股基础数据与日线行情。

    依赖的核心接口：stock_basic、trade_cal、daily、stk_limit、stock_st。
    其中 `stock_st` 可能受积分权限影响，因此实现中做了权限失败降级：
    优先使用接口结果，失败后退化为基于证券简称前缀的 best-effort 推断。
    """

    def __init__(
        self,
        token: str | None = None,
        token_env: str = "TUSHARE_TOKEN",
        client: Any | None = None,
        adj_type: str = "",
        timeout_seconds: float | None = None,
    ) -> None:
        self._token = token or os.getenv(token_env)
        self._token_env = token_env
        self._client = client
        self._adj_type = adj_type
        self._timeout_seconds = timeout_seconds

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if not self._token:
            raise DataSourceError(f"未提供 Tushare Token；请设置配置项 data.tushare_token 或环境变量 {self._token_env}")
        try:
            import tushare as ts
        except ImportError as exc:
            raise ExternalDependencyError("未安装 tushare；请执行 pip install '.[tushare]' 或 pip install tushare") from exc
        self._client = ts.pro_api(self._token)
        return self._client

    def fetch_bundle(
        self,
        start_date: str,
        end_date: str,
        ts_codes: list[str] | None = None,
        exchange: str = "SSE",
    ) -> MarketDataBundle:
        client = self._get_client()
        securities = self._fetch_securities(client, end_date=end_date)
        if ts_codes is not None:
            securities = {code: security for code, security in securities.items() if code in set(ts_codes)}
        calendar = self._fetch_calendar(client, start_date=start_date, end_date=end_date, exchange=exchange)
        bars = self._fetch_bars(client, start_date=start_date, end_date=end_date, securities=securities)
        return MarketDataBundle(securities=securities, calendar=calendar, bars=bars)

    def _fetch_securities(self, client: Any, end_date: str) -> dict[str, Security]:
        """抓取证券主数据并尽量覆盖历史生命周期。

        Notes:
            为降低历史回测的幸存者偏差，本实现会尝试同时抓取上市、暂停上市、
            退市状态证券。若第三方接口限制导致某一状态无法返回，则保留已成功
            抓取的状态集合，而不是将失败伪装为全量覆盖。
        """
        frames: list[pd.DataFrame] = []
        last_error: Exception | None = None
        for list_status in ("L", "P", "D"):
            try:
                frame = call_with_timeout(
                    client.stock_basic,
                    exchange="",
                    list_status=list_status,
                    fields="ts_code,name,exchange,market,list_status,list_date,delist_date",
                    timeout_seconds=self._timeout_seconds,
                    operation_name=f"tushare.stock_basic[{list_status}]",
                )
            except Exception as exc:
                last_error = exc
                continue
            if frame is not None and not frame.empty:
                frames.append(frame)
        if not frames:
            if isinstance(last_error, ExternalServiceTimeoutError):
                raise last_error
            raise DataSourceError("调用 Tushare stock_basic 失败，且未返回任何证券主数据") from last_error
        frame = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
        st_codes = self._fetch_st_codes(client, trade_date=end_date)
        securities: dict[str, Security] = {}
        for _, row in frame.iterrows():
            ts_code = str(row["ts_code"])
            name = str(row.get("name", ""))
            securities[ts_code] = Security(
                ts_code=ts_code,
                name=name,
                exchange=self._normalize_exchange(row.get("exchange"), ts_code),
                board=self._normalize_board(row.get("market"), ts_code),
                is_st=ts_code in st_codes or name.upper().startswith(("ST", "*ST")),
                status=str(row.get("list_status", "L")),
                list_date=parse_yyyymmdd(row.get("list_date")),
                delist_date=parse_yyyymmdd(row.get("delist_date")),
            )
        return securities

    def _fetch_st_codes(self, client: Any, trade_date: str) -> set[str]:
        try:
            frame = call_with_timeout(client.stock_st, trade_date=trade_date, timeout_seconds=self._timeout_seconds, operation_name="tushare.stock_st")
        except Exception:
            return set()
        if frame is None or frame.empty or "ts_code" not in frame.columns:
            return set()
        return {str(item) for item in frame["ts_code"].tolist()}

    def _fetch_calendar(self, client: Any, start_date: str, end_date: str, exchange: str) -> list[TradingCalendarEntry]:
        try:
            frame = call_with_timeout(client.trade_cal, exchange=exchange, start_date=start_date, end_date=end_date, timeout_seconds=self._timeout_seconds, operation_name="tushare.trade_cal")
        except ExternalServiceTimeoutError:
            raise
        except Exception as exc:
            raise DataSourceError("调用 Tushare trade_cal 失败") from exc
        if frame is None or frame.empty:
            return []
        entries: list[TradingCalendarEntry] = []
        for _, row in frame.iterrows():
            entries.append(
                TradingCalendarEntry(
                    exchange=str(row.get("exchange", exchange)),
                    cal_date=parse_yyyymmdd(row.get("cal_date")) or parse_yyyymmdd(row.get("trade_date")) or date.today(),
                    is_open=str(row.get("is_open", "0")) == "1" or int(row.get("is_open", 0)) == 1,
                    pretrade_date=parse_yyyymmdd(row.get("pretrade_date")),
                )
            )
        return entries

    def _fetch_bars(self, client: Any, start_date: str, end_date: str, securities: dict[str, Security]) -> list[Bar]:
        ts_code_arg = ",".join(sorted(securities)) if securities else None
        try:
            frame = call_with_timeout(client.daily, ts_code=ts_code_arg, start_date=start_date, end_date=end_date, timeout_seconds=self._timeout_seconds, operation_name="tushare.daily")
        except ExternalServiceTimeoutError:
            raise
        except Exception as exc:
            raise DataSourceError("调用 Tushare daily 失败") from exc
        if frame is None or frame.empty:
            return []
        limits_frame: pd.DataFrame | None = None
        try:
            limits_frame = call_with_timeout(client.stk_limit, ts_code=ts_code_arg, start_date=start_date, end_date=end_date, timeout_seconds=self._timeout_seconds, operation_name="tushare.stk_limit")
        except Exception:
            limits_frame = None
        if limits_frame is not None and not limits_frame.empty:
            limits_frame = limits_frame[["ts_code", "trade_date", "up_limit", "down_limit"]].copy()
            frame = frame.merge(limits_frame, on=["ts_code", "trade_date"], how="left")
        frame = frame.sort_values(["ts_code", "trade_date"])
        bars: list[Bar] = []
        for _, row in frame.iterrows():
            ts_code = str(row["ts_code"])
            security = securities.get(ts_code)
            pre_close = row.get("pre_close")
            pre_close_value = None if pd.isna(pre_close) else float(pre_close)
            close_value = float(row["close"])
            up_limit = row.get("up_limit")
            down_limit = row.get("down_limit")
            if pd.isna(up_limit) or pd.isna(down_limit):
                limit_up, limit_down = MarketRules.infer_limit_state(close_value, pre_close_value, security)
            else:
                limit_up = close_value >= float(up_limit) - 1e-9
                limit_down = close_value <= float(down_limit) + 1e-9
            bars.append(
                Bar(
                    ts_code=ts_code,
                    trade_date=parse_yyyymmdd(row["trade_date"]) or date.today(),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=close_value,
                    volume=float(row.get("vol", row.get("volume", 0.0))),
                    amount=float(row.get("amount", 0.0)),
                    pre_close=pre_close_value,
                    suspended=False,
                    limit_up=bool(limit_up),
                    limit_down=bool(limit_down),
                    adj_type=self._adj_type,
                )
            )
        return bars

    @staticmethod
    def _normalize_exchange(exchange: Any, ts_code: str) -> str:
        text = str(exchange or "").upper()
        if text:
            return text
        if ts_code.endswith(".SH"):
            return "SSE"
        if ts_code.endswith(".SZ"):
            return "SZSE"
        if ts_code.endswith(".BJ"):
            return "BSE"
        return "UNKNOWN"

    @staticmethod
    def _normalize_board(market: Any, ts_code: str) -> str:
        text = str(market or "").strip()
        if text:
            mapping = {
                "主板": "主板",
                "创业板": "创业板",
                "科创板": "科创板",
                "北交所": "北交所",
            }
            return mapping.get(text, text)
        code = ts_code.split(".")[0]
        if code.startswith(("688", "689")):
            return "科创板"
        if code.startswith(("300", "301")):
            return "创业板"
        if code.startswith(("8", "4", "9")):
            return "北交所"
        return "主板"
