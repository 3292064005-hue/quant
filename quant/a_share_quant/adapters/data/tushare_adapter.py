"""Tushare 数据适配器。"""
from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)


class TushareDataAdapter:
    """从 Tushare Pro 同步 A 股基础数据与日线行情。"""

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
        warnings: list[str] = []
        degradation_flags: list[str] = []
        securities = self._fetch_securities(client, end_date=end_date, warnings=warnings, degradation_flags=degradation_flags)
        if ts_codes is not None:
            securities = {code: security for code, security in securities.items() if code in set(ts_codes)}
        calendar = self._fetch_calendar(client, start_date=start_date, end_date=end_date, exchange=exchange)
        bars = self._fetch_bars(client, start_date=start_date, end_date=end_date, securities=securities, warnings=warnings, degradation_flags=degradation_flags)
        return MarketDataBundle(
            securities=securities,
            calendar=calendar,
            bars=bars,
            warnings=warnings,
            degradation_flags=degradation_flags,
            metadata={"provider": "tushare"},
        )

    def _fetch_securities(self, client: Any, end_date: str, warnings: list[str], degradation_flags: list[str]) -> dict[str, Security]:
        frames: list[pd.DataFrame] = []
        last_error: Exception | None = None
        successful_statuses: list[str] = []
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
                degradation_flags.append(f"stock_basic_status_missing:{list_status}")
                warnings.append(f"Tushare stock_basic list_status={list_status} 拉取失败，已保留其余成功状态")
                logger.warning("Tushare stock_basic 拉取失败 list_status=%s error=%s", list_status, exc)
                continue
            if frame is not None and not frame.empty:
                frames.append(frame)
                successful_statuses.append(list_status)
        if not frames:
            if isinstance(last_error, ExternalServiceTimeoutError):
                raise last_error
            raise DataSourceError("调用 Tushare stock_basic 失败，且未返回任何证券主数据") from last_error
        frame = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
        st_codes = self._fetch_st_codes(client, trade_date=end_date, warnings=warnings, degradation_flags=degradation_flags)
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
        if len(successful_statuses) < 3:
            warnings.append(f"Tushare stock_basic 仅成功覆盖状态={successful_statuses}")
        return securities

    def _fetch_st_codes(self, client: Any, trade_date: str, warnings: list[str], degradation_flags: list[str]) -> set[str]:
        try:
            frame = call_with_timeout(client.stock_st, trade_date=trade_date, timeout_seconds=self._timeout_seconds, operation_name="tushare.stock_st")
        except Exception as exc:
            degradation_flags.append("stock_st_unavailable")
            warnings.append("Tushare stock_st 不可用，ST 标记已退化为名称前缀 best-effort 识别")
            logger.warning("Tushare stock_st 拉取失败 error=%s", exc)
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

    def _fetch_bars(
        self,
        client: Any,
        start_date: str,
        end_date: str,
        securities: dict[str, Security],
        warnings: list[str],
        degradation_flags: list[str],
    ) -> list[Bar]:
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
        except Exception as exc:
            degradation_flags.append("stk_limit_unavailable")
            warnings.append("Tushare stk_limit 不可用，涨跌停状态已退化为 pre_close + 规则推断")
            logger.warning("Tushare stk_limit 拉取失败 error=%s", exc)
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
