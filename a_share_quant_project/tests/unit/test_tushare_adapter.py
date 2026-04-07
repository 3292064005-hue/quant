from datetime import date

import pandas as pd

from a_share_quant.adapters.data.tushare_adapter import TushareDataAdapter


class _FakeTushareClient:
    def stock_basic(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "exchange": "SSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19991110",
                    "delist_date": None,
                },
                {
                    "ts_code": "300001.SZ",
                    "name": "特锐德",
                    "exchange": "SZSE",
                    "market": "创业板",
                    "list_status": "L",
                    "list_date": "20091030",
                    "delist_date": None,
                },
            ]
        )

    def stock_st(self, **kwargs):
        return pd.DataFrame([{"ts_code": "600000.SH"}])

    def trade_cal(self, **kwargs):
        return pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260105", "is_open": "1", "pretrade_date": "20260102"},
                {"exchange": "SSE", "cal_date": "20260106", "is_open": "1", "pretrade_date": "20260105"},
            ]
        )

    def daily(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260105",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.9,
                    "close": 11.0,
                    "pre_close": 10.0,
                    "vol": 1000,
                    "amount": 10000,
                },
                {
                    "ts_code": "300001.SZ",
                    "trade_date": "20260105",
                    "open": 20.0,
                    "high": 24.0,
                    "low": 19.0,
                    "close": 24.0,
                    "pre_close": 20.0,
                    "vol": 2000,
                    "amount": 40000,
                },
            ]
        )

    def stk_limit(self, **kwargs):
        return pd.DataFrame(
            [
                {"ts_code": "600000.SH", "trade_date": "20260105", "up_limit": 11.0, "down_limit": 9.0},
                {"ts_code": "300001.SZ", "trade_date": "20260105", "up_limit": 24.0, "down_limit": 16.0},
            ]
        )


def test_tushare_adapter_maps_bundle() -> None:
    adapter = TushareDataAdapter(token="dummy", client=_FakeTushareClient())
    bundle = adapter.fetch_bundle(start_date="20260101", end_date="20260106")
    assert len(bundle.securities) == 2
    assert bundle.securities["600000.SH"].is_st is True
    assert bundle.securities["300001.SZ"].board == "创业板"
    assert len(bundle.calendar) == 2
    assert len(bundle.bars) == 2
    sh_bar = next(item for item in bundle.bars if item.ts_code == "600000.SH")
    cy_bar = next(item for item in bundle.bars if item.ts_code == "300001.SZ")
    assert sh_bar.limit_up is True
    assert cy_bar.limit_up is True
    assert sh_bar.trade_date == date(2026, 1, 5)


class _PartiallyFailingTushareClient(_FakeTushareClient):
    def stock_basic(self, **kwargs):
        if kwargs.get("list_status") == "D":
            raise RuntimeError("temporary failure")
        return super().stock_basic(**kwargs)


def test_tushare_adapter_preserves_successful_status_frames_when_one_status_fails() -> None:
    adapter = TushareDataAdapter(token="dummy", client=_PartiallyFailingTushareClient())
    bundle = adapter.fetch_bundle(start_date="20260101", end_date="20260106")
    assert len(bundle.securities) == 2


import pytest

from a_share_quant.core.exceptions import DataSourceError


def test_tushare_adapter_requires_token_when_no_client_injected(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    adapter = TushareDataAdapter(token=None, token_env="TUSHARE_TOKEN")
    with pytest.raises(DataSourceError):
        adapter.fetch_bundle(start_date="20260101", end_date="20260106")
