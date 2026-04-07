import pandas as pd

from a_share_quant.adapters.data.akshare_adapter import AKShareDataAdapter


class _FakeAKClient:
    def stock_info_a_code_name(self):
        return pd.DataFrame([
            {"code": "600000", "name": "浦发银行"},
            {"code": "688001", "name": "华兴源创"},
        ])

    def stock_zh_a_hist(self, symbol, period, start_date, end_date, adjust):
        if symbol == "600000":
            return pd.DataFrame(
                [
                    {"日期": "2026-01-05", "开盘": 10.0, "收盘": 10.0, "最高": 10.1, "最低": 9.9, "成交量": 1000, "成交额": 10000, "涨跌额": 0.0},
                    {"日期": "2026-01-06", "开盘": 10.0, "收盘": 11.0, "最高": 11.0, "最低": 10.0, "成交量": 1000, "成交额": 11000, "涨跌额": 1.0},
                ]
            )
        return pd.DataFrame(
            [
                {"日期": "2026-01-05", "开盘": 20.0, "收盘": 20.0, "最高": 20.5, "最低": 19.8, "成交量": 1000, "成交额": 20000, "涨跌额": 0.0},
                {"日期": "2026-01-06", "开盘": 20.0, "收盘": 24.0, "最高": 24.0, "最低": 19.9, "成交量": 1000, "成交额": 24000, "涨跌额": 4.0},
            ]
        )


def test_akshare_adapter_maps_bundle_and_infers_limits() -> None:
    adapter = AKShareDataAdapter(client=_FakeAKClient())
    bundle = adapter.fetch_bundle(start_date="20260101", end_date="20260110")
    assert len(bundle.securities) == 2
    assert bundle.securities["688001.SH"].board == "科创板"
    assert len(bundle.calendar) == 2
    target_bar = next(item for item in bundle.bars if item.ts_code == "688001.SH" and item.close == 24.0)
    assert target_bar.limit_up is True
