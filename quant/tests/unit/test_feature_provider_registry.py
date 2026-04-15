from __future__ import annotations

from datetime import date, timedelta

from a_share_quant.domain.models import Bar
from a_share_quant.providers.feature_provider import FeatureProvider


def _bars(closes: list[float]) -> list[Bar]:
    start = date(2024, 1, 1)
    rows: list[Bar] = []
    for index, close in enumerate(closes):
        rows.append(
            Bar(
                ts_code="600000.SH",
                trade_date=start + timedelta(days=index),
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1.0,
                amount=1.0,
            )
        )
    return rows


def test_feature_provider_registry_describes_builtin_features() -> None:
    provider = FeatureProvider()
    features = {item.name: item for item in provider.describe_features()}
    assert {"momentum", "daily_return"}.issubset(features)
    assert features["momentum"].params["lookback"] == "int>=1"
    assert features["daily_return"].required_history_bars == 2


def test_feature_provider_supports_daily_return_batch() -> None:
    provider = FeatureProvider()
    values = provider.compute_feature_batch(
        "daily_return",
        {
            "600000.SH": _bars([10.0, 11.0]),
            "000001.SZ": _bars([5.0, 5.5]),
        },
    )
    assert round(values["600000.SH"], 6) == 0.1
    assert round(values["000001.SZ"], 6) == 0.1
