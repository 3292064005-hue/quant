
from __future__ import annotations

from datetime import date
from typing import cast

from a_share_quant.config.models import DataSection
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.services.data_service import DataService


class _StreamRepo:
    def __init__(self) -> None:
        self.iter_calls = 0

    def load_securities(self, ts_codes=None, as_of_date=None, active_only=False):
        return {"600000.SH": Security(ts_code="600000.SH", name="浦发银行", exchange="SH", board="MAIN")}

    def load_trade_dates(self, start_date=None, end_date=None, exchanges=None, open_only=True):
        return [date(2024, 1, 2)]

    def load_calendar(self, exchange=None, *, exchanges=None, start_date=None, end_date=None):
        return [TradingCalendarEntry(exchange="SH", cal_date=date(2024, 1, 2), is_open=True)]

    def iter_day_bars(self, trade_dates, ts_codes=None):
        self.iter_calls += 1
        yield date(2024, 1, 2), {
            "600000.SH": Bar(
                ts_code="600000.SH",
                trade_date=date(2024, 1, 2),
                open=10.0,
                high=10.2,
                low=9.8,
                close=10.1,
                volume=1000.0,
                amount=10100.0,
            )
        }

    def load_distinct_import_run_ids(self, start_date=None, end_date=None, ts_codes=None, exchanges=None):
        return []


def test_prepare_stream_market_data_uses_single_iter_day_bars_pass(tmp_path) -> None:
    repo = _StreamRepo()
    config = DataSection(storage_dir=str(tmp_path), reports_dir=str(tmp_path / "reports"))
    service = DataService(cast(MarketRepository, repo), config)

    bundle = service.prepare_stream_market_data()
    consumed = list(bundle.day_batches)
    assert bundle.lineage_tracker is not None
    lineage = bundle.lineage_tracker.finalize()

    assert len(consumed) == 1
    assert repo.iter_calls == 1
    assert lineage.dataset_digest is not None
