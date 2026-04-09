"""数据集提供器。"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from a_share_quant.services.data_service import DataService, LoadedMarketData, StreamingMarketData


@dataclass(frozen=True, slots=True)
class DatasetRequest:
    """研究数据集请求。"""

    start_date: date | None = None
    end_date: date | None = None
    ts_codes: tuple[str, ...] = ()
    as_of_date: date | None = None
    active_only: bool = False
    access_mode: str = "preload"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ts_codes"] = list(self.ts_codes)
        return payload


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    """研究数据集摘要。"""

    request: DatasetRequest
    symbol_count: int
    calendar_count: int
    bar_symbol_count: int
    total_bar_count: int
    dataset_version_id: str | None
    dataset_digest: str
    import_run_ids: tuple[str, ...] = field(default_factory=tuple)
    provider_name: str = "dataset"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["request"] = self.request.to_dict()
        payload["import_run_ids"] = list(self.import_run_ids)
        return payload


class DatasetProvider:
    """把 DataService 暴露为正式 dataset provider。"""

    def __init__(self, data_service: DataService) -> None:
        self.data_service = data_service

    def build_request(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
        access_mode: str = "preload",
    ) -> DatasetRequest:
        """构造标准化数据集请求。"""
        normalized_codes = tuple(ts_codes or ())
        if access_mode not in {"preload", "stream"}:
            raise ValueError(f"不支持的 access_mode: {access_mode}")
        return DatasetRequest(
            start_date=start_date,
            end_date=end_date,
            ts_codes=normalized_codes,
            as_of_date=as_of_date,
            active_only=active_only,
            access_mode=access_mode,
        )

    def load_snapshot(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> LoadedMarketData:
        """加载预加载数据快照。"""
        request = self.build_request(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
            access_mode="preload",
        )
        return self.data_service.load_market_data_bundle(
            start_date=request.start_date,
            end_date=request.end_date,
            ts_codes=list(request.ts_codes) or None,
            as_of_date=request.as_of_date,
            active_only=request.active_only,
        )

    def stream_snapshot(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> StreamingMarketData:
        """加载流式数据快照。"""
        request = self.build_request(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
            access_mode="stream",
        )
        return self.data_service.prepare_stream_market_data(
            start_date=request.start_date,
            end_date=request.end_date,
            ts_codes=list(request.ts_codes) or None,
            as_of_date=request.as_of_date,
            active_only=request.active_only,
        )

    def summarize_snapshot(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        as_of_date: date | None = None,
        active_only: bool = False,
    ) -> DatasetSummary:
        """返回正式研究数据集摘要。"""
        request = self.build_request(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
            access_mode="preload",
        )
        snapshot = self.load_snapshot(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            as_of_date=as_of_date,
            active_only=active_only,
        )
        total_bar_count = sum(len(bars) for bars in snapshot.bars_by_symbol.values())
        return DatasetSummary(
            request=request,
            symbol_count=len(snapshot.securities),
            calendar_count=len(snapshot.trade_calendar),
            bar_symbol_count=len(snapshot.bars_by_symbol),
            total_bar_count=total_bar_count,
            dataset_version_id=snapshot.data_lineage.dataset_version_id,
            dataset_digest=snapshot.data_lineage.dataset_digest or "",
            import_run_ids=tuple(snapshot.data_lineage.import_run_ids),
            provider_name="provider.dataset",
        )
