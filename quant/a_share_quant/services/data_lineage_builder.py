"""数据谱系与 digest 计算。"""
from __future__ import annotations

import hashlib
import json
from datetime import date

from a_share_quant.config.models import DataSection
from a_share_quant.domain.models import Bar, DataImportRun, DataLineage, Security, TradingCalendarEntry
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.dataset_version_repository import DatasetVersionRepository
from a_share_quant.repositories.market_repository import MarketRepository
from a_share_quant.services.data_service_types import StreamLineageTracker, update_digest


class DataLineageBuilder:
    """负责数据谱系与 dataset digest 计算。"""

    def __init__(
        self,
        market_repository: MarketRepository,
        data_config: DataSection,
        data_import_repository: DataImportRepository | None = None,
        dataset_version_repository: DatasetVersionRepository | None = None,
    ) -> None:
        self.market_repository = market_repository
        self.data_config = data_config
        self.data_import_repository = data_import_repository
        self.dataset_version_repository = dataset_version_repository

    def build_data_lineage(
        self,
        *,
        bars_by_symbol: dict[str, list[Bar]],
        securities: dict[str, Security],
        trade_calendar: list[TradingCalendarEntry],
        exchange_scope: list[str],
        explicit_start_date: date | None = None,
        explicit_end_date: date | None = None,
        requested_ts_codes: list[str] | None = None,
    ) -> DataLineage:
        all_dates = sorted({bar.trade_date for bars in bars_by_symbol.values() for bar in bars})
        data_start_date = explicit_start_date or (all_dates[0] if all_dates else None)
        data_end_date = explicit_end_date or (all_dates[-1] if all_dates else None)
        digest_payload = {
            "bars": [
                {
                    "ts_code": bar.ts_code,
                    "trade_date": bar.trade_date.isoformat(),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "amount": bar.amount,
                    "adj_type": bar.adj_type,
                }
                for ts_code in sorted(bars_by_symbol)
                for bar in bars_by_symbol[ts_code]
            ],
            "securities": [
                {
                    "ts_code": sec.ts_code,
                    "exchange": sec.exchange,
                    "board": sec.board,
                    "status": sec.status,
                    "list_date": sec.list_date.isoformat() if sec.list_date else None,
                    "delist_date": sec.delist_date.isoformat() if sec.delist_date else None,
                }
                for sec in sorted(securities.values(), key=lambda item: item.ts_code)
            ],
            "trade_calendar": [
                {
                    "exchange": item.exchange,
                    "cal_date": item.cal_date.isoformat(),
                    "is_open": item.is_open,
                    "pretrade_date": item.pretrade_date.isoformat() if item.pretrade_date else None,
                }
                for item in trade_calendar
            ],
        }
        dataset_digest = hashlib.sha256(json.dumps(digest_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        return self._finalize_data_lineage(
            dataset_digest=dataset_digest,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            requested_ts_codes=requested_ts_codes,
            exchange_scope=exchange_scope,
            securities=securities,
            trade_calendar=trade_calendar,
            bar_symbols=sorted(bars_by_symbol),
            bar_count=sum(len(item) for item in bars_by_symbol.values()),
            trade_date_count=len(all_dates),
        )

    def build_stream_data_lineage(
        self,
        *,
        securities: dict[str, Security],
        trade_dates: list[date],
        exchange_scope: list[str],
        explicit_start_date: date | None = None,
        explicit_end_date: date | None = None,
        requested_ts_codes: list[str] | None = None,
    ) -> DataLineage:
        data_start_date = explicit_start_date or (trade_dates[0] if trade_dates else None)
        data_end_date = explicit_end_date or (trade_dates[-1] if trade_dates else None)
        calendar = self.market_repository.load_calendar(exchanges=exchange_scope, start_date=data_start_date, end_date=data_end_date)
        digest = hashlib.sha256()
        update_digest(digest, {"section": "securities"})
        for security in sorted(securities.values(), key=lambda item: item.ts_code):
            update_digest(
                digest,
                {
                    "ts_code": security.ts_code,
                    "exchange": security.exchange,
                    "board": security.board,
                    "status": security.status,
                    "list_date": security.list_date.isoformat() if security.list_date else None,
                    "delist_date": security.delist_date.isoformat() if security.delist_date else None,
                },
            )
        update_digest(digest, {"section": "trade_calendar"})
        for item in calendar:
            update_digest(
                digest,
                {
                    "exchange": item.exchange,
                    "cal_date": item.cal_date.isoformat(),
                    "is_open": item.is_open,
                    "pretrade_date": item.pretrade_date.isoformat() if item.pretrade_date else None,
                },
            )
        update_digest(digest, {"section": "bars"})
        bar_symbols: set[str] = set()
        bar_count = 0
        stream_iter = self.market_repository.iter_day_bars(trade_dates, ts_codes=requested_ts_codes)
        for trade_date, day_bars in stream_iter:
            for ts_code in sorted(day_bars):
                bar = day_bars[ts_code]
                bar_symbols.add(ts_code)
                bar_count += 1
                update_digest(
                    digest,
                    {
                        "ts_code": bar.ts_code,
                        "trade_date": trade_date.isoformat(),
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "amount": bar.amount,
                        "adj_type": bar.adj_type,
                    },
                )
        dataset_digest = digest.hexdigest()
        return self._finalize_data_lineage(
            dataset_digest=dataset_digest,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            requested_ts_codes=requested_ts_codes,
            exchange_scope=exchange_scope,
            securities=securities,
            trade_calendar=calendar,
            bar_symbols=sorted(bar_symbols),
            bar_count=bar_count,
            trade_date_count=len(trade_dates),
        )

    def load_latest_data_lineage(self) -> DataLineage:
        securities = self.market_repository.load_securities()
        bars_by_symbol = self.market_repository.load_bars_grouped()
        exchange_scope = self._resolve_exchange_scope(securities=securities, requested_ts_codes=None)
        calendar = self.market_repository.load_calendar(exchanges=exchange_scope)
        return self.build_data_lineage(
            bars_by_symbol=bars_by_symbol,
            securities=securities,
            trade_calendar=calendar,
            exchange_scope=exchange_scope,
        )

    def prepare_stream_tracker(
        self,
        *,
        securities: dict[str, Security],
        trade_dates: list[date],
        exchange_scope: list[str],
        data_start_date: date | None,
        data_end_date: date | None,
        requested_ts_codes: list[str] | None,
    ) -> tuple[StreamLineageTracker, DataLineage]:
        calendar = self.market_repository.load_calendar(exchanges=exchange_scope, start_date=data_start_date, end_date=data_end_date)
        digest = hashlib.sha256()
        update_digest(digest, {"section": "securities"})
        for security in sorted(securities.values(), key=lambda item: item.ts_code):
            update_digest(
                digest,
                {
                    "ts_code": security.ts_code,
                    "exchange": security.exchange,
                    "board": security.board,
                    "status": security.status,
                    "list_date": security.list_date.isoformat() if security.list_date else None,
                    "delist_date": security.delist_date.isoformat() if security.delist_date else None,
                },
            )
        update_digest(digest, {"section": "trade_calendar"})
        for item in calendar:
            update_digest(
                digest,
                {
                    "exchange": item.exchange,
                    "cal_date": item.cal_date.isoformat(),
                    "is_open": item.is_open,
                    "pretrade_date": item.pretrade_date.isoformat() if item.pretrade_date else None,
                },
            )
        update_digest(digest, {"section": "bars"})
        tracker = StreamLineageTracker(
            base_digest=digest,
            day_batches=self.market_repository.iter_day_bars(trade_dates, ts_codes=requested_ts_codes),
            finalize_callback=lambda *, dataset_digest, bar_symbols, bar_count: self._finalize_data_lineage(
                dataset_digest=dataset_digest,
                data_start_date=data_start_date,
                data_end_date=data_end_date,
                requested_ts_codes=requested_ts_codes,
                exchange_scope=exchange_scope,
                securities=securities,
                trade_calendar=calendar,
                bar_symbols=bar_symbols,
                bar_count=bar_count,
                trade_date_count=len(trade_dates),
            ),
        )
        provisional_lineage = DataLineage(
            data_source="database_snapshot",
            data_start_date=data_start_date.isoformat() if data_start_date else None,
            data_end_date=data_end_date.isoformat() if data_end_date else None,
        )
        return tracker, provisional_lineage

    def _finalize_data_lineage(
        self,
        *,
        dataset_digest: str,
        data_start_date: date | None,
        data_end_date: date | None,
        requested_ts_codes: list[str] | None,
        exchange_scope: list[str],
        securities: dict[str, Security],
        trade_calendar: list[TradingCalendarEntry],
        bar_symbols: list[str],
        bar_count: int,
        trade_date_count: int,
    ) -> DataLineage:
        import_runs = self._resolve_import_runs(
            start_date=data_start_date,
            end_date=data_end_date,
            ts_codes=requested_ts_codes,
            exchange_scope=exchange_scope,
        )
        import_run_ids = sorted({item.import_run_id for item in import_runs})
        degradation_flags = sorted(self._merge_json_array_fields(import_runs, field_name="degradation_flags_json"))
        warnings = sorted(self._merge_json_array_fields(import_runs, field_name="warnings_json"))
        data_source = self._resolve_data_source(import_runs)
        scope_payload = {
            "requested_ts_codes": sorted(requested_ts_codes or []),
            "resolved_symbols": bar_symbols,
            "exchange_scope": exchange_scope,
            "security_count": len(securities),
            "calendar_entry_count": len(trade_calendar),
            "bar_count": bar_count,
            "trade_date_count": trade_date_count,
        }
        dataset_version_id: str | None = None
        if self.dataset_version_repository is not None:
            version = self.dataset_version_repository.create_or_touch(
                dataset_digest=dataset_digest,
                data_source=data_source,
                data_start_date=data_start_date.isoformat() if data_start_date else None,
                data_end_date=data_end_date.isoformat() if data_end_date else None,
                scope=scope_payload,
                import_run_ids=import_run_ids,
                degradation_flags=degradation_flags,
                warnings=warnings,
            )
            dataset_version_id = version.dataset_version_id
        return DataLineage(
            dataset_version_id=dataset_version_id,
            import_run_id=import_run_ids[0] if len(import_run_ids) == 1 else None,
            import_run_ids=import_run_ids,
            data_source=data_source,
            data_start_date=data_start_date.isoformat() if data_start_date else None,
            data_end_date=data_end_date.isoformat() if data_end_date else None,
            dataset_digest=dataset_digest,
            degradation_flags=degradation_flags,
            warnings=warnings,
        )

    def _resolve_import_runs(
        self,
        *,
        start_date: date | None,
        end_date: date | None,
        ts_codes: list[str] | None,
        exchange_scope: list[str],
    ) -> list[DataImportRun]:
        if self.data_import_repository is None:
            return []
        import_run_ids = self.market_repository.load_distinct_import_run_ids(
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            exchanges=exchange_scope,
        )
        if not import_run_ids:
            return []
        runs = self.data_import_repository.list_runs(import_run_ids)
        return [item for item in runs if item.status == "COMPLETED"]

    @staticmethod
    def _merge_json_array_fields(import_runs: list[DataImportRun], *, field_name: str) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for item in import_runs:
            for value in json.loads(getattr(item, field_name) or "[]"):
                if value not in seen:
                    seen.add(value)
                    merged.append(value)
        return merged

    @staticmethod
    def _resolve_data_source(import_runs: list[DataImportRun]) -> str:
        if not import_runs:
            return "database_snapshot"
        sources = sorted({item.source for item in import_runs})
        if len(sources) == 1:
            return sources[0]
        return "mixed_snapshot"

    def _resolve_exchange_scope(self, *, securities: dict[str, Security], requested_ts_codes: list[str] | None) -> list[str]:
        exchanges = {item.exchange for item in securities.values() if item.exchange}
        if exchanges:
            return sorted(exchanges)
        inferred = {self._infer_exchange_from_ts_code(item) for item in requested_ts_codes or []}
        inferred.discard(None)
        if inferred:
            return sorted(str(item) for item in inferred)
        return [self.data_config.default_exchange]

    @staticmethod
    def _infer_exchange_from_ts_code(ts_code: str) -> str | None:
        normalized = str(ts_code).strip().upper()
        if normalized.endswith(".SH"):
            return "SSE"
        if normalized.endswith(".SZ"):
            return "SZSE"
        if normalized.endswith(".BJ"):
            return "BSE"
        return None
