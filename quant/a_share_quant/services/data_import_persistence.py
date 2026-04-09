"""市场数据导入持久化。"""
from __future__ import annotations

from a_share_quant.adapters.data.base import MarketDataBundle
from a_share_quant.repositories.data_import_repository import DataImportRepository
from a_share_quant.repositories.market_repository import MarketRepository


class DataImportPersistence:
    """负责把外部数据包持久化并补写导入审计。"""

    def __init__(
        self,
        market_repository: MarketRepository,
        data_import_repository: DataImportRepository | None,
    ) -> None:
        self.market_repository = market_repository
        self.data_import_repository = data_import_repository
        self.last_import_run_id: str | None = None

    def persist(self, bundle: MarketDataBundle, *, source: str, request_context: dict) -> str | None:
        """以显式事务落库市场数据，并返回最新导入运行 ID。"""
        import_run_id: str | None = None
        self.last_import_run_id = None
        repository = self.data_import_repository
        if repository is not None:
            import_run_id = repository.create_run(source=source, request_context=request_context)
            self.last_import_run_id = import_run_id
        try:
            with self.market_repository.store.transaction():
                self.market_repository.upsert_securities(bundle.securities, source_import_run_id=import_run_id)
                self.market_repository.upsert_calendar(bundle.calendar, source_import_run_id=import_run_id)
                self.market_repository.upsert_bars(bundle.bars, source_import_run_id=import_run_id)
                if import_run_id is not None:
                    self._write_quality_events(import_run_id, bundle)
            if import_run_id is not None and repository is not None:
                repository.finish_run(
                    import_run_id,
                    status="COMPLETED",
                    securities_count=len(bundle.securities),
                    calendar_count=len(bundle.calendar),
                    bars_count=len(bundle.bars),
                    degradation_flags=bundle.degradation_flags,
                    warnings=bundle.warnings,
                )
        except Exception as exc:
            if import_run_id is not None and repository is not None:
                repository.write_quality_event(
                    import_run_id,
                    event_type="import_failed",
                    payload={
                        "error": str(exc),
                        "source": source,
                        "requested_symbols": request_context.get("ts_codes", []),
                    },
                    level="ERROR",
                )
                repository.finish_run(
                    import_run_id,
                    status="FAILED",
                    securities_count=len(bundle.securities),
                    calendar_count=len(bundle.calendar),
                    bars_count=len(bundle.bars),
                    degradation_flags=bundle.degradation_flags,
                    warnings=bundle.warnings,
                    error_message=str(exc),
                )
            raise
        return import_run_id

    def _write_quality_events(self, import_run_id: str, bundle: MarketDataBundle) -> None:
        repository = self.data_import_repository
        if repository is None:
            return
        repository.write_quality_event(
            import_run_id,
            event_type="row_count_summary",
            payload={
                "securities_count": len(bundle.securities),
                "calendar_count": len(bundle.calendar),
                "bars_count": len(bundle.bars),
            },
            level="INFO",
        )
        if not bundle.calendar and bundle.bars:
            repository.write_quality_event(
                import_run_id,
                event_type="calendar_missing",
                payload={"calendar_policy": "resolved_later_by_trading_session_service", "bars_count": len(bundle.bars)},
                level="WARNING",
            )
        for flag in bundle.degradation_flags:
            repository.write_quality_event(
                import_run_id,
                event_type="degradation_flag",
                payload={"flag": flag},
                level="WARNING",
            )
        for warning in bundle.warnings:
            repository.write_quality_event(
                import_run_id,
                event_type="provider_warning",
                payload={"warning": warning},
                level="WARNING",
            )
