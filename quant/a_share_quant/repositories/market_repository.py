"""行情仓储。"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from datetime import date

from a_share_quant.core.utils import now_iso, parse_yyyymmdd
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry
from a_share_quant.storage.sqlite_store import SQLiteStore


class MarketRepository:
    """持久化证券、交易日历与日线数据。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def upsert_securities(self, securities: dict[str, Security], *, source_import_run_id: str | None = None) -> None:
        """写入证券主数据，并记录行级导入来源。"""
        rows = []
        now = now_iso()
        for item in securities.values():
            rows.append(
                (
                    item.ts_code,
                    item.name,
                    item.exchange,
                    item.board,
                    int(item.is_st),
                    item.status,
                    item.list_date.isoformat() if item.list_date else None,
                    item.delist_date.isoformat() if item.delist_date else None,
                    source_import_run_id,
                    now,
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO securities
            (ts_code, name, exchange, board, is_st, status, list_date, delist_date, source_import_run_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def upsert_calendar(self, entries: list[TradingCalendarEntry], *, source_import_run_id: str | None = None) -> None:
        """写入交易日历，并记录行级导入来源。"""
        rows = []
        now = now_iso()
        for item in entries:
            rows.append(
                (
                    item.exchange,
                    item.cal_date.isoformat(),
                    int(item.is_open),
                    item.pretrade_date.isoformat() if item.pretrade_date else None,
                    source_import_run_id,
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO trading_calendar (exchange, cal_date, is_open, pretrade_date, source_import_run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def upsert_bars(self, bars: list[Bar], *, source_import_run_id: str | None = None) -> None:
        """写入日线行情，并记录行级导入来源。"""
        rows = []
        now = now_iso()
        for bar in bars:
            rows.append(
                (
                    bar.ts_code,
                    bar.trade_date.isoformat(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.amount,
                    bar.pre_close,
                    int(bar.suspended),
                    int(bar.limit_up),
                    int(bar.limit_down),
                    bar.adj_type,
                    source_import_run_id,
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO bars_daily
            (ts_code, trade_date, open, high, low, close, volume, amount, pre_close, suspended, limit_up, limit_down, adj_type, source_import_run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def load_securities(self, ts_codes: list[str] | None = None, as_of_date: date | None = None, active_only: bool = False) -> dict[str, Security]:
        """读取证券元数据。"""
        params: list[str] = []
        sql = "SELECT ts_code, name, exchange, board, is_st, status, list_date, delist_date FROM securities"
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            sql += f" WHERE ts_code IN ({placeholders})"
            params.extend(ts_codes)
        rows = self.store.query(sql, tuple(params))
        securities = {
            row["ts_code"]: Security(
                ts_code=row["ts_code"],
                name=row["name"],
                exchange=row["exchange"],
                board=row["board"],
                is_st=bool(row["is_st"]),
                status=row["status"],
                list_date=parse_yyyymmdd(row["list_date"]),
                delist_date=parse_yyyymmdd(row["delist_date"]),
            )
            for row in rows
        }
        if active_only and as_of_date is not None:
            securities = {code: security for code, security in securities.items() if security.is_active_on(as_of_date)}
        return securities

    def load_calendar(
        self,
        exchange: str | None = None,
        *,
        exchanges: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[TradingCalendarEntry]:
        """读取交易日历，可按单交易所或交易所集合过滤。"""
        selected_exchanges = self._normalize_exchange_scope(exchange=exchange, exchanges=exchanges)
        conditions: list[str] = []
        params: list[str] = []
        if selected_exchanges:
            placeholders = ",".join("?" for _ in selected_exchanges)
            conditions.append(f"exchange IN ({placeholders})")
            params.extend(selected_exchanges)
        if start_date is not None:
            conditions.append("cal_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("cal_date <= ?")
            params.append(end_date.isoformat())
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self.store.query(
            f"SELECT exchange, cal_date, is_open, pretrade_date FROM trading_calendar{where_clause} ORDER BY cal_date, exchange",
            tuple(params),
        )
        return [
            TradingCalendarEntry(
                exchange=row["exchange"],
                cal_date=date.fromisoformat(row["cal_date"]),
                is_open=bool(row["is_open"]),
                pretrade_date=parse_yyyymmdd(row["pretrade_date"]),
            )
            for row in rows
        ]

    def load_bar_trade_dates(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> list[date]:
        """仅从 bars 表读取去重交易日，不再隐式冒充正式交易日历。"""
        conditions: list[str] = []
        params: list[str] = []
        if start_date is not None:
            conditions.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("trade_date <= ?")
            params.append(end_date.isoformat())
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            conditions.append(f"ts_code IN ({placeholders})")
            params.extend(ts_codes)
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        bar_rows = self.store.query(f"SELECT DISTINCT trade_date FROM bars_daily{where_clause} ORDER BY trade_date", tuple(params))
        return [date.fromisoformat(row["trade_date"]) for row in bar_rows]

    def load_trade_dates(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        exchange: str | None = None,
        *,
        exchanges: list[str] | None = None,
        open_only: bool = True,
    ) -> list[date]:
        """优先从交易日历读取统一交易日；若日历缺失，则退回 bars 去重日期。"""
        selected_exchanges = self._normalize_exchange_scope(exchange=exchange, exchanges=exchanges)
        conditions: list[str] = []
        params: list[str | int] = []
        if selected_exchanges:
            placeholders = ",".join("?" for _ in selected_exchanges)
            conditions.append(f"exchange IN ({placeholders})")
            params.extend(selected_exchanges)
        if start_date is not None:
            conditions.append("cal_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("cal_date <= ?")
            params.append(end_date.isoformat())
        if open_only:
            conditions.append("is_open = 1")
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self.store.query(f"SELECT DISTINCT cal_date FROM trading_calendar{where_clause} ORDER BY cal_date", tuple(params))
        if rows:
            return [date.fromisoformat(row["cal_date"]) for row in rows]
        bar_conditions: list[str] = []
        bar_params: list[str] = []
        if start_date is not None:
            bar_conditions.append("trade_date >= ?")
            bar_params.append(start_date.isoformat())
        if end_date is not None:
            bar_conditions.append("trade_date <= ?")
            bar_params.append(end_date.isoformat())
        where_clause = f" WHERE {' AND '.join(bar_conditions)}" if bar_conditions else ""
        bar_rows = self.store.query(f"SELECT DISTINCT trade_date FROM bars_daily{where_clause} ORDER BY trade_date", tuple(bar_params))
        return [date.fromisoformat(row["trade_date"]) for row in bar_rows]

    def load_bars_grouped(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> dict[str, list[Bar]]:
        """按证券分组读取行情。"""
        conditions: list[str] = []
        params: list[str] = []
        if start_date is not None:
            conditions.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("trade_date <= ?")
            params.append(end_date.isoformat())
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            conditions.append(f"ts_code IN ({placeholders})")
            params.extend(ts_codes)
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self.store.query(
            f"SELECT ts_code, trade_date, open, high, low, close, volume, amount, pre_close, suspended, limit_up, limit_down, adj_type FROM bars_daily{where_clause} ORDER BY trade_date, ts_code",
            tuple(params),
        )
        grouped: dict[str, list[Bar]] = defaultdict(list)
        for row in rows:
            grouped[row["ts_code"]].append(self._row_to_bar(row))
        return dict(grouped)

    def stream_bars_by_trade_date(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> Iterator[tuple[date, dict[str, Bar]]]:
        """按交易日流式读取行情。

        Args:
            start_date: 可选开始日期；为空时从库内最早交易日开始。
            end_date: 可选结束日期；为空时读到库内最新交易日。
            ts_codes: 可选证券过滤范围。

        Returns:
            逐交易日 ``(trade_date, day_bars)`` 迭代器。

        Boundary Behavior:
            - 若存在交易日历，则按交易日历顺序输出，即使某天无 bar 也返回空字典；
            - 若无交易日历，则退回 bars 表去重日期；
            - 该接口是 ``provider.bar`` 的正式契约，不再暴露空壳实现。
        """
        trade_dates = self.load_trade_dates(start_date=start_date, end_date=end_date)
        return self.iter_day_bars(trade_dates, ts_codes=ts_codes)

    def iter_day_bars(
        self,
        trade_dates: list[date],
        ts_codes: list[str] | None = None,
    ) -> Iterator[tuple[date, dict[str, Bar]]]:
        """按交易日顺序流式返回当日行情。"""
        if not trade_dates:
            return
        start_date = trade_dates[0]
        end_date = trade_dates[-1]
        conditions = ["trade_date >= ?", "trade_date <= ?"]
        params: list[str] = [start_date.isoformat(), end_date.isoformat()]
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            conditions.append(f"ts_code IN ({placeholders})")
            params.extend(ts_codes)
        where_clause = " AND ".join(conditions)
        row_iter = self.store.iterate(
            f"SELECT ts_code, trade_date, open, high, low, close, volume, amount, pre_close, suspended, limit_up, limit_down, adj_type FROM bars_daily WHERE {where_clause} ORDER BY trade_date, ts_code",
            tuple(params),
        )
        current_row = next(row_iter, None)
        for trade_date in trade_dates:
            day_bars: dict[str, Bar] = {}
            while current_row is not None and date.fromisoformat(current_row["trade_date"]) == trade_date:
                bar = self._row_to_bar(current_row)
                day_bars[bar.ts_code] = bar
                current_row = next(row_iter, None)
            yield trade_date, day_bars

    def load_history_window(self, ts_codes: list[str], end_date: date, window: int) -> dict[str, list[Bar]]:
        """按证券读取截至某日的最近 N 根 bar。"""
        if not ts_codes or window <= 0:
            return {}
        placeholders = ",".join("?" for _ in ts_codes)
        sql = f"""
            SELECT ts_code, trade_date, open, high, low, close, volume, amount, pre_close, suspended, limit_up, limit_down, adj_type
            FROM bars_daily
            WHERE ts_code IN ({placeholders}) AND trade_date <= ?
            ORDER BY ts_code, trade_date DESC
        """
        rows = self.store.query(sql, tuple(ts_codes) + (end_date.isoformat(),))
        grouped: dict[str, list[Bar]] = defaultdict(list)
        for row in rows:
            if len(grouped[row["ts_code"]]) >= window:
                continue
            grouped[row["ts_code"]].append(self._row_to_bar(row))
        for bars in grouped.values():
            bars.reverse()
        return dict(grouped)

    def load_distinct_import_run_ids(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
        exchanges: list[str] | None = None,
    ) -> list[str]:
        """读取当前数据快照实际涉及的导入运行集合。"""
        import_run_ids: set[str] = set()
        import_run_ids.update(self._query_security_import_run_ids(ts_codes=ts_codes))
        import_run_ids.update(self._query_calendar_import_run_ids(start_date=start_date, end_date=end_date, exchanges=exchanges))
        import_run_ids.update(self._query_bar_import_run_ids(start_date=start_date, end_date=end_date, ts_codes=ts_codes))
        return sorted(import_run_ids)

    @staticmethod
    def _normalize_exchange_scope(*, exchange: str | None = None, exchanges: list[str] | None = None) -> list[str]:
        selected: list[str] = []
        if exchanges:
            selected.extend(item for item in exchanges if item)
        if exchange:
            selected.append(exchange)
        if not selected:
            return []
        return sorted({item for item in selected})

    def _query_security_import_run_ids(self, *, ts_codes: list[str] | None = None) -> list[str]:
        conditions: list[str] = ["source_import_run_id IS NOT NULL"]
        params: list[str] = []
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            conditions.append(f"ts_code IN ({placeholders})")
            params.extend(ts_codes)
        rows = self.store.query(
            f"SELECT DISTINCT source_import_run_id FROM securities WHERE {' AND '.join(conditions)} ORDER BY source_import_run_id",
            tuple(params),
        )
        return [str(row["source_import_run_id"]) for row in rows if row["source_import_run_id"]]

    def _query_calendar_import_run_ids(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        exchanges: list[str] | None = None,
    ) -> list[str]:
        conditions: list[str] = ["source_import_run_id IS NOT NULL"]
        params: list[str] = []
        if exchanges:
            placeholders = ",".join("?" for _ in exchanges)
            conditions.append(f"exchange IN ({placeholders})")
            params.extend(exchanges)
        if start_date is not None:
            conditions.append("cal_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("cal_date <= ?")
            params.append(end_date.isoformat())
        rows = self.store.query(
            f"SELECT DISTINCT source_import_run_id FROM trading_calendar WHERE {' AND '.join(conditions)} ORDER BY source_import_run_id",
            tuple(params),
        )
        return [str(row["source_import_run_id"]) for row in rows if row["source_import_run_id"]]

    def _query_bar_import_run_ids(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        ts_codes: list[str] | None = None,
    ) -> list[str]:
        conditions: list[str] = ["source_import_run_id IS NOT NULL"]
        params: list[str] = []
        if start_date is not None:
            conditions.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("trade_date <= ?")
            params.append(end_date.isoformat())
        if ts_codes:
            placeholders = ",".join("?" for _ in ts_codes)
            conditions.append(f"ts_code IN ({placeholders})")
            params.extend(ts_codes)
        rows = self.store.query(
            f"SELECT DISTINCT source_import_run_id FROM bars_daily WHERE {' AND '.join(conditions)} ORDER BY source_import_run_id",
            tuple(params),
        )
        return [str(row["source_import_run_id"]) for row in rows if row["source_import_run_id"]]

    @staticmethod
    def _row_to_bar(row) -> Bar:
        return Bar(
            ts_code=row["ts_code"],
            trade_date=date.fromisoformat(row["trade_date"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            amount=float(row["amount"]),
            pre_close=None if row["pre_close"] is None else float(row["pre_close"]),
            suspended=bool(row["suspended"]),
            limit_up=bool(row["limit_up"]),
            limit_down=bool(row["limit_down"]),
            adj_type=row["adj_type"],
        )
