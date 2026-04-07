"""行情仓储。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Iterator

from a_share_quant.core.utils import now_iso, parse_yyyymmdd
from a_share_quant.domain.models import Bar, Security, TradingCalendarEntry
from a_share_quant.storage.sqlite_store import SQLiteStore


class MarketRepository:
    """持久化证券、交易日历与日线数据。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def upsert_securities(self, securities: dict[str, Security]) -> None:
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
                    now,
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO securities
            (ts_code, name, exchange, board, is_st, status, list_date, delist_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def upsert_calendar(self, entries: list[TradingCalendarEntry]) -> None:
        rows = []
        now = now_iso()
        for item in entries:
            rows.append(
                (
                    item.exchange,
                    item.cal_date.isoformat(),
                    int(item.is_open),
                    item.pretrade_date.isoformat() if item.pretrade_date else None,
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO trading_calendar (exchange, cal_date, is_open, pretrade_date, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

    def upsert_bars(self, bars: list[Bar]) -> None:
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
                    now,
                )
            )
        self.store.executemany(
            """
            INSERT OR REPLACE INTO bars_daily
            (ts_code, trade_date, open, high, low, close, volume, amount, pre_close, suspended, limit_up, limit_down, adj_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def load_calendar(self, exchange: str | None = None) -> list[TradingCalendarEntry]:
        if exchange:
            rows = self.store.query(
                "SELECT exchange, cal_date, is_open, pretrade_date FROM trading_calendar WHERE exchange = ? ORDER BY cal_date",
                (exchange,),
            )
        else:
            rows = self.store.query("SELECT exchange, cal_date, is_open, pretrade_date FROM trading_calendar ORDER BY exchange, cal_date")
        return [
            TradingCalendarEntry(
                exchange=row["exchange"],
                cal_date=date.fromisoformat(row["cal_date"]),
                is_open=bool(row["is_open"]),
                pretrade_date=parse_yyyymmdd(row["pretrade_date"]),
            )
            for row in rows
        ]

    def load_trade_dates(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        exchange: str | None = None,
        open_only: bool = True,
    ) -> list[date]:
        """优先从交易日历读取交易日；若日历缺失，则退回 bars 去重日期。"""
        conditions: list[str] = []
        params: list[str | int] = []
        if exchange is not None:
            conditions.append("exchange = ?")
            params.append(exchange)
        if start_date is not None:
            conditions.append("cal_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            conditions.append("cal_date <= ?")
            params.append(end_date.isoformat())
        if open_only:
            conditions.append("is_open = 1")
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self.store.query(f"SELECT cal_date FROM trading_calendar{where_clause} ORDER BY cal_date", tuple(params))
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

    def iter_day_bars(
        self,
        trade_dates: list[date],
        ts_codes: list[str] | None = None,
    ) -> Iterator[tuple[date, dict[str, Bar]]]:
        """按交易日顺序流式返回当日行情。

        Args:
            trade_dates: 交易日主轴。对没有 bar 的日期，会返回空字典而不是跳过。
            ts_codes: 可选证券过滤集合。

        Returns:
            逐日 ``(trade_date, day_bars)`` 迭代器。
        """
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
        return {ts_code: list(reversed(bars)) for ts_code, bars in grouped.items()}

    @staticmethod
    def _row_to_bar(row) -> Bar:
        return Bar(
            ts_code=row["ts_code"],
            trade_date=date.fromisoformat(row["trade_date"]),
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            amount=row["amount"],
            pre_close=row["pre_close"],
            suspended=bool(row["suspended"]),
            limit_up=bool(row["limit_up"]),
            limit_down=bool(row["limit_down"]),
            adj_type=row["adj_type"],
        )
