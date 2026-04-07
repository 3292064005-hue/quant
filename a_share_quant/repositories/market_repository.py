"""行情仓储。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date

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
        """读取证券元数据。

        Args:
            ts_codes: 仅加载指定证券集合。
            as_of_date: 若提供，则可结合 `active_only=True` 过滤历史有效证券池。
            active_only: 是否过滤为给定日期有效证券。
        """
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
            grouped[row["ts_code"]].append(
                Bar(
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
            )
        return dict(grouped)
