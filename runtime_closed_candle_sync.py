from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

from binance import AsyncClient

from logger_setup import setup_logger
from runtime_state import update_symbol_state

logger = setup_logger()

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "futures_1m_history.sqlite3"
KLINE_INTERVAL = "1m"
KLINE_MS = 60_000
SQLITE_BUSY_TIMEOUT_MS = 30_000
SYNC_INTERVAL_SECONDS = 70.0
REQUEST_DELAY_SECONDS = 0.08
REST_FETCH_LIMIT = 5


def _ensure_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS futures_klines_1m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                quote_volume REAL,
                trades INTEGER,
                taker_base_volume REAL,
                taker_quote_volume REAL,
                is_closed INTEGER NOT NULL DEFAULT 1,
                source TEXT NOT NULL DEFAULT 'rest',
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        connection.commit()


def _latest_closed_open_time_ms(now_ms: int | None = None) -> int:
    now_ms = int(now_ms or time.time() * 1000)
    return ((now_ms // KLINE_MS) - 1) * KLINE_MS


def _get_last_open_time(symbol: str) -> int | None:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        row = connection.execute(
            "SELECT MAX(open_time) FROM futures_klines_1m WHERE symbol = ?",
            (symbol,),
        ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _save_closed_klines(symbol: str, klines: list[list], source: str = "rest-runtime") -> int:
    if not klines:
        return 0

    _ensure_db()
    updated_at = int(time.time() * 1000)
    rows = [
        (
            symbol,
            int(item[0]),
            float(item[1]),
            float(item[2]),
            float(item[3]),
            float(item[4]),
            float(item[5]),
            int(item[6]),
            float(item[7]),
            int(item[8]),
            float(item[9]),
            float(item[10]),
            1,
            source,
            updated_at,
        )
        for item in klines
    ]

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        connection.executemany(
            """
            INSERT INTO futures_klines_1m (
                symbol,
                open_time,
                open,
                high,
                low,
                close,
                volume,
                close_time,
                quote_volume,
                trades,
                taker_base_volume,
                taker_quote_volume,
                is_closed,
                source,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, open_time) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                close_time = excluded.close_time,
                quote_volume = excluded.quote_volume,
                trades = excluded.trades,
                taker_base_volume = excluded.taker_base_volume,
                taker_quote_volume = excluded.taker_quote_volume,
                is_closed = excluded.is_closed,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        connection.commit()
    return len(rows)


class RuntimeClosedCandleSyncService:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_requested = False
        self._target_symbols: set[str] = set()
        self._lock = asyncio.Lock()

    def start(self, client: AsyncClient) -> None:
        if self._task and not self._task.done():
            return
        self._stop_requested = False
        self._task = asyncio.create_task(self._run(client))

    async def stop(self) -> None:
        self._stop_requested = True
        if self._task and not self._task.done():
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    async def update_targets(self, symbols: list[str]) -> None:
        async with self._lock:
            self._target_symbols = set(symbols)

    async def _run(self, client: AsyncClient) -> None:
        while not self._stop_requested:
            async with self._lock:
                symbols = sorted(self._target_symbols)

            if not symbols:
                await asyncio.sleep(5.0)
                continue

            for symbol in symbols:
                if self._stop_requested:
                    return
                try:
                    await self._sync_symbol(client, symbol)
                except Exception as exc:
                    logger.warning(
                        f"runtime-candle-sync: {symbol} failed: {type(exc).__name__}: {exc}"
                    )
                await asyncio.sleep(REQUEST_DELAY_SECONDS)

            await asyncio.sleep(SYNC_INTERVAL_SECONDS)

    async def _sync_symbol(self, client: AsyncClient, symbol: str) -> None:
        last_open_time = await asyncio.to_thread(_get_last_open_time, symbol)
        if last_open_time is None:
            return

        latest_closed_open_time = _latest_closed_open_time_ms()
        fetch_start = last_open_time + KLINE_MS
        if fetch_start > latest_closed_open_time:
            update_symbol_state(
                symbol,
                db_sync_status="up_to_date",
                db_sync_last_checked_at=int(time.time() * 1000),
                db_sync_last_open_time=last_open_time,
            )
            return

        klines = await client.futures_klines(
            symbol=symbol,
            interval=KLINE_INTERVAL,
            startTime=fetch_start,
            limit=REST_FETCH_LIMIT,
        )
        closed_klines = [
            item for item in klines
            if int(item[0]) <= latest_closed_open_time and int(item[6]) <= (latest_closed_open_time + KLINE_MS - 1)
        ]
        if not closed_klines:
            update_symbol_state(
                symbol,
                db_sync_status="waiting_closed_candle",
                db_sync_last_checked_at=int(time.time() * 1000),
                db_sync_last_open_time=last_open_time,
            )
            return

        saved = await asyncio.to_thread(_save_closed_klines, symbol, closed_klines)
        synced_open_time = int(closed_klines[-1][0])
        update_symbol_state(
            symbol,
            db_sync_status="saved",
            db_sync_last_checked_at=int(time.time() * 1000),
            db_sync_last_saved_count=saved,
            db_sync_last_open_time=synced_open_time,
        )
        logger.info(
            f"runtime-candle-sync: {symbol} saved_closed_1m={saved}, "
            f"last_open_time={synced_open_time}"
        )


runtime_closed_candle_sync_service = RuntimeClosedCandleSyncService()
