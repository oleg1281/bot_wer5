from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

from logger_setup import setup_logger
from runtime_state import update_symbol_state

logger = setup_logger()

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "futures_1m_history.sqlite3"
KLINE_MS = 60_000
SQLITE_BUSY_TIMEOUT_MS = 30_000


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


def _save_closed_stream_kline(symbol: str, event: dict) -> int:
    _ensure_db()
    updated_at = int(time.time() * 1000)
    row = (
        symbol,
        int(event["open_time"]),
        float(event["open"]),
        float(event["high"]),
        float(event["low"]),
        float(event["close"]),
        float(event["volume"]),
        int(event["close_time"]),
        float(event["quote_volume"]) if event.get("quote_volume") is not None else None,
        int(event["trades"]) if event.get("trades") is not None else None,
        float(event["taker_base_volume"]) if event.get("taker_base_volume") is not None else None,
        float(event["taker_quote_volume"]) if event.get("taker_quote_volume") is not None else None,
        1,
        "stream",
        updated_at,
    )

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute(
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
            row,
        )
        connection.commit()
    return int(event["open_time"])


class StreamClosedCandleWriter:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()
        self._stop_requested = False

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_requested = False
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_requested = True
        if self._task and not self._task.done():
            await self._queue.put(("", {}))
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    def enqueue(self, symbol: str, event: dict) -> None:
        if not symbol or not event:
            return
        self._queue.put_nowait((symbol, dict(event)))

    async def _run(self) -> None:
        while True:
            symbol, event = await self._queue.get()
            if self._stop_requested and not symbol:
                self._queue.task_done()
                return
            try:
                saved_open_time = await asyncio.to_thread(_save_closed_stream_kline, symbol, event)
                latest_closed_open_time = ((int(time.time() * 1000) // KLINE_MS) - 1) * KLINE_MS
                lag_candles = max(0, (latest_closed_open_time - saved_open_time) // KLINE_MS)
                update_symbol_state(
                    symbol,
                    history_status="ready" if lag_candles == 0 else "partial",
                    history_ready=lag_candles == 0,
                    history_last_open_time=saved_open_time,
                    history_lag_candles=lag_candles,
                    db_sync_status="saved_from_stream",
                    db_sync_last_open_time=saved_open_time,
                    db_sync_last_saved_count=1,
                    db_sync_last_checked_at=int(time.time() * 1000),
                )
            except Exception as exc:
                logger.error(
                    f"stream-candle-writer: {symbol} failed to save closed stream candle: "
                    f"{type(exc).__name__}: {exc}"
                )
                update_symbol_state(
                    symbol,
                    db_sync_status="stream_save_error",
                    db_sync_error=f"{type(exc).__name__}: {exc}",
                    db_sync_last_checked_at=int(time.time() * 1000),
                )
            finally:
                self._queue.task_done()


stream_closed_candle_writer = StreamClosedCandleWriter()
