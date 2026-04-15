from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

from binance import AsyncClient

from logger_setup import setup_logger
from runtime_state import get_symbol_state, update_symbol_state

logger = setup_logger()

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "futures_1m_history.sqlite3"
KLINE_INTERVAL = "1m"
KLINE_MS = 60_000
TARGET_WINDOW_CANDLES = 1500
SYNC_REQUEST_DELAY_SECONDS = 0.08
SYNC_SYMBOL_START_DELAY_SECONDS = 0.7


def _ensure_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
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
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_futures_klines_1m_symbol_close_time
            ON futures_klines_1m(symbol, close_time)
            """
        )
        connection.commit()


def _latest_closed_open_time_ms(now_ms: int | None = None) -> int:
    now_ms = int(now_ms or time.time() * 1000)
    return ((now_ms // KLINE_MS) - 1) * KLINE_MS


def _window_start_open_time(latest_closed_open_time: int, onboard_time: int) -> int:
    target_start = latest_closed_open_time - ((TARGET_WINDOW_CANDLES - 1) * KLINE_MS)
    return max(onboard_time, target_start)


def _history_span_candles(latest_closed_open_time: int, onboard_time: int) -> int:
    if onboard_time > latest_closed_open_time:
        return 0
    return ((latest_closed_open_time - onboard_time) // KLINE_MS) + 1


def _get_last_open_time(symbol: str) -> int | None:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as connection:
        row = connection.execute(
            "SELECT MAX(open_time) FROM futures_klines_1m WHERE symbol = ?",
            (symbol,),
        ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _save_closed_klines(symbol: str, klines: list[list]) -> int:
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
            "rest",
            updated_at,
        )
        for item in klines
    ]

    with sqlite3.connect(DB_PATH) as connection:
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


class HistorySyncService:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_requested = False
        self._target_symbols: dict[str, int] = {}
        self._pending_symbols: set[str] = set()
        self._processed_symbols: set[str] = set()
        self._lock = asyncio.Lock()
        self._last_symbol_started_at = 0.0

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

    async def update_targets(self, symbol_onboard_times: dict[str, int]) -> None:
        async with self._lock:
            previous_target_symbols = set(self._target_symbols.keys())
            self._target_symbols = dict(symbol_onboard_times)
            self._pending_symbols.intersection_update(symbol_onboard_times.keys())
            for symbol in symbol_onboard_times:
                symbol_state = get_symbol_state(symbol)
                newly_enabled_target = symbol not in previous_target_symbols
                should_enqueue = (
                    symbol not in self._pending_symbols
                    and (
                        newly_enabled_target
                        or
                        symbol not in self._processed_symbols
                        or not bool(symbol_state.get("history_ready"))
                        or symbol_state.get("history_status") in {"pending", "partial", "desynced", "error"}
                    )
                )
                if symbol_state.get("history_status") == "awaiting_history":
                    should_enqueue = False
                if should_enqueue:
                    self._pending_symbols.add(symbol)
                    update_symbol_state(
                        symbol,
                        history_status="pending",
                        history_ready=False,
                    )

    async def _run(self, client: AsyncClient) -> None:
        while not self._stop_requested:
            symbol = None
            onboard_time = None

            async with self._lock:
                if self._pending_symbols:
                    symbol = sorted(self._pending_symbols)[0]
                    self._pending_symbols.discard(symbol)
                    onboard_time = self._target_symbols.get(symbol)

            if not symbol or onboard_time is None:
                await asyncio.sleep(1.0)
                continue

            await self._start_delay()
            await self._sync_symbol_guarded(client, symbol, onboard_time)
            self._processed_symbols.add(symbol)
            await asyncio.sleep(SYNC_REQUEST_DELAY_SECONDS)

    async def _start_delay(self) -> None:
        now = time.monotonic()
        wait_before_start = max(
            0.0,
            (self._last_symbol_started_at + SYNC_SYMBOL_START_DELAY_SECONDS) - now,
        )
        if wait_before_start > 0:
            await asyncio.sleep(wait_before_start)
        self._last_symbol_started_at = time.monotonic()

    async def _sync_symbol_guarded(self, client: AsyncClient, symbol: str, onboard_time: int) -> None:
        try:
            await self._sync_symbol(client, symbol, onboard_time)
        except Exception as exc:
            logger.error(f"history-sync: {symbol} failed: {type(exc).__name__}: {exc}")
            update_symbol_state(
                symbol,
                history_status="error",
                history_ready=False,
                history_error=f"{type(exc).__name__}: {exc}",
            )

    async def _sync_symbol(self, client: AsyncClient, symbol: str, onboard_time: int) -> None:
        latest_closed_open_time = _latest_closed_open_time_ms()
        target_start = _window_start_open_time(latest_closed_open_time, onboard_time)
        last_open_time = await asyncio.to_thread(_get_last_open_time, symbol)
        history_span_candles = _history_span_candles(latest_closed_open_time, onboard_time)
        current_state = get_symbol_state(symbol)
        was_ready = bool(current_state.get("history_ready"))

        update_symbol_state(
            symbol,
            history_status="syncing",
            history_ready=was_ready,
            history_error=None,
            history_last_open_time=last_open_time,
        )

        if last_open_time is not None and last_open_time >= latest_closed_open_time:
            logger.debug(f"history-sync: {symbol} ready, database already up to date")
            update_symbol_state(
                symbol,
                history_status="ready",
                history_ready=True,
                history_last_open_time=last_open_time,
                history_lag_candles=0,
            )
            return

        if last_open_time is None and history_span_candles > TARGET_WINDOW_CANDLES:
            logger.info(
                f"history-sync: {symbol} awaiting full history, empty local history and span="
                f"{history_span_candles} candles"
            )
            update_symbol_state(
                symbol,
                history_status="awaiting_history",
                history_ready=False,
                history_lag_candles=history_span_candles,
                history_window_start=target_start,
            )
            return

        if last_open_time is not None:
            lag_candles = max(0, (latest_closed_open_time - last_open_time) // KLINE_MS)
            if lag_candles > TARGET_WINDOW_CANDLES:
                logger.info(
                    f"history-sync: {symbol} awaiting full history, lag={lag_candles} candles"
                )
                update_symbol_state(
                    symbol,
                    history_status="awaiting_history",
                    history_ready=False,
                    history_last_open_time=last_open_time,
                    history_lag_candles=lag_candles,
                    history_window_start=target_start,
                )
                return

        fetch_start = target_start
        if last_open_time is not None and last_open_time >= target_start:
            fetch_start = last_open_time + KLINE_MS

        logger.debug(
            f"history-sync: {symbol} syncing closed 1m candles from {fetch_start} "
            f"(target_window_start={target_start})"
        )
        klines = await client.futures_klines(
            symbol=symbol,
            interval=KLINE_INTERVAL,
            startTime=fetch_start,
            limit=TARGET_WINDOW_CANDLES,
        )

        closed_klines = [
            item for item in klines
            if int(item[6]) <= (latest_closed_open_time + KLINE_MS - 1)
        ]

        if closed_klines:
            await asyncio.to_thread(_save_closed_klines, symbol, closed_klines)
            last_open_time = int(closed_klines[-1][0])
        else:
            last_open_time = await asyncio.to_thread(_get_last_open_time, symbol)

        lag_candles = 0
        if last_open_time is not None:
            lag_candles = max(0, (latest_closed_open_time - last_open_time) // KLINE_MS)

        update_symbol_state(
            symbol,
            history_status="ready" if lag_candles == 0 else "partial",
            history_ready=lag_candles == 0,
            history_last_open_time=last_open_time,
            history_lag_candles=lag_candles,
            history_window_start=target_start,
        )
        logger.debug(
            f"history-sync: {symbol} sync finished, lag_candles={lag_candles}, "
            f"status={'ready' if lag_candles == 0 else 'partial'}"
        )


history_sync_service = HistorySyncService()
