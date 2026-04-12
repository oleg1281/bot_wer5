from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from binance import AsyncClient

from config import api_key, api_secret
from logger_setup import setup_logger
from runtime_state import get_candle_loader_state, snapshot_runtime_state, update_candle_loader

logger = setup_logger()

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "runtime_market_cache.sqlite3"


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
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        connection.commit()


def _save_klines(symbol: str, klines: list[list], updated_at: str) -> None:
    _ensure_db()
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
            updated_at,
        )
        for item in klines
    ]

    with sqlite3.connect(DB_PATH) as connection:
        connection.executemany(
            """
            INSERT OR REPLACE INTO futures_klines_1m (
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
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()


class CandleLoaderService:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_requested = False

    def start(self) -> bool:
        if self._task and not self._task.done():
            return False

        runtime = snapshot_runtime_state()
        tracked_symbols = list(runtime.get("tracked_symbols", []))
        loader_state = get_candle_loader_state()
        if not tracked_symbols:
            update_candle_loader(
                status="idle",
                running=False,
                total_symbols=0,
                completed_symbols=0,
                current_symbol=None,
                last_error="No tracked symbols available yet.",
            )
            return False

        self._stop_requested = False
        self._task = asyncio.create_task(self._run(tracked_symbols, loader_state))
        return True

    def stop(self) -> bool:
        if not self._task or self._task.done():
            return False
        self._stop_requested = True
        update_candle_loader(status="stopping", running=True)
        return True

    def update_settings(self, start_delay_seconds: float) -> dict:
        safe_delay = max(0.0, round(start_delay_seconds, 2))
        update_candle_loader(
            start_delay_seconds=safe_delay,
            last_error=None,
        )
        return get_candle_loader_state()

    async def _run(self, tracked_symbols: list[str], loader_state: dict) -> None:
        target_limit = int(loader_state.get("target_limit", 1400))
        start_delay = float(loader_state.get("start_delay_seconds", 0.7))

        client = None
        completed = 0

        update_candle_loader(
            status="running",
            running=True,
            total_symbols=len(tracked_symbols),
            completed_symbols=0,
            current_symbol=None,
            last_completed_symbol=None,
            last_error=None,
        )

        try:
            client = await AsyncClient.create(
                api_key=api_key.strip() if api_key else None,
                api_secret=api_secret.strip() if api_secret else None,
            )

            for symbol in tracked_symbols:
                if self._stop_requested:
                    update_candle_loader(
                        status="stopped",
                        running=False,
                        current_symbol=None,
                        completed_symbols=completed,
                    )
                    return

                update_candle_loader(current_symbol=symbol, completed_symbols=completed)
                klines = await client.futures_klines(symbol=symbol, interval="1m", limit=target_limit)
                await asyncio.to_thread(_save_klines, symbol, klines, get_candle_loader_state()["updated_at"])

                completed += 1
                update_candle_loader(
                    completed_symbols=completed,
                    last_completed_symbol=symbol,
                    current_symbol=symbol,
                )

                if completed < len(tracked_symbols):
                    await asyncio.sleep(start_delay)

            update_candle_loader(
                status="completed",
                running=False,
                current_symbol=None,
                completed_symbols=completed,
            )
        except Exception as exc:
            logger.error(f"candle-loader: {type(exc).__name__}: {exc!r}")
            update_candle_loader(
                status="error",
                running=False,
                current_symbol=None,
                completed_symbols=completed,
                last_error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            self._stop_requested = False
            if client is not None:
                await client.close_connection()
            self._task = None


candle_loader_service = CandleLoaderService()
