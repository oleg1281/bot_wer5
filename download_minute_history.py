import argparse
import asyncio
import sqlite3
import time
from pathlib import Path

from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from config import api_key, api_secret
from logger_setup import setup_logger

logger = setup_logger("history_loader")

# ----------------------------
# Main tuning parameters
# Edit these values here when you want to change speed or storage location.
# ----------------------------

# Local SQLite database file.
DB_PATH = Path("data/futures_1m_history.sqlite3")

# Kline request parameters.
KLINE_INTERVAL = "1m"
KLINE_MS = 60_000
REST_LIMIT = 1500

# Delay between consecutive REST requests made by one worker.
REQUEST_DELAY_SECONDS = 0.05

# Number of symbols downloaded in parallel.
MAX_PARALLEL_SYMBOLS = 2

# Hard maximum request rate across all workers.
# This is the main parameter for limiting top speed.
# Examples:
#   1800 = faster
#   1500 = balanced
#   1200 = safer
MAX_REQUESTS_PER_MINUTE = 600

# Effective minimum gap between request starts across all workers.
GLOBAL_REQUEST_INTERVAL_SECONDS = 60.0 / MAX_REQUESTS_PER_MINUTE
GLOBAL_REQUEST_INTERVAL_MAX_SECONDS = 0.50
RATE_LIMIT_SLOWDOWN_MULTIPLIER = 1.8
RATE_LIMIT_RECOVERY_STEP_SECONDS = 0.005
RATE_LIMIT_RECOVERY_EVERY_SUCCESS = 25

# SQLite settings.
SQLITE_BUSY_TIMEOUT_MS = 30_000

# Retry policy for temporary REST/network errors.
REST_MAX_RETRIES = 6
REST_RETRY_BASE_SECONDS = 2.0
RATE_LIMIT_RETRY_SECONDS = 15.0

# Retry policy for temporary SQLite locks.
DB_MAX_RETRIES = 8
DB_RETRY_SECONDS = 1.5

EXCLUDED_PAIRS = {
    "BTCUSDT_241227",
    "ETHUSDT_240927",
    "ETHUSDT_241227",
    "BTCUSDT_240927",
    "USDCUSDT",
    "ETHUSDT_250328",
    "BTCUSDT_250328",
}
ALLOWED_QUOTE_ASSETS = {"USDT", "USDC"}

_REQUEST_PACE_LOCK = asyncio.Lock()
_NEXT_REQUEST_TS = 0.0
_CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS = GLOBAL_REQUEST_INTERVAL_SECONDS
_SUCCESS_SINCE_RATE_LIMIT = 0
_RATE_LIMIT_HIT_COUNT = 0
_PROGRESS_LOCK = asyncio.Lock()
_COMPLETED_SYMBOLS = 0
_TOTAL_SYMBOLS = 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download full 1m futures history into a local SQLite database."
    )
    parser.add_argument(
        "--db-path",
        default=str(DB_PATH),
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional explicit symbol list, for example BTCUSDT ETHUSDT.",
    )
    parser.add_argument(
        "--include-non-trading",
        action="store_true",
        help="Include symbols that are not in official TRADING status.",
    )
    return parser.parse_args()


def ensure_database(conn: sqlite3.Connection):
    conn.execute(
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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_futures_klines_1m_symbol_close_time
        ON futures_klines_1m(symbol, close_time)
        """
    )
    conn.commit()


def configure_database(conn: sqlite3.Connection):
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")


def get_last_open_time(conn: sqlite3.Connection, symbol: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(open_time) FROM futures_klines_1m WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def upsert_klines(conn: sqlite3.Connection, symbol: str, candles: list[list]) -> int:
    updated_at = int(time.time() * 1000)
    rows = [
        (
            symbol,
            int(candle[0]),
            float(candle[1]),
            float(candle[2]),
            float(candle[3]),
            float(candle[4]),
            float(candle[5]),
            int(candle[6]),
            float(candle[7]),
            int(candle[8]),
            float(candle[9]),
            float(candle[10]),
            1,
            "rest",
            updated_at,
        )
        for candle in candles
    ]

    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            conn.executemany(
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
            conn.commit()
            return len(rows)
        except sqlite3.OperationalError as exc:
            conn.rollback()
            message = str(exc).lower()
            if "locked" not in message or attempt >= DB_MAX_RETRIES:
                raise
            delay = DB_RETRY_SECONDS * attempt
            logger.warning(
                f"{symbol}: database locked during upsert, retry {attempt}/{DB_MAX_RETRIES} "
                f"in {delay:.1f}s"
            )
            time.sleep(delay)

    return 0


async def fetch_klines_with_retries(
    client: AsyncClient,
    symbol: str,
    start_time: int,
) -> list[list]:
    global _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS, _NEXT_REQUEST_TS, _SUCCESS_SINCE_RATE_LIMIT
    global _RATE_LIMIT_HIT_COUNT

    for attempt in range(1, REST_MAX_RETRIES + 1):
        try:
            async with _REQUEST_PACE_LOCK:
                now = time.monotonic()
                wait_seconds = max(0.0, _NEXT_REQUEST_TS - now)
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                _NEXT_REQUEST_TS = time.monotonic() + _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS

            candles = await client.futures_klines(
                symbol=symbol,
                interval=KLINE_INTERVAL,
                startTime=start_time,
                limit=REST_LIMIT,
            )
            async with _REQUEST_PACE_LOCK:
                _SUCCESS_SINCE_RATE_LIMIT += 1
                if (
                    _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS > GLOBAL_REQUEST_INTERVAL_SECONDS
                    and _SUCCESS_SINCE_RATE_LIMIT >= RATE_LIMIT_RECOVERY_EVERY_SUCCESS
                ):
                    _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS = max(
                        GLOBAL_REQUEST_INTERVAL_SECONDS,
                        _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS - RATE_LIMIT_RECOVERY_STEP_SECONDS,
                    )
                    _SUCCESS_SINCE_RATE_LIMIT = 0
                    logger.info(
                        "Request pace recovered to %.3fs between global request starts",
                        _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS,
                    )
            return candles
        except BinanceAPIException as exc:
            if exc.code == -1003 and attempt < REST_MAX_RETRIES:
                async with _REQUEST_PACE_LOCK:
                    _RATE_LIMIT_HIT_COUNT += 1
                    _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS = min(
                        GLOBAL_REQUEST_INTERVAL_MAX_SECONDS,
                        max(
                            GLOBAL_REQUEST_INTERVAL_SECONDS,
                            _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS * RATE_LIMIT_SLOWDOWN_MULTIPLIER,
                        ),
                    )
                    _SUCCESS_SINCE_RATE_LIMIT = 0
                delay = RATE_LIMIT_RETRY_SECONDS * attempt
                logger.warning(
                    f"{symbol}: Binance rate limit hit (-1003), retry {attempt}/{REST_MAX_RETRIES} "
                    f"in {delay:.1f}s; global request interval raised to "
                    f"{_CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS:.3f}s"
                )
                await asyncio.sleep(delay)
                continue
            raise
        except asyncio.TimeoutError:
            if attempt >= REST_MAX_RETRIES:
                raise
            delay = REST_RETRY_BASE_SECONDS * attempt
            logger.warning(
                f"{symbol}: futures_klines timeout, retry {attempt}/{REST_MAX_RETRIES} "
                f"in {delay:.1f}s"
            )
            await asyncio.sleep(delay)
        except Exception as exc:
            message = str(exc).lower()
            retryable = any(
                marker in message
                for marker in ("timeout", "tempor", "connection reset", "server disconnected")
            )
            if not retryable or attempt >= REST_MAX_RETRIES:
                raise
            delay = REST_RETRY_BASE_SECONDS * attempt
            logger.warning(
                f"{symbol}: transient REST error '{exc}', retry {attempt}/{REST_MAX_RETRIES} "
                f"in {delay:.1f}s"
            )
            await asyncio.sleep(delay)

    return []


async def create_client() -> AsyncClient:
    return await AsyncClient.create(
        api_key=api_key.strip() if api_key else None,
        api_secret=api_secret.strip() if api_secret else None,
    )


async def fetch_symbol_metadata(client: AsyncClient, include_non_trading: bool) -> list[dict]:
    exchange_info = await client.futures_exchange_info()
    symbols: list[dict] = []

    for symbol_info in exchange_info["symbols"]:
        symbol = symbol_info["symbol"]
        if symbol in EXCLUDED_PAIRS:
            continue
        if symbol_info.get("quoteAsset") not in ALLOWED_QUOTE_ASSETS:
            continue
        if symbol_info.get("contractType") != "PERPETUAL":
            continue
        if not include_non_trading and symbol_info.get("status") != "TRADING":
            continue

        onboard_time = int(symbol_info.get("onboardDate") or 0)
        if onboard_time <= 0:
            continue

        symbols.append(
            {
                "symbol": symbol,
                "status": symbol_info.get("status", "UNKNOWN"),
                "onboard_time": onboard_time - (onboard_time % KLINE_MS),
            }
        )

    return sorted(symbols, key=lambda item: item["symbol"])


async def download_symbol_history(
    client: AsyncClient,
    db_path: Path,
    symbol: str,
    onboard_time: int,
):
    conn = sqlite3.connect(db_path)
    configure_database(conn)

    try:
        last_open_time = get_last_open_time(conn, symbol)
        start_time = (last_open_time + KLINE_MS) if last_open_time is not None else onboard_time
        total_saved = 0
        batch_count = 0

        logger.info(
            f"{symbol}: downloading 1m history from {start_time} "
            f"({'resume' if last_open_time is not None else 'full'})"
        )

        while True:
            now_ms = int(time.time() * 1000)
            if start_time > now_ms:
                logger.info(f"{symbol}: already up to date")
                return

            candles = await fetch_klines_with_retries(client, symbol, start_time)
            if not candles:
                break

            closed_candles = [
                candle for candle in candles
                if int(candle[6]) <= now_ms
            ]
            if not closed_candles:
                break

            saved = upsert_klines(conn, symbol, closed_candles)
            total_saved += saved
            batch_count += 1
            start_time = int(closed_candles[-1][0]) + KLINE_MS

            if batch_count % 20 == 0:
                logger.info(
                    f"{symbol}: batches={batch_count}, candles_saved={total_saved}, "
                    f"last_open_time={closed_candles[-1][0]}"
                )

            if len(closed_candles) < len(candles):
                break

            if len(candles) < REST_LIMIT:
                break

            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        logger.info(f"{symbol}: finished, candles_saved={total_saved}")
    finally:
        conn.close()


async def download_symbol_history_guarded(
    semaphore: asyncio.Semaphore,
    client: AsyncClient,
    db_path: Path,
    symbol: str,
    onboard_time: int,
):
    async with semaphore:
        try:
            await download_symbol_history(
                client=client,
                db_path=db_path,
                symbol=symbol,
                onboard_time=onboard_time,
            )
        except Exception as exc:
            logger.exception(f"{symbol}: failed with unrecoverable error: {exc}")
        finally:
            async with _PROGRESS_LOCK:
                global _COMPLETED_SYMBOLS
                _COMPLETED_SYMBOLS += 1
                done = _COMPLETED_SYMBOLS
                total = _TOTAL_SYMBOLS
            logger.info(
                "Progress: done %s/%s symbols | current pace %.3fs | rate-limit events %s",
                done,
                total,
                _CURRENT_GLOBAL_REQUEST_INTERVAL_SECONDS,
                _RATE_LIMIT_HIT_COUNT,
            )


async def main():
    global _TOTAL_SYMBOLS, _COMPLETED_SYMBOLS, _RATE_LIMIT_HIT_COUNT

    args = parse_args()
    db_path = Path(args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        configure_database(conn)
        ensure_database(conn)
        client = await create_client()
        try:
            if args.symbols:
                all_symbols = await fetch_symbol_metadata(client, include_non_trading=True)
                metadata_by_symbol = {item["symbol"]: item for item in all_symbols}
                selected = []
                for symbol in args.symbols:
                    item = metadata_by_symbol.get(symbol.upper())
                    if item is None:
                        logger.warning(f"{symbol}: symbol not found in futures exchange info, skipped")
                        continue
                    selected.append(item)
                symbols = selected
            else:
                symbols = await fetch_symbol_metadata(
                    client,
                    include_non_trading=args.include_non_trading,
                )

            _TOTAL_SYMBOLS = len(symbols)
            _COMPLETED_SYMBOLS = 0
            _RATE_LIMIT_HIT_COUNT = 0

            logger.info(
                f"Minute history loader started: symbols={len(symbols)}, db_path={db_path}, "
                f"parallel={MAX_PARALLEL_SYMBOLS}, request_delay={REQUEST_DELAY_SECONDS}, "
                f"max_requests_per_minute={MAX_REQUESTS_PER_MINUTE}, "
                f"global_request_interval={GLOBAL_REQUEST_INTERVAL_SECONDS:.3f}"
            )

            semaphore = asyncio.Semaphore(MAX_PARALLEL_SYMBOLS)
            tasks = [
                asyncio.create_task(
                    download_symbol_history_guarded(
                        semaphore=semaphore,
                        client=client,
                        db_path=db_path,
                        symbol=item["symbol"],
                        onboard_time=item["onboard_time"],
                    )
                )
                for item in symbols
            ]
            await asyncio.gather(*tasks)
        finally:
            await client.close_connection()
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
