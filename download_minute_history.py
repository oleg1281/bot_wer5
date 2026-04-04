import argparse
import asyncio
import sqlite3
import time
from pathlib import Path

from binance import AsyncClient

from config import api_key, api_secret
from logger_setup import setup_logger

logger = setup_logger("history_loader")

DB_PATH = Path(__file__).resolve().parent / "data" / "futures_1m_history.sqlite3"
KLINE_INTERVAL = "1m"
KLINE_MS = 60_000
REST_LIMIT = 1500
REQUEST_DELAY_SECONDS = 0.12

EXCLUDED_PAIRS = {
    "BTCUSDT_241227",
    "ETHUSDT_240927",
    "ETHUSDT_241227",
    "BTCUSDT_240927",
    "USDCUSDT",
    "ETHUSDT_250328",
    "BTCUSDT_250328",
}


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
        if symbol_info.get("quoteAsset") != "USDT":
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
    conn: sqlite3.Connection,
    symbol: str,
    onboard_time: int,
):
    last_open_time = get_last_open_time(conn, symbol)
    start_time = (last_open_time + KLINE_MS) if last_open_time is not None else onboard_time
    now_ms = int(time.time() * 1000)
    total_saved = 0
    batch_count = 0

    if start_time > now_ms:
        logger.info(f"{symbol}: already up to date")
        return

    logger.info(
        f"{symbol}: downloading 1m history from {start_time} "
        f"({'resume' if last_open_time is not None else 'full'})"
    )

    while start_time <= now_ms:
        candles = await client.futures_klines(
            symbol=symbol,
            interval=KLINE_INTERVAL,
            startTime=start_time,
            limit=REST_LIMIT,
        )
        if not candles:
            break

        saved = upsert_klines(conn, symbol, candles)
        total_saved += saved
        batch_count += 1
        start_time = int(candles[-1][0]) + KLINE_MS

        if batch_count % 20 == 0:
            logger.info(
                f"{symbol}: batches={batch_count}, candles_saved={total_saved}, "
                f"last_open_time={candles[-1][0]}"
            )

        if len(candles) < REST_LIMIT:
            break

        await asyncio.sleep(REQUEST_DELAY_SECONDS)

    logger.info(f"{symbol}: finished, candles_saved={total_saved}")


async def main():
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
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

            logger.info(
                f"Minute history loader started: symbols={len(symbols)}, db_path={db_path}"
            )

            for item in symbols:
                await download_symbol_history(
                    client,
                    conn,
                    item["symbol"],
                    item["onboard_time"],
                )
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
        finally:
            await client.close_connection()
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
