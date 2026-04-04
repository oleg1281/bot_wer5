import asyncio
import random
from typing import Iterable

from binance import BinanceSocketManager

from logger_setup import setup_logger

logger = setup_logger()

MARKET_BATCH_SIZE = 25
MARKET_STREAM_TIMEOUT = 120
RECONNECT_BASE_DELAY = 5
RECONNECT_MAX_DELAY = 60
WS_MAX_QUEUE_SIZE = 2000
BOOK_TICKER_SAMPLE_EVERY = 1


def chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]


def build_market_streams(symbols: Iterable[str]) -> list[str]:
    streams: list[str] = []
    for symbol in symbols:
        streams.append(f"{symbol.lower()}@kline_1m")
        streams.append(f"{symbol.lower()}@markPrice")
    return streams


def normalize_market_event(message: dict) -> dict | None:
    stream_name = message.get("stream")
    payload = message.get("data")

    if not stream_name or not isinstance(payload, dict):
        return None

    if payload.get("e") == "kline" or "@kline_" in stream_name.lower():
        kline = payload["k"]
        return {
            "event_type": "kline",
            "stream": stream_name,
            "pair": payload.get("s"),
            "close": float(kline["c"]),
            "closed": bool(kline["x"]),
            "interval": kline["i"],
            "event_time": payload.get("E"),
        }

    if payload.get("e") == "markPriceUpdate" or "@markprice" in stream_name.lower():
        return {
            "event_type": "mark_price",
            "stream": stream_name,
            "pair": payload.get("s"),
            "mark_price": float(payload["p"]),
            "index_price": float(payload["i"]) if payload.get("i") is not None else None,
            "event_time": payload.get("E"),
            "funding_time": payload.get("T"),
            "funding_rate": float(payload["r"]) if payload.get("r") is not None else None,
        }

    if payload.get("e") == "bookTicker" or "@bookticker" in stream_name.lower():
        return {
            "event_type": "book_ticker",
            "stream": stream_name,
            "pair": payload.get("s"),
            "best_bid_price": float(payload["b"]) if payload.get("b") is not None else None,
            "best_bid_qty": float(payload["B"]) if payload.get("B") is not None else None,
            "best_ask_price": float(payload["a"]) if payload.get("a") is not None else None,
            "best_ask_qty": float(payload["A"]) if payload.get("A") is not None else None,
            "event_time": payload.get("E"),
            "update_id": payload.get("u"),
        }

    return None


def normalize_book_ticker_payload(payload: dict) -> dict | None:
    if not isinstance(payload, dict):
        return None

    symbol = payload.get("s")
    update_id = payload.get("u")
    if not symbol or update_id is None:
        return None

    return {
        "event_type": "book_ticker",
        "stream": "!bookTicker",
        "pair": symbol,
        "best_bid_price": float(payload["b"]) if payload.get("b") is not None else None,
        "best_bid_qty": float(payload["B"]) if payload.get("B") is not None else None,
        "best_ask_price": float(payload["a"]) if payload.get("a") is not None else None,
        "best_ask_qty": float(payload["A"]) if payload.get("A") is not None else None,
        "event_time": payload.get("E"),
        "update_id": update_id,
    }


def normalize_user_event(message: dict) -> dict | None:
    if not isinstance(message, dict):
        return None

    event_type = message.get("e")
    if not event_type:
        return None

    if event_type == "ORDER_TRADE_UPDATE":
        order = message.get("o", {})
        return {
            "event_type": "user_order_update",
            "symbol": order.get("s"),
            "side": order.get("S"),
            "order_type": order.get("o"),
            "order_status": order.get("X"),
            "execution_type": order.get("x"),
            "client_order_id": order.get("c"),
            "order_id": order.get("i"),
            "last_filled_qty": float(order["l"]) if order.get("l") is not None else None,
            "last_filled_price": float(order["L"]) if order.get("L") is not None else None,
            "avg_price": float(order["ap"]) if order.get("ap") is not None else None,
            "position_side": order.get("ps"),
            "event_time": message.get("E"),
        }

    if event_type == "ACCOUNT_UPDATE":
        account_update = message.get("a", {})
        return {
            "event_type": "user_account_update",
            "balances": account_update.get("B", []),
            "positions": account_update.get("P", []),
            "reason": account_update.get("m"),
            "event_time": message.get("E"),
        }

    return {
        "event_type": "user_event",
        "user_event_type": event_type,
        "event_time": message.get("E"),
        "payload": message,
    }


async def multiplex_market_worker(
    symbols: list[str],
    queue: asyncio.Queue,
    client,
    batch_id: str = "market-batch",
    runtime_state=None,
):
    if runtime_state is None:
        runtime_state = {}

    runtime_state.setdefault("reconnect_delay", RECONNECT_BASE_DELAY)
    runtime_state.setdefault("messages_received", 0)

    bm = BinanceSocketManager(
        client,
        user_timeout=MARKET_STREAM_TIMEOUT,
        max_queue_size=WS_MAX_QUEUE_SIZE,
    )
    market_streams = build_market_streams(symbols)

    logger.info(
        f"{batch_id}: starting multiplex for {len(symbols)} pairs, "
        f"market_streams={len(market_streams)}"
    )

    while True:
        try:
            async with bm.futures_multiplex_socket(
                market_streams, category="market"
            ) as market_stream:
                runtime_state["reconnect_delay"] = RECONNECT_BASE_DELAY
                logger.info(f"{batch_id}: multiplex stream started")

                while True:
                    message = await asyncio.wait_for(
                        market_stream.recv(), timeout=float(MARKET_STREAM_TIMEOUT)
                    )
                    event = normalize_market_event(message)
                    if event is None:
                        continue

                    runtime_state["messages_received"] += 1
                    await queue.put(event)
        except Exception as e:
            delay = runtime_state["reconnect_delay"]
            sleep_for = min(delay + random.uniform(0, delay * 0.2), RECONNECT_MAX_DELAY)
            logger.error(f"{batch_id}: multiplex socket error {e}, retrying in {sleep_for:.1f}s")
            await asyncio.sleep(sleep_for)
            runtime_state["reconnect_delay"] = min(delay * 2, RECONNECT_MAX_DELAY)


async def user_stream_worker(
    queue: asyncio.Queue,
    client,
    stream_id: str = "user-stream",
    runtime_state=None,
):
    if runtime_state is None:
        runtime_state = {}

    runtime_state.setdefault("reconnect_delay", RECONNECT_BASE_DELAY)
    runtime_state.setdefault("messages_received", 0)

    bm = BinanceSocketManager(
        client,
        user_timeout=MARKET_STREAM_TIMEOUT,
        max_queue_size=WS_MAX_QUEUE_SIZE,
    )

    logger.info(f"{stream_id}: starting futures user data stream")

    while True:
        try:
            async with bm.futures_user_socket() as user_stream:
                runtime_state["reconnect_delay"] = RECONNECT_BASE_DELAY
                logger.info(f"{stream_id}: futures user stream started")

                while True:
                    message = await user_stream.recv()
                    event = normalize_user_event(message)
                    if event is None:
                        continue

                    runtime_state["messages_received"] += 1
                    await queue.put(event)
        except Exception as e:
            delay = runtime_state["reconnect_delay"]
            sleep_for = min(delay + random.uniform(0, delay * 0.2), RECONNECT_MAX_DELAY)
            logger.error(
                f"{stream_id}: user stream error {type(e).__name__}: {e!r}, "
                f"retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)
            runtime_state["reconnect_delay"] = min(delay * 2, RECONNECT_MAX_DELAY)


async def all_book_ticker_worker(
    tracked_symbols: list[str],
    queue: asyncio.Queue,
    client,
    stream_id: str = "book-stream",
    runtime_state=None,
):
    if runtime_state is None:
        runtime_state = {}

    runtime_state.setdefault("reconnect_delay", RECONNECT_BASE_DELAY)
    runtime_state.setdefault("messages_received", 0)
    runtime_state.setdefault("sampled_out_messages", 0)
    runtime_state.setdefault("per_symbol_seen_counts", {})

    bm = BinanceSocketManager(
        client,
        user_timeout=MARKET_STREAM_TIMEOUT,
        max_queue_size=WS_MAX_QUEUE_SIZE,
    )
    tracked_symbol_set = set(tracked_symbols)

    logger.info(
        f"{stream_id}: starting global futures bookTicker stream for {len(tracked_symbol_set)} pairs, "
        f"sample_every={BOOK_TICKER_SAMPLE_EVERY}"
    )

    while True:
        try:
            async with bm.all_ticker_futures_socket() as book_stream:
                runtime_state["reconnect_delay"] = RECONNECT_BASE_DELAY
                logger.info(f"{stream_id}: global futures bookTicker stream started")

                while True:
                    message = await asyncio.wait_for(
                        book_stream.recv(), timeout=float(MARKET_STREAM_TIMEOUT)
                    )
                    if isinstance(message, dict) and "data" in message:
                        data_payload = message.get("data")
                        if isinstance(data_payload, list):
                            payloads = data_payload
                        elif isinstance(data_payload, dict):
                            payloads = [data_payload]
                        else:
                            payloads = []
                    else:
                        payloads = message if isinstance(message, list) else [message]

                    for payload in payloads:
                        event = normalize_book_ticker_payload(payload)
                        if event is None:
                            continue

                        pair = event["pair"]
                        if pair not in tracked_symbol_set:
                            continue

                        seen_counts = runtime_state["per_symbol_seen_counts"]
                        seen_counts[pair] = int(seen_counts.get(pair, 0)) + 1
                        if BOOK_TICKER_SAMPLE_EVERY > 1 and seen_counts[pair] % BOOK_TICKER_SAMPLE_EVERY != 0:
                            runtime_state["sampled_out_messages"] += 1
                            continue

                        runtime_state["messages_received"] += 1
                        await queue.put(event)
        except Exception as e:
            delay = runtime_state["reconnect_delay"]
            sleep_for = min(delay + random.uniform(0, delay * 0.2), RECONNECT_MAX_DELAY)
            logger.error(
                f"{stream_id}: bookTicker socket error {type(e).__name__}: {e!r}, "
                f"retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)
            runtime_state["reconnect_delay"] = min(delay * 2, RECONNECT_MAX_DELAY)
