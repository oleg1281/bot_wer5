import asyncio
import random

from binance import BinanceSocketManager

from logger_setup import setup_logger
from worker_utils import SymbolActivationPendingError, candles_get

logger = setup_logger()

DEFAULT_STREAM_TIMEOUT = 90
ELEVATED_STREAM_TIMEOUT = 180
RECONNECT_BASE_DELAY = 5
RECONNECT_MAX_DELAY = 60


class StreamTimeoutEscalationError(Exception):
    pass


async def worker(symbol: str, queue: asyncio.Queue, client, runtime_state=None):
    max_retries = 5
    if runtime_state is None:
        runtime_state = {}

    runtime_state.setdefault("stream_timeout", DEFAULT_STREAM_TIMEOUT)
    runtime_state.setdefault("stream_timeout_failures", 0)
    runtime_state.setdefault("reconnect_delay", RECONNECT_BASE_DELAY)

    for attempt in range(max_retries):
        try:
            candle_m_1500 = await candles_get(symbol, "1m", 1445, client)
            if candle_m_1500.size == 0:
                raise RuntimeError(f"{symbol}: empty warmup candles")

            logger.info(f"{symbol}: loaded {len(candle_m_1500)} warmup candles")
            break
        except SymbolActivationPendingError:
            raise
        except Exception as e:
            logger.warning(f"{symbol}: warmup attempt {attempt + 1}/{max_retries} failed: {e}")
            await asyncio.sleep(5)
    else:
        raise RuntimeError(f"{symbol}: could not load initial candles")

    bm = BinanceSocketManager(client)
    while True:
        try:
            async with bm.kline_futures_socket(symbol, interval="1m") as stream:
                timeout_seconds = runtime_state["stream_timeout"]
                logger.info(f"{symbol}: stream started (timeout={timeout_seconds}s)")

                while True:
                    try:
                        res = await asyncio.wait_for(stream.recv(), timeout=float(timeout_seconds))
                        close_price = float(res["k"]["c"])
                        runtime_state["stream_timeout_failures"] = 0
                        runtime_state["stream_timeout"] = DEFAULT_STREAM_TIMEOUT
                        runtime_state["reconnect_delay"] = RECONNECT_BASE_DELAY
                        await queue.put({"pair": symbol, "close": close_price})
                    except asyncio.TimeoutError:
                        runtime_state["stream_timeout_failures"] += 1
                        failures = runtime_state["stream_timeout_failures"]

                        if failures == 2:
                            runtime_state["stream_timeout"] = ELEVATED_STREAM_TIMEOUT

                        timeout_seconds = runtime_state["stream_timeout"]
                        logger.warning(
                            f"{symbol}: no data for {timeout_seconds}s, keeping socket open "
                            f"(idle_timeouts={failures}, next_timeout={timeout_seconds}s)"
                        )
                        continue

        except Exception as e:
            delay = runtime_state["reconnect_delay"]
            sleep_for = min(delay + random.uniform(0, delay * 0.2), RECONNECT_MAX_DELAY)
            logger.error(
                f"{symbol}: socket error {e}, retrying in {sleep_for:.1f}s"
            )
            await asyncio.sleep(sleep_for)
            runtime_state["reconnect_delay"] = min(delay * 2, RECONNECT_MAX_DELAY)
