import asyncio

from binance import BinanceSocketManager

from logger_setup import setup_logger
from worker_utils import SymbolActivationPendingError, candles_get

logger = setup_logger()
DEFAULT_STREAM_TIMEOUT = 60
SECOND_TIMEOUT_STREAM_TIMEOUT = 180
TIMEOUT_INCREMENT = 60
MAX_STREAM_TIMEOUT_FAILURES = 4


class StreamTimeoutEscalationError(Exception):
    pass


async def worker(symbol: str, queue: asyncio.Queue, client, runtime_state=None):
    max_retries = 5
    if runtime_state is None:
        runtime_state = {}

    runtime_state.setdefault("stream_timeout", DEFAULT_STREAM_TIMEOUT)
    runtime_state.setdefault("stream_timeout_failures", 0)

    for attempt in range(max_retries):
        try:
            candle_m_1500 = await candles_get(symbol, "1m", 1445, client)
            if candle_m_1500.size == 0:
                raise RuntimeError(f"{symbol}: пустые данные при загрузке стартовых свечей")

            logger.info(f"{symbol}: успешно загружено {len(candle_m_1500)} минутных свечей")
            break
        except SymbolActivationPendingError:
            raise
        except Exception as e:
            logger.warning(f"{symbol}: попытка {attempt + 1}/{max_retries} не удалась: {e}")
            await asyncio.sleep(5)
    else:
        raise RuntimeError(f"{symbol}: не удалось получить начальные данные")

    bm = BinanceSocketManager(client)
    while True:
        try:
            async with bm.kline_futures_socket(symbol, interval="1m") as stream:
                timeout_seconds = runtime_state["stream_timeout"]
                logger.info(f"{symbol}: поток запущен ✅ (timeout={timeout_seconds}s)")
                while True:
                    try:
                        res = await asyncio.wait_for(stream.recv(), timeout=float(timeout_seconds))
                        close_price = float(res["k"]["c"])
                        await queue.put({"pair": symbol, "close": close_price})
                    except asyncio.TimeoutError:
                        runtime_state["stream_timeout_failures"] += 1
                        failures = runtime_state["stream_timeout_failures"]

                        if failures == 2:
                            runtime_state["stream_timeout"] = SECOND_TIMEOUT_STREAM_TIMEOUT
                        elif failures == 3:
                            runtime_state["stream_timeout"] += TIMEOUT_INCREMENT
                        elif failures >= MAX_STREAM_TIMEOUT_FAILURES:
                            raise StreamTimeoutEscalationError(
                                f"{symbol}: blocked after {failures} stream timeouts"
                            )

                        logger.warning(
                            f"{symbol}: нет данных {timeout_seconds} сек, перезапуск сокета "
                            f"(timeouts={failures}, next_timeout={runtime_state['stream_timeout']}s)"
                        )
                        break

        except StreamTimeoutEscalationError:
            raise
        except Exception as e:
            logger.error(f"{symbol}: ошибка сокета {e}, пробую снова через 10 сек")
            await asyncio.sleep(10)
