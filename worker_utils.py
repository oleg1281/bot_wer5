import asyncio
import numpy as np
import logging
from binance.exceptions import BinanceAPIException, BinanceRequestException

async def candles_get(symbol, interval, limit, client, retries=5, base_delay=2, max_delay=60):
    attempt = 0
    delay = base_delay

    while attempt < retries:
        try:
            candles = await client.futures_klines(symbol=symbol, interval=interval, limit=limit, timeout=30)
            candles_array = np.array([row[:5] for row in candles], dtype=float)
            return candles_array

        except (BinanceAPIException, BinanceRequestException, asyncio.TimeoutError) as e:
            logging.warning(f"{symbol}: попытка {attempt + 1}/{retries} не удалась: {e}")
            attempt += 1

            if attempt < retries:
                delay = min(delay * 2, max_delay)
                logging.info(f"{symbol}: повторная попытка через {delay} секунд...")
                await asyncio.sleep(delay)
            else:
                logging.error(f"{symbol}: превышено число попыток.")
                raise RuntimeError(f"{symbol}: не удалось получить свечи после {retries} попыток") from e

        except Exception as e:
            logging.error(f"{symbol}: непредвиденная ошибка: {e}")
            raise

