import asyncio
from logger_setup import setup_logger
from worker_utils import candles_get
from binance import BinanceSocketManager
import numpy as np

logger = setup_logger()

async def worker(symbol: str, queue: asyncio.Queue, client):
    """Скачивает свечи и слушает поток обновлений"""
    service = {...}
    retry_count = 0
    max_retries = 5

    # === ЭТАП 1. Получаем исторические данные ===
    for attempt in range(max_retries):
        try:
            candle_m_1500 = await candles_get(symbol, '1m', 1445, client)
            if candle_m_1500.size == 0:
                logger.error(f"{symbol}: пустые данные — прекращаем работу")
                return
            logger.info(f"{symbol}: успешно загружено {len(candle_m_1500)} минутных свечей")
            break
        except Exception as e:
            logger.warning(f"{symbol}: попытка {attempt+1}/{max_retries} не удалась: {e}")
            await asyncio.sleep(5)
    else:
        logger.error(f"{symbol}: не удалось получить начальные данные, завершаем задачу")
        return

    # === ЭТАП 2. Основной вечный цикл ===
    bm = BinanceSocketManager(client)
    while True:
        try:
            # слушаем websocket 1m
            async with bm.kline_futures_socket(symbol, interval="1m") as stream:
                logger.info(f"{symbol}: поток запущен ✅")
                while True:
                    try:
                        res = await asyncio.wait_for(stream.recv(), timeout=60.0)

                        # Пример простейшей обработки:
                        close_price = float(res["k"]["c"])
                        await queue.put({"pair": symbol, "close": close_price})
                        #logger.debug(f"{symbol}: новая цена {close_price}")

                    except asyncio.TimeoutError:
                        logger.warning(f"{symbol}: нет данных 60 сек, перезапуск сокета...")
                        break  # перезапускаем socket

        except Exception as e:
            logger.error(f"{symbol}: ошибка сокета {e}, пробую снова через 10 сек")
            await asyncio.sleep(10)




#await queue.put(fake_candle)
