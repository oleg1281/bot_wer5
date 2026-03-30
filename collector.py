import asyncio
from logger_setup import setup_logger

logger = setup_logger()

async def collector(queue: asyncio.Queue):
    """Собирает свечи от всех воркеров"""
    candles = []  # или можно сразу писать в БД
    while True:
        candle = await queue.get()
        candles.append(candle)
        queue.task_done()

        # 🔹 Проверяем размер очереди каждые 50 сообщений
        if queue.qsize() % 50 == 0:
            pass
            #logger.info(f"📦 Размер очереди: {queue.qsize()} элементов")
        #logger.info(f"Collector получил свечу: {candle['pair']} {candle['close']}")
