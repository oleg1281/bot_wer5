# supervisor.py
import asyncio
import aiohttp
from binance import AsyncClient
from logger_setup import setup_logger
from workers import worker
from collector import collector

logger = setup_logger()
MAX_RETRIES = 10

class TaskInfo:
    def __init__(self, pair, worker_func, queue, client):
        self.pair = pair
        self.worker_func = worker_func
        self.queue = queue  # сохраняем очередь
        self.client = client  # ✅ сохраняем общий client
        self.task = None
        self.retries = 0
        self.blocked = False

    async def start(self, reason="initial"):
        if self.blocked or (self.task and not self.task.done()):
            return
        logger.warning(f"[{self.pair}] starting (reason={reason}, retries={self.retries})")
        self.task = asyncio.create_task(self._runner())

    async def _runner(self):
        try:
            await self.worker_func(self.pair, self.queue, self.client)
            logger.debug(f"[{self.pair}] finished worker normally")
            self.retries = 0
        except Exception as e:
            logger.error(f"[{self.pair}] crashed: {e}")
            self.retries += 1
            if self.retries > MAX_RETRIES:
                self.blocked = True
                logger.critical(f"[{self.pair}] blocked after {MAX_RETRIES} retries")
            else:
                await asyncio.sleep(1)
                await self.start(reason="restart")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

async def check_network():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.binance.com/api/v3/ping", timeout=10) as r:
                return r.status == 200
    except:
        return False

async def get_pairs(client):
    res = await client.futures_exchange_info()

    excluded_pairs = ['BTCUSDT_241227','ETHUSDT_240927','ETHUSDT_241227',
                      'BTCUSDT_240927','USDCUSDT','BTCSTUSDT','CKBUSDT',
                      'ETHUSDT_250328','BTCUSDT_250328','REEFUSDT']
    usdt_pairs = sorted([s['symbol'] for s in res['symbols']
                         if 'USDT' in s['symbol'] and s['symbol'] not in excluded_pairs])
    return usdt_pairs

async def supervisor():
    tasks: dict[str, TaskInfo] = {}                                         # Создаем пустой словарь
    queue = asyncio.Queue()                                                 # Создание асинхронной очереди
    client = await AsyncClient.create()
    logger.info("Binance client создан ✅")

    # запускаем сборщик данных отдельно
    collector_task = asyncio.create_task(collector(queue))                  # Запускает функцию collector(queue) как отдельную асинхронную задачу, которая начинает работать параллельно с остальными корутинами.

    while True:
        if not await check_network():                                       # Проверяет доступность API Binance
            logger.debug("Network DOWN, stopping all tasks...")             # И если не доступно выводит ошибку
            await asyncio.gather(*(t.stop() for t in tasks.values()))       # Останавливаем все задачи
            await asyncio.sleep(10)                                         # Ждём 10 секунд
            continue

        new_pairs = set(await get_pairs(client))
        new_pairs = list(new_pairs)[:350]  # ✅ берём только первые 50 пар
        active = sum(1 for t in tasks.values() if not t.blocked and t.task and not t.task.done())
        logger.info(f"Всего пар={len(new_pairs)}, в работе={active}")

        # добавление новых
        for p in set(new_pairs) - set(tasks.keys()):
            tasks[p] = TaskInfo(p, worker, queue, client)
            await tasks[p].start(reason="new")
            logger.debug(f"Добавлена пара {p}")
            await asyncio.sleep(0.7)

        # удаление исчезнувших
        for p in set(tasks.keys()) - set(new_pairs):
            await tasks[p].stop()
            del tasks[p]
            logger.debug(f"Удалена пара {p}")

        # рестарт упавших
        for t in tasks.values():
            if not t.blocked and (not t.task or t.task.done()):
                await t.start(reason="monitor")
                await asyncio.sleep(0.7)

        await asyncio.sleep(10)

    # 👇 закрываем клиент, когда всё завершено
    await client.close_connection()

async def main():
    await supervisor()

if __name__ == "__main__":
    asyncio.run(main())
