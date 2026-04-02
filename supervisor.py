# supervisor.py
import asyncio

import aiohttp
from binance import AsyncClient

from collector import collector
from logger_setup import setup_logger
from worker_utils import SymbolActivationPendingError
from workers import StreamTimeoutEscalationError, worker

logger = setup_logger()
MAX_RETRIES = 10
CLIENT_CREATE_MAX_RETRIES = 5
CLIENT_CREATE_RETRY_DELAY = 5
PENDING_SYMBOL_RETRY_BASE_DELAY = 60
PENDING_SYMBOL_RETRY_MAX_DELAY = 900


class TaskInfo:
    def __init__(self, pair, worker_func, queue, client):
        self.pair = pair
        self.worker_func = worker_func
        self.queue = queue
        self.client = client
        self.task = None
        self.retries = 0
        self.blocked = False
        self.pending_retries = 0
        self.next_retry_at = 0.0
        self.runtime_state = {}

    async def start(self, reason="initial"):
        loop = asyncio.get_running_loop()
        if self.blocked or (self.task and not self.task.done()):
            return
        if loop.time() < self.next_retry_at:
            return

        logger.warning(f"[{self.pair}] starting (reason={reason}, retries={self.retries})")
        self.task = asyncio.create_task(self._runner())

    async def _runner(self):
        try:
            await self.worker_func(self.pair, self.queue, self.client, self.runtime_state)
            logger.debug(f"[{self.pair}] finished worker normally")
            self.retries = 0
            self.pending_retries = 0
            self.next_retry_at = 0.0
        except SymbolActivationPendingError:
            self.pending_retries += 1
            delay = min(
                PENDING_SYMBOL_RETRY_BASE_DELAY * (2 ** (self.pending_retries - 1)),
                PENDING_SYMBOL_RETRY_MAX_DELAY,
            )
            self.next_retry_at = asyncio.get_running_loop().time() + delay
            logger.info(
                f"[{self.pair}] symbol is listed but not active in streams yet; "
                f"next check in {delay} sec"
            )
        except StreamTimeoutEscalationError as e:
            self.blocked = True
            logger.error(f"[{self.pair}] blocked due to repeated stream timeouts: {e}")
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
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ping", timeout=10) as response:
                return response.status == 200
    except Exception as e:
        logger.warning(f"Network check failed: {e}")
        return False


async def create_binance_client():
    last_error = None

    for attempt in range(1, CLIENT_CREATE_MAX_RETRIES + 1):
        try:
            logger.info(
                f"Creating Binance client (attempt {attempt}/{CLIENT_CREATE_MAX_RETRIES})"
            )
            client = await AsyncClient.create()
            logger.info("Binance client created successfully")
            return client
        except Exception as e:
            last_error = e
            logger.error(f"Failed to create Binance client on attempt {attempt}: {e}")
            if attempt < CLIENT_CREATE_MAX_RETRIES:
                logger.info(
                    f"Retrying Binance client creation in {CLIENT_CREATE_RETRY_DELAY} seconds"
                )
                await asyncio.sleep(CLIENT_CREATE_RETRY_DELAY)

    raise RuntimeError(
        f"Could not create Binance client after {CLIENT_CREATE_MAX_RETRIES} attempts"
    ) from last_error


async def get_pairs(client):
    res = await client.futures_exchange_info()

    excluded_pairs = [
        "BTCUSDT_241227", "ETHUSDT_240927", "ETHUSDT_241227",
        "BTCUSDT_240927", "USDCUSDT", "BTCSTUSDT", "CKBUSDT",
        "ETHUSDT_250328", "BTCUSDT_250328", "REEFUSDT",
    ]
    usdt_pairs = sorted(
        [s["symbol"] for s in res["symbols"] if "USDT" in s["symbol"] and s["symbol"] not in excluded_pairs]
    )
    return usdt_pairs


async def supervisor():
    tasks: dict[str, TaskInfo] = {}
    queue = asyncio.Queue()
    client = None
    collector_task = None

    try:
        client = await create_binance_client()
        collector_task = asyncio.create_task(collector(queue))

        while True:
            if not await check_network():
                logger.debug("Network DOWN, stopping all tasks...")
                await asyncio.gather(*(t.stop() for t in tasks.values()), return_exceptions=True)
                await asyncio.sleep(10)
                continue

            new_pairs = set(await get_pairs(client))
            new_pairs = list(new_pairs)[:]
            active = sum(
                1 for t in tasks.values() if not t.blocked and t.task and not t.task.done()
            )
            inactive_pairs = sorted(
                pair for pair, task_info in tasks.items()
                if not task_info.blocked and (not task_info.task or task_info.task.done())
            )
            blocked_pairs = sorted(
                pair for pair, task_info in tasks.items()
                if task_info.blocked
            )
            logger.info(
                f"Total pairs={len(new_pairs)}, active={active}, "
                f"inactive={len(inactive_pairs)}, inactive_pairs={inactive_pairs}, "
                f"blocked={len(blocked_pairs)}, blocked_pairs={blocked_pairs}"
            )

            for pair in set(new_pairs) - set(tasks.keys()):
                tasks[pair] = TaskInfo(pair, worker, queue, client)
                await tasks[pair].start(reason="new")
                logger.debug(f"Added pair {pair}")
                await asyncio.sleep(0.6)

            for pair in set(tasks.keys()) - set(new_pairs):
                await tasks[pair].stop()
                del tasks[pair]
                logger.debug(f"Removed pair {pair}")

            for task_info in tasks.values():
                if not task_info.blocked and (not task_info.task or task_info.task.done()):
                    await task_info.start(reason="monitor")
                    await asyncio.sleep(0.7)

            await asyncio.sleep(10)
    finally:
        await asyncio.gather(*(t.stop() for t in tasks.values()), return_exceptions=True)

        if collector_task and not collector_task.done():
            collector_task.cancel()
            await asyncio.gather(collector_task, return_exceptions=True)

        if client is not None:
            logger.info("Closing Binance client connection")
            await client.close_connection()


async def main():
    await supervisor()
