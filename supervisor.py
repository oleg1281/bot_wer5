# supervisor.py
import asyncio

import aiohttp
from binance import AsyncClient

from collector import collector
from config import api_key, api_secret
from history_sync import history_sync_service
from logger_setup import setup_logger
from runtime_state import get_symbol_state, update_overview, update_symbol_lists, update_symbol_state
from stream_closed_candle_writer import stream_closed_candle_writer
from workers import (
    MARKET_BATCH_SIZE,
    all_book_ticker_worker,
    multiplex_market_worker,
    user_stream_worker,
)

logger = setup_logger()

MAX_RETRIES = 10
CLIENT_CREATE_MAX_RETRIES = 5
CLIENT_CREATE_RETRY_DELAY = 5
FULL_PAIR_REFRESH_INTERVAL = 3600
STATUS_REFRESH_INTERVAL = 30
SUPERVISOR_LOOP_INTERVAL_SECONDS = 2
PAIR_STALE_WINDOW_SECONDS = 60
PAIR_STALE_WINDOWS_TO_WEAK = 5
WEAK_PAIR_STALE_WINDOWS_TO_BLOCK = 5
KLINE_STALE_WINDOW_SECONDS = 60
WEAK_RECOVERY_EVENT_THRESHOLD = 3
STREAM_STAGING_FREEZE_SECONDS = 20

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


class TaskInfo:
    def __init__(self, name, worker_func, worker_args, allow_blocking=True):
        self.name = name
        self.worker_func = worker_func
        self.worker_args = worker_args
        self.allow_blocking = allow_blocking
        self.task = None
        self.retries = 0
        self.blocked = False
        self.runtime_state = {}

    async def start(self, reason="initial"):
        if self.blocked or (self.task and not self.task.done()):
            return

        logger.warning(f"[{self.name}] starting (reason={reason}, retries={self.retries})")
        self.task = asyncio.create_task(self._runner())

    async def _runner(self):
        try:
            await self.worker_func(*self.worker_args, runtime_state=self.runtime_state)
            logger.debug(f"[{self.name}] worker finished normally")
            self.retries = 0
        except Exception as e:
            logger.error(f"[{self.name}] crashed: {e}")
            self.retries += 1
            if self.allow_blocking and self.retries > MAX_RETRIES:
                self.blocked = True
                logger.critical(f"[{self.name}] blocked after {MAX_RETRIES} retries")
            else:
                await asyncio.sleep(1)
        finally:
            self.task = None

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.task = None


class MarketBatchCoordinator:
    def __init__(self, batch_size: int, freeze_seconds: int) -> None:
        self.batch_size = batch_size
        self.freeze_seconds = freeze_seconds
        self._stable_batches: list[dict] = []
        self._staging_batch: dict | None = None
        self._next_batch_index = 1

    def _make_batch(self, now: float) -> dict:
        name = f"market-batch-{self._next_batch_index:02d}"
        self._next_batch_index += 1
        return {
            "name": name,
            "symbols": [],
            "created_at": now,
        }

    def _promote_staging_if_needed(self, now: float) -> None:
        if not self._staging_batch:
            return

        symbols = self._staging_batch["symbols"]
        created_at = float(self._staging_batch["created_at"])
        if not symbols:
            self._staging_batch = None
            return

        if len(symbols) >= self.batch_size or (now - created_at) >= self.freeze_seconds:
            self._stable_batches.append(self._staging_batch)
            self._staging_batch = None

    def update(self, pairs: list[str], now: float) -> None:
        desired_set = set(pairs)

        if self._staging_batch is not None:
            self._staging_batch["symbols"] = [
                symbol for symbol in self._staging_batch["symbols"] if symbol in desired_set
            ]
            if not self._staging_batch["symbols"]:
                self._staging_batch = None

        self._promote_staging_if_needed(now)

        assigned_symbols = {
            symbol
            for batch in self._stable_batches
            for symbol in batch["symbols"]
        }
        if self._staging_batch is not None:
            assigned_symbols.update(self._staging_batch["symbols"])

        new_symbols = [symbol for symbol in pairs if symbol not in assigned_symbols]
        for symbol in new_symbols:
            if self._staging_batch is None:
                self._staging_batch = self._make_batch(now)

            self._staging_batch["symbols"].append(symbol)
            if len(self._staging_batch["symbols"]) >= self.batch_size:
                self._promote_staging_if_needed(now)

        self._promote_staging_if_needed(now)

    def build_task_specs(self, queue: asyncio.Queue, client) -> dict[str, tuple]:
        specs: dict[str, tuple] = {}
        for batch in self._stable_batches:
            specs[batch["name"]] = (multiplex_market_worker, [list(batch["symbols"]), queue, client, batch["name"]])
        return specs

    def get_batch_name_by_symbol(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for batch in self._stable_batches:
            for symbol in batch["symbols"]:
                mapping[symbol] = batch["name"]
        return mapping


async def check_network():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://fapi.binance.com/fapi/v1/ping", timeout=10) as response:
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
            client = await AsyncClient.create(
                api_key=api_key.strip() if api_key else None,
                api_secret=api_secret.strip() if api_secret else None,
            )
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


async def fetch_exchange_symbol_metadata(client) -> dict[str, dict]:
    res = await client.futures_exchange_info()
    symbol_metadata: dict[str, dict] = {}

    for symbol_info in res["symbols"]:
        symbol = symbol_info["symbol"]
        if symbol in EXCLUDED_PAIRS:
            continue
        if symbol_info.get("quoteAsset") not in ALLOWED_QUOTE_ASSETS:
            continue
        if symbol_info.get("contractType") != "PERPETUAL":
            continue

        onboard_time = int(symbol_info.get("onboardDate") or 0)
        symbol_metadata[symbol] = {
            "status": symbol_info.get("status", "UNKNOWN"),
            "onboard_time": onboard_time - (onboard_time % 60_000)
            if onboard_time > 0
            else 0,
        }

    return symbol_metadata


def split_symbols_by_status(symbol_metadata: dict[str, dict]) -> tuple[list[str], list[str]]:
    tradable_pairs = sorted(
        symbol for symbol, meta in symbol_metadata.items() if meta.get("status") == "TRADING"
    )
    pending_pairs = sorted(
        symbol for symbol, meta in symbol_metadata.items() if meta.get("status") != "TRADING"
    )
    return tradable_pairs, pending_pairs


def extract_batch_symbols(task_info: TaskInfo | None) -> list[str]:
    if not task_info or not task_info.worker_args:
        return []
    batch_symbols = task_info.worker_args[0]
    return list(batch_symbols) if isinstance(batch_symbols, list) else []


def build_weak_task_specs(pairs: list[str], queue: asyncio.Queue, client):
    specs = {}
    for symbol in sorted(pairs):
        name = f"weak-pair-{symbol}"
        specs[name] = (multiplex_market_worker, [[symbol], queue, client, name])
    return specs


async def reconcile_tasks(tasks: dict[str, TaskInfo], desired_specs: dict[str, tuple]):
    current_names = set(tasks.keys())
    desired_names = set(desired_specs.keys())

    for name in sorted(current_names - desired_names):
        await tasks[name].stop()
        del tasks[name]
        logger.info(f"[{name}] removed")

    for name in sorted(desired_names - current_names):
        worker_func, worker_args = desired_specs[name]
        tasks[name] = TaskInfo(name, worker_func, worker_args, allow_blocking=False)
        await tasks[name].start(reason="new")
        await asyncio.sleep(0.3)

    for name in sorted(current_names & desired_names):
        worker_func, worker_args = desired_specs[name]
        existing_task = tasks[name]
        if existing_task.worker_func is worker_func and existing_task.worker_args == worker_args:
            continue

        await existing_task.stop()
        tasks[name] = TaskInfo(name, worker_func, worker_args, allow_blocking=False)
        await tasks[name].start(reason="rebalance")
        await asyncio.sleep(0.3)


def initialize_pair_watch(pair_activity: dict[str, dict], pairs: list[str], now: float):
    for pair in pairs:
        pair_state = pair_activity.setdefault(pair, {})
        pair_state.setdefault("last_seen_at", now)
        pair_state.setdefault("stale_windows", 0)


def reset_pair_watch(pair_activity: dict[str, dict], pairs: list[str], now: float):
    for pair in pairs:
        pair_state = pair_activity.setdefault(pair, {})
        pair_state["last_seen_at"] = now
        pair_state["stale_windows"] = 0
        pair_state["last_kline_seen_at"] = now
        pair_state["kline_stale_windows"] = 0
        pair_state["weak_recovery_events"] = 0


def evaluate_stale_pairs(
    pairs: list[str],
    pair_activity: dict[str, dict],
    weak_pairs: set[str],
    blocked_pairs: set[str],
    now: float,
) -> tuple[list[str], list[str], list[tuple[str, int, str]]]:
    newly_weak: list[str] = []
    newly_blocked: list[str] = []
    stale_progress: list[tuple[str, int, str]] = []

    for pair in pairs:
        if pair in blocked_pairs:
            continue

        pair_state = pair_activity.setdefault(pair, {})
        last_seen_at = pair_state.setdefault("last_seen_at", now)
        previous_windows = int(pair_state.get("stale_windows", 0))
        observed_windows = int(max(0.0, now - last_seen_at) // PAIR_STALE_WINDOW_SECONDS)

        if observed_windows <= previous_windows:
            continue

        pair_state["stale_windows"] = observed_windows
        pair_mode = "weak" if pair in weak_pairs else "normal"
        stale_progress.append((pair, observed_windows, pair_mode))

        if pair in weak_pairs:
            if observed_windows >= WEAK_PAIR_STALE_WINDOWS_TO_BLOCK:
                weak_pairs.discard(pair)
                blocked_pairs.add(pair)
                pair_state["stale_windows"] = 0
                pair_state["weak_recovery_events"] = 0
                pair_state["last_seen_at"] = now
                newly_blocked.append(pair)
        elif observed_windows >= PAIR_STALE_WINDOWS_TO_WEAK:
            weak_pairs.add(pair)
            pair_state["stale_windows"] = 0
            pair_state["weak_recovery_events"] = 0
            pair_state["last_seen_at"] = now
            newly_weak.append(pair)

    return newly_weak, newly_blocked, stale_progress


def evaluate_kline_stale_pairs(
    pairs: list[str],
    pair_activity: dict[str, dict],
    now: float,
) -> list[tuple[str, int, str]]:
    kline_progress: list[tuple[str, int, str]] = []

    for pair in pairs:
        pair_state = pair_activity.setdefault(pair, {})
        last_kline_seen_at = pair_state.setdefault("last_kline_seen_at", now)
        previous_windows = int(pair_state.get("kline_stale_windows", 0))
        observed_windows = int(max(0.0, now - last_kline_seen_at) // KLINE_STALE_WINDOW_SECONDS)

        if observed_windows <= previous_windows:
            continue

        pair_state["kline_stale_windows"] = observed_windows
        pair_mode = "weak" if pair_state.get("is_weak_pair") else "normal"
        kline_progress.append((pair, observed_windows, pair_mode))

    return kline_progress


def evaluate_weak_recoveries(
    weak_pairs: set[str],
    pair_activity: dict[str, dict],
) -> list[str]:
    recovered_pairs: list[str] = []

    for pair in sorted(weak_pairs):
        pair_state = pair_activity.setdefault(pair, {})
        if int(pair_state.get("weak_recovery_events", 0)) >= WEAK_RECOVERY_EVENT_THRESHOLD:
            recovered_pairs.append(pair)

    return recovered_pairs


async def supervisor():
    market_tasks: dict[str, TaskInfo] = {}
    book_task: TaskInfo | None = None
    weak_tasks: dict[str, TaskInfo] = {}
    user_tasks: dict[str, TaskInfo] = {}
    queue = asyncio.Queue()
    client = None
    collector_task = None
    pair_activity: dict[str, dict] = {}

    known_symbol_metadata: dict[str, dict] = {}
    current_status_trading_pairs: list[str] = []
    current_pending_status_pairs: list[str] = []
    previous_pending_status_pairs: set[str] = set()
    previous_weak_pairs: set[str] = set()
    previous_blocked_pairs: set[str] = set()
    weak_pairs: set[str] = set()
    blocked_pairs: set[str] = set()
    market_batch_coordinator = MarketBatchCoordinator(
        MARKET_BATCH_SIZE,
        STREAM_STAGING_FREEZE_SECONDS,
    )

    next_full_pairs_refresh_at = 0.0
    next_status_refresh_at = 0.0
    network_was_down = False

    try:
        client = await create_binance_client()
        history_sync_service.start(client)
        stream_closed_candle_writer.start()
        collector_task = asyncio.create_task(collector(queue, pair_activity))
        user_tasks["user-stream"] = TaskInfo(
            "user-stream",
            user_stream_worker,
            [queue, client, "user-stream"],
            allow_blocking=False,
        )
        await user_tasks["user-stream"].start(reason="new")

        known_symbol_metadata = await fetch_exchange_symbol_metadata(client)
        await history_sync_service.update_targets(
            {
                symbol: meta.get("onboard_time", 0)
                for symbol, meta in known_symbol_metadata.items()
                if meta.get("status") == "TRADING"
            }
        )

        while True:
            loop = asyncio.get_running_loop()
            now = loop.time()

            if not await check_network():
                logger.warning("Network down, stopping all market tasks...")
                update_overview(bot_status="degraded", exchange_status="disconnected")
                network_was_down = True
                await asyncio.gather(
                    *(t.stop() for t in market_tasks.values()),
                    *( [book_task.stop()] if book_task else [] ),
                    *(t.stop() for t in weak_tasks.values()),
                    *(t.stop() for t in user_tasks.values()),
                    return_exceptions=True,
                )
                await asyncio.sleep(10)
                continue

            if network_was_down:
                network_was_down = False
                reset_pair_watch(pair_activity, current_status_trading_pairs, now)
                logger.info("Network recovered, pair stale counters reset")
                update_overview(bot_status="running", exchange_status="connected")

            should_refresh_status = now >= next_status_refresh_at or not known_symbol_metadata
            should_log_full_refresh = now >= next_full_pairs_refresh_at or not known_symbol_metadata

            if should_refresh_status:
                known_symbol_metadata = await fetch_exchange_symbol_metadata(client)
                next_status_refresh_at = now + STATUS_REFRESH_INTERVAL

                if should_log_full_refresh:
                    next_full_pairs_refresh_at = now + FULL_PAIR_REFRESH_INTERVAL
                    logger.info(
                        f"Full symbol universe refreshed: total_known_symbols={len(known_symbol_metadata)}"
                    )

            status_trading_pairs, pending_status_pairs = split_symbols_by_status(known_symbol_metadata)
            status_trading_set = set(status_trading_pairs)
            pending_status_set = set(pending_status_pairs)

            await history_sync_service.update_targets(
                {
                    symbol: meta.get("onboard_time", 0)
                    for symbol, meta in known_symbol_metadata.items()
                    if symbol in status_trading_set
                }
            )

            for pair, pair_state in pair_activity.items():
                pair_state["is_weak_pair"] = pair in weak_pairs

            removed_from_official_trading = sorted(
                (weak_pairs | blocked_pairs) - status_trading_set
            )
            if removed_from_official_trading:
                for pair in removed_from_official_trading:
                    weak_pairs.discard(pair)
                    blocked_pairs.discard(pair)
                logger.info(
                    f"Pairs removed from official TRADING: count={len(removed_from_official_trading)}, "
                    f"pairs={removed_from_official_trading[:20]}"
                )

            current_status_trading_pairs = status_trading_pairs
            current_pending_status_pairs = pending_status_pairs

            ready_pairs = sorted(
                pair
                for pair in current_status_trading_pairs
                if get_symbol_state(pair).get("history_ready")
            )
            initialize_pair_watch(pair_activity, ready_pairs, now)
            newly_weak_pairs, newly_blocked_pairs, stale_progress = evaluate_stale_pairs(
                ready_pairs,
                pair_activity,
                weak_pairs,
                blocked_pairs,
                now,
            )
            kline_stale_progress = evaluate_kline_stale_pairs(
                ready_pairs,
                pair_activity,
                now,
            )
            recovered_weak_pairs = evaluate_weak_recoveries(weak_pairs, pair_activity)
            if recovered_weak_pairs:
                for pair in recovered_weak_pairs:
                    weak_pairs.discard(pair)
                    pair_state = pair_activity.setdefault(pair, {})
                    pair_state["weak_recovery_events"] = 0
                    pair_state["stale_windows"] = 0
                    pair_state["kline_stale_windows"] = 0
                    pair_state["last_seen_at"] = now
                    pair_state["last_kline_seen_at"] = now

            normal_pairs = sorted(
                pair for pair in ready_pairs
                if pair not in weak_pairs and pair not in blocked_pairs
            )
            weak_pair_list = sorted(
                pair for pair in ready_pairs if pair in weak_pairs
            )

            market_batch_coordinator.update(normal_pairs, now)
            desired_market_specs = market_batch_coordinator.build_task_specs(queue, client)
            desired_weak_specs = build_weak_task_specs(weak_pair_list, queue, client)
            batch_name_by_symbol = market_batch_coordinator.get_batch_name_by_symbol()

            await reconcile_tasks(market_tasks, desired_market_specs)
            await reconcile_tasks(weak_tasks, desired_weak_specs)

            if book_task is None:
                book_task = TaskInfo(
                    "book-stream",
                    all_book_ticker_worker,
                    [[], queue, client, "book-stream"],
                    allow_blocking=False,
                )
                await book_task.start(reason="new")

            newly_pending_status_pairs = sorted(
                pending_status_set - previous_pending_status_pairs
            )
            newly_logged_weak_pairs = sorted(set(weak_pair_list) - previous_weak_pairs)
            newly_logged_blocked_pairs = sorted(blocked_pairs - previous_blocked_pairs)

            if newly_pending_status_pairs:
                logger.info(
                    f"Pairs are pending official TRADING status: count={len(newly_pending_status_pairs)}, "
                    f"pairs={newly_pending_status_pairs[:20]}"
                )

            if recovered_weak_pairs:
                logger.info(
                    f"Pairs recovered from weak isolated batches back to normal: "
                    f"count={len(recovered_weak_pairs)}, pairs={recovered_weak_pairs[:20]}"
                )

            if stale_progress:
                stale_pairs = [
                    f"{pair}({window_number}/{PAIR_STALE_WINDOWS_TO_WEAK},{pair_mode})"
                    for pair, window_number, pair_mode in stale_progress
                ]
                logger.warning(
                    f"Pairs with no data progress: count={len(stale_progress)}, "
                    f"pairs={stale_pairs[:20]}"
                )

            if kline_stale_progress:
                kline_pairs = [
                    f"{pair}({window_number * KLINE_STALE_WINDOW_SECONDS}s,{pair_mode})"
                    for pair, window_number, pair_mode in kline_stale_progress
                ]
                logger.warning(
                    f"Pairs with stale kline stream: count={len(kline_stale_progress)}, "
                    f"pairs={kline_pairs[:20]}"
                )

            weak_log_pairs = sorted(set(newly_weak_pairs) | set(newly_logged_weak_pairs))
            if weak_log_pairs:
                logger.warning(
                    f"Pairs moved to weak isolated batches after 5 stale windows: "
                    f"count={len(weak_log_pairs)}, pairs={weak_log_pairs[:20]}"
                )

            blocked_log_pairs = sorted(set(newly_blocked_pairs) | set(newly_logged_blocked_pairs))
            if blocked_log_pairs:
                logger.error(
                    f"Pairs blocked after failing in weak isolated batches: "
                    f"count={len(blocked_log_pairs)}, pairs={blocked_log_pairs[:20]}"
                )

            previous_pending_status_pairs = pending_status_set
            previous_weak_pairs = set(weak_pair_list)
            previous_blocked_pairs = set(blocked_pairs)

            active_market = sum(
                1 for t in market_tasks.values() if not t.blocked and t.task and not t.task.done()
            )
            active_book = int(
                bool(book_task and not book_task.blocked and book_task.task and not book_task.task.done())
            )
            active_weak = sum(
                1 for t in weak_tasks.values() if not t.blocked and t.task and not t.task.done()
            )
            blocked_market = sorted(name for name, task in market_tasks.items() if task.blocked)
            blocked_book = [book_task.name] if book_task and book_task.blocked else []
            blocked_weak = sorted(name for name, task in weak_tasks.items() if task.blocked)
            logger.debug(
                f"official_trading={len(current_status_trading_pairs)}, pending_status={len(current_pending_status_pairs)}, "
                f"ready_pairs={len(ready_pairs)}, normal_pairs={len(normal_pairs)}, "
                f"weak_pairs={len(weak_pair_list)}, blocked_pairs={len(blocked_pairs)}, "
                f"batch_size={MARKET_BATCH_SIZE}, market_batches={len(market_tasks)}, "
                f"active_market_batches={active_market}, book_stream_active={active_book}, "
                f"weak_batches={len(weak_tasks)}, "
                f"active_weak_batches={active_weak}, blocked_market_batches={blocked_market}, "
                f"blocked_book_streams={blocked_book}, blocked_weak_batches={blocked_weak}, "
                f"blocked_pair_symbols={sorted(blocked_pairs)}"
            )

            update_overview(
                bot_status="running",
                exchange_status="connected",
                tracked_pairs=len(current_status_trading_pairs),
                weak_pairs=len(weak_pair_list),
                blocked_pairs=len(blocked_pairs),
                market_batches=len(market_tasks),
                active_market_batches=active_market,
                book_stream_active=active_book,
                weak_batches=len(weak_tasks),
                active_weak_batches=active_weak,
                pending_status=len(current_pending_status_pairs),
            )
            update_symbol_lists(
                tracked_symbols=current_status_trading_pairs,
                pending_symbols=current_pending_status_pairs,
            )

            for symbol in current_status_trading_pairs:
                pair_state = pair_activity.get(symbol, {})
                update_symbol_state(
                    symbol,
                    lifecycle="tracked",
                    stream_confirmed=bool(pair_state.get("stream_confirmed", False)),
                    stream_batch_id=batch_name_by_symbol.get(symbol),
                    is_weak=symbol in weak_pairs,
                    is_blocked=symbol in blocked_pairs,
                )

            for symbol in current_pending_status_pairs:
                update_symbol_state(
                    symbol,
                    lifecycle="pending",
                    stream_confirmed=False,
                    stream_batch_id=None,
                    is_weak=False,
                    is_blocked=False,
                )

            for task_info in list(market_tasks.values()):
                if not task_info.blocked and (not task_info.task or task_info.task.done()):
                    await task_info.start(reason="monitor")
                    await asyncio.sleep(0.2)

            if book_task and not book_task.blocked and (not book_task.task or book_task.task.done()):
                await book_task.start(reason="monitor")
                await asyncio.sleep(0.2)

            for task_info in list(weak_tasks.values()):
                if not task_info.blocked and (not task_info.task or task_info.task.done()):
                    await task_info.start(reason="monitor")
                    await asyncio.sleep(0.2)

            for task_info in list(user_tasks.values()):
                if not task_info.blocked and (not task_info.task or task_info.task.done()):
                    await task_info.start(reason="monitor")
                    await asyncio.sleep(0.2)

            await asyncio.sleep(SUPERVISOR_LOOP_INTERVAL_SECONDS)
    finally:
        await asyncio.gather(
            *(t.stop() for t in list(market_tasks.values())),
            *( [book_task.stop()] if book_task else [] ),
            *(t.stop() for t in list(weak_tasks.values())),
            *(t.stop() for t in list(user_tasks.values())),
            return_exceptions=True,
        )

        if collector_task and not collector_task.done():
            collector_task.cancel()
            await asyncio.gather(collector_task, return_exceptions=True)

        await history_sync_service.stop()
        await stream_closed_candle_writer.stop()

        if client is not None:
            logger.info("Closing Binance client connection")
            await client.close_connection()
        update_overview(bot_status="stopped", exchange_status="disconnected")


async def main():
    await supervisor()
