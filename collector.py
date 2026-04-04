import asyncio
from collections import defaultdict

from logger_setup import setup_logger

logger = setup_logger()


async def collector(queue: asyncio.Queue, pair_activity: dict[str, dict] | None = None):
    latest_klines_by_pair: dict[str, dict] = {}
    latest_mark_price_by_pair: dict[str, dict] = {}
    latest_book_ticker_by_pair: dict[str, dict] = {}
    counters = defaultdict(int)
    latest_user_events: dict[str, dict] = {}

    if pair_activity is None:
        pair_activity = {}

    while True:
        loop = asyncio.get_running_loop()
        event = await queue.get()
        event_type = event.get("event_type", "unknown")
        pair = event.get("pair") or "_global"

        counters[event_type] += 1

        if event_type == "kline":
            latest_klines_by_pair[pair] = event
            pair_state = pair_activity.setdefault(pair, {})
            pair_state["stream_confirmed"] = True
            pair_state["last_event_time"] = event.get("event_time")
            pair_state["last_interval"] = event.get("interval")
            pair_state["last_closed"] = event.get("closed")
            pair_state["last_seen_at"] = loop.time()
            pair_state["stale_windows"] = 0
            pair_state["last_kline_seen_at"] = loop.time()
            pair_state["kline_stale_windows"] = 0
            pair_state["weak_recovery_events"] = pair_state.get("weak_recovery_events", 0) + 1
        elif event_type == "mark_price":
            latest_mark_price_by_pair[pair] = event
            pair_state = pair_activity.setdefault(pair, {})
            pair_state["last_seen_at"] = loop.time()
            pair_state["stale_windows"] = 0
            pair_state["weak_recovery_events"] = pair_state.get("weak_recovery_events", 0) + 1
        elif event_type == "book_ticker":
            latest_book_ticker_by_pair[pair] = event
            pair_state = pair_activity.setdefault(pair, {})
            pair_state["last_seen_at"] = loop.time()
            pair_state["stale_windows"] = 0
            pair_state["weak_recovery_events"] = pair_state.get("weak_recovery_events", 0) + 1
        elif event_type.startswith("user_"):
            latest_user_events[event_type] = event

        queue.task_done()

        total_events = sum(counters.values())
        if total_events % 1000 == 0:
            logger.info(
                "collector: "
                f"total_events={total_events}, "
                f"kline={counters['kline']}, "
                f"mark_price={counters['mark_price']}, "
                f"book_ticker={counters['book_ticker']}, "
                f"user_events={counters['user_order_update'] + counters['user_account_update'] + counters['user_event']}, "
                f"queue_size={queue.qsize()}"
            )
