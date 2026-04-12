import asyncio
from collections import defaultdict

from logger_setup import setup_logger
from runtime_state import (
    append_live_kline,
    append_user_event,
    get_symbol_state,
    record_queue_backlog,
    record_symbol_stream_event,
    update_account_state,
    update_overview,
    update_symbol_state,
)
from stream_closed_candle_writer import stream_closed_candle_writer

logger = setup_logger()


async def collector(queue: asyncio.Queue, pair_activity: dict[str, dict] | None = None):
    latest_klines_by_pair: dict[str, dict] = {}
    latest_mark_price_by_pair: dict[str, dict] = {}
    latest_book_ticker_by_pair: dict[str, dict] = {}
    counters = defaultdict(int)
    latest_user_events: dict[str, dict] = {}
    recent_order_updates: list[dict] = []

    if pair_activity is None:
        pair_activity = {}

    while True:
        loop = asyncio.get_running_loop()
        event = await queue.get()
        queue_backlog = queue.qsize() + 1
        event_type = event.get("event_type", "unknown")
        pair = event.get("pair") or "_global"

        counters[event_type] += 1

        if event_type == "kline":
            record_symbol_stream_event(pair, "kline")
            latest_klines_by_pair[pair] = event
            live_points = append_live_kline(pair, event)
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
            update_symbol_state(
                pair,
                stream_confirmed=True,
                last_stream_event_time=event.get("event_time"),
                last_kline_closed=bool(event.get("closed")),
                last_interval=event.get("interval"),
                live_window_points=live_points,
                live_last_price=float(event.get("close") or 0.0),
                live_last_open_time=event.get("open_time"),
                live_last_close_time=event.get("close_time"),
            )
            if event.get("closed"):
                symbol_state = get_symbol_state(pair)
                last_open_time = symbol_state.get("history_last_open_time")
                was_ready = bool(symbol_state.get("history_ready"))
                current_open_time = int(event.get("open_time") or 0)
                if last_open_time is not None:
                    expected_open_time = int(last_open_time) + 60_000
                    if current_open_time > expected_open_time:
                        missing_candles = max(1, (current_open_time - expected_open_time) // 60_000)
                        update_symbol_state(
                            pair,
                            history_status="desynced",
                            history_ready=was_ready,
                            history_lag_candles=missing_candles,
                            history_gap_from_open_time=int(last_open_time),
                            history_gap_to_open_time=current_open_time,
                            db_sync_status="gap_detected",
                            db_sync_last_checked_at=int(event.get("event_time") or 0),
                        )
                        stream_closed_candle_writer.enqueue(pair, event)
                    else:
                        stream_closed_candle_writer.enqueue(pair, event)
                else:
                    stream_closed_candle_writer.enqueue(pair, event)
        elif event_type == "mark_price":
            record_symbol_stream_event(pair, "mark_price")
            latest_mark_price_by_pair[pair] = event
            pair_state = pair_activity.setdefault(pair, {})
            pair_state["last_seen_at"] = loop.time()
            pair_state["stale_windows"] = 0
            pair_state["weak_recovery_events"] = pair_state.get("weak_recovery_events", 0) + 1
        elif event_type == "book_ticker":
            record_symbol_stream_event(pair, "book_ticker")
            latest_book_ticker_by_pair[pair] = event
            pair_state = pair_activity.setdefault(pair, {})
            pair_state["last_seen_at"] = loop.time()
            pair_state["stale_windows"] = 0
            pair_state["weak_recovery_events"] = pair_state.get("weak_recovery_events", 0) + 1
        elif event_type.startswith("user_"):
            latest_user_events[event_type] = event
            append_user_event(event)
            if event_type == "user_account_update":
                update_account_state(
                    positions=event.get("positions", []),
                    balances=event.get("balances", []),
                )
            elif event_type == "user_order_update":
                recent_order_updates.insert(0, event)
                del recent_order_updates[25:]
                update_account_state(orders=recent_order_updates)

        total_events = sum(counters.values())
        user_stream_events = counters["user_order_update"] + counters["user_account_update"] + counters["user_event"]
        record_queue_backlog(queue_backlog)
        update_overview(
            user_events=user_stream_events,
            stream_events_total=total_events,
            stream_kline_events=counters["kline"],
            stream_mark_price_events=counters["mark_price"],
            stream_book_ticker_events=counters["book_ticker"],
            stream_user_events=user_stream_events,
        )

        queue.task_done()
        if total_events % 1000 == 0:
            logger.debug(
                "collector: "
                f"total_events={total_events}, "
                f"kline={counters['kline']}, "
                f"mark_price={counters['mark_price']}, "
                f"book_ticker={counters['book_ticker']}, "
                f"user_events={counters['user_order_update'] + counters['user_account_update'] + counters['user_event']}, "
                f"queue_size={queue_backlog}"
            )
