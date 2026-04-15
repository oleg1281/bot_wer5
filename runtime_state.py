from __future__ import annotations

from copy import deepcopy
from collections import deque
from datetime import datetime, timezone
from time import monotonic


NETWORK_AVERAGE_WINDOW_SECONDS = 60.0
NETWORK_SAMPLES = deque()
NETWORK_BYTES_WINDOW_TOTAL = 0
QUEUE_PEAK_WINDOW_SECONDS = 10.0
QUEUE_SAMPLES = deque()
LIVE_KLINE_WINDOW_MS = 3 * 60_000
LIVE_KLINE_WINDOWS: dict[str, deque] = {}
SYMBOL_EVENT_WINDOW_SECONDS = 60.0
SYMBOL_EVENT_WINDOWS: dict[str, dict[str, deque]] = {}


RUNTIME_STATE = {
    "overview": {
        "bot_status": "starting",
        "exchange_status": "connecting",
        "tracked_pairs": 0,
        "weak_pairs": 0,
        "blocked_pairs": 0,
        "queue_size": 0,
        "queue_peak": 0,
        "book_ws_queue_limit": 0,
        "book_ws_overflow_count": 0,
        "book_ws_restart_count": 0,
        "book_ws_sampled_out": 0,
        "network_rx_kbps": 0.0,
        "open_positions": 0,
        "open_orders": 0,
        "account_equity": 0.0,
        "available_balance": 0.0,
        "unrealized_pnl": 0.0,
        "realized_pnl_today": 0.0,
        "user_events": 0,
        "stream_events_total": 0,
        "stream_kline_events": 0,
        "stream_mark_price_events": 0,
        "stream_book_ticker_events": 0,
        "stream_user_events": 0,
        "market_batches": 0,
        "active_market_batches": 0,
        "book_stream_active": 0,
        "weak_batches": 0,
        "active_weak_batches": 0,
        "pending_status": 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    },
    "positions": [],
    "orders": [],
    "balances": [],
    "recent_user_events": [],
    "tracked_symbols": [],
    "pending_symbols": [],
    "symbol_states": {},
    "stream_batches": {
        "market": [],
        "book": [],
        "weak": [],
    },
    "candle_loader": {
        "status": "idle",
        "running": False,
        "target_limit": 1400,
        "start_delay_seconds": 0.7,
        "total_symbols": 0,
        "completed_symbols": 0,
        "current_symbol": None,
        "last_completed_symbol": None,
        "last_error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    },
}


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_overview(**values) -> None:
    RUNTIME_STATE["overview"].update(values)
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def append_user_event(event: dict, limit: int = 50) -> None:
    events = RUNTIME_STATE["recent_user_events"]
    events.insert(0, deepcopy(event))
    del events[limit:]
    user_events = int(RUNTIME_STATE["overview"].get("user_events", 0)) + 1
    update_overview(user_events=user_events)


def update_account_state(*, positions=None, orders=None, balances=None) -> None:
    if positions is not None:
        RUNTIME_STATE["positions"] = deepcopy(positions)
        RUNTIME_STATE["overview"]["open_positions"] = len(positions)
    if orders is not None:
        RUNTIME_STATE["orders"] = deepcopy(orders)
        RUNTIME_STATE["overview"]["open_orders"] = len(orders)
    if balances is not None:
        RUNTIME_STATE["balances"] = deepcopy(balances)
        usdt_balance = next(
            (balance for balance in balances if balance.get("a") == "USDT"),
            None,
        )
        if usdt_balance:
            wallet = float(usdt_balance.get("wb", 0.0))
            available = float(usdt_balance.get("cw", wallet))
            RUNTIME_STATE["overview"]["account_equity"] = wallet
            RUNTIME_STATE["overview"]["available_balance"] = available
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def update_symbol_lists(*, tracked_symbols=None, pending_symbols=None) -> None:
    if tracked_symbols is not None:
        RUNTIME_STATE["tracked_symbols"] = list(tracked_symbols)
    if pending_symbols is not None:
        RUNTIME_STATE["pending_symbols"] = list(pending_symbols)
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def update_stream_batches(*, market_batches=None, book_batches=None, weak_batches=None) -> None:
    stream_batches = RUNTIME_STATE.setdefault("stream_batches", {"market": [], "book": [], "weak": []})
    if market_batches is not None:
        stream_batches["market"] = deepcopy(list(market_batches))
    if book_batches is not None:
        stream_batches["book"] = deepcopy(list(book_batches))
    if weak_batches is not None:
        stream_batches["weak"] = deepcopy(list(weak_batches))
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def update_symbol_state(symbol: str, **values) -> None:
    if not symbol:
        return

    symbol_states = RUNTIME_STATE.setdefault("symbol_states", {})
    state = symbol_states.setdefault(symbol, {})
    state.update(deepcopy(values))
    state["updated_at"] = _stamp()
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def apply_disabled_symbols(symbols: list[str]) -> None:
    normalized_symbols = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    if not normalized_symbols:
        return

    disabled_set = set(normalized_symbols)

    tracked_symbols = [symbol for symbol in RUNTIME_STATE.get("tracked_symbols", []) if symbol not in disabled_set]
    pending_symbols = [symbol for symbol in RUNTIME_STATE.get("pending_symbols", []) if symbol not in disabled_set]
    RUNTIME_STATE["tracked_symbols"] = tracked_symbols
    RUNTIME_STATE["pending_symbols"] = pending_symbols

    stream_batches = RUNTIME_STATE.setdefault("stream_batches", {"market": [], "book": [], "weak": []})
    for batch_type in ("market", "book", "weak"):
        cleaned_batches = []
        for batch in stream_batches.get(batch_type, []):
            symbols_in_batch = [
                symbol for symbol in batch.get("symbols", [])
                if symbol not in disabled_set
            ]
            if not symbols_in_batch:
                continue

            cleaned_batch = deepcopy(batch)
            cleaned_batch["symbols"] = symbols_in_batch
            cleaned_batch["symbol_count"] = len(symbols_in_batch)
            cleaned_batches.append(cleaned_batch)
        stream_batches[batch_type] = cleaned_batches

    for symbol in normalized_symbols:
        update_symbol_state(
            symbol,
            lifecycle="disabled",
            stream_confirmed=False,
            stream_batch_id=None,
            is_weak=False,
            is_blocked=False,
            project_status="DISABLED",
        )

    overview = RUNTIME_STATE.setdefault("overview", {})
    overview["tracked_pairs"] = len(tracked_symbols)
    overview["updated_at"] = _stamp()


def get_symbol_state(symbol: str) -> dict:
    return deepcopy(RUNTIME_STATE.get("symbol_states", {}).get(symbol, {}))


def record_symbol_stream_event(symbol: str, event_type: str) -> None:
    if not symbol or not event_type:
        return

    now = monotonic()
    symbol_windows = SYMBOL_EVENT_WINDOWS.setdefault(symbol, {})
    event_window = symbol_windows.setdefault(event_type, deque())
    event_window.append(now)

    cutoff = now - SYMBOL_EVENT_WINDOW_SECONDS
    while event_window and event_window[0] < cutoff:
        event_window.popleft()

    kline_count = len(symbol_windows.get("kline", ()))
    mark_price_count = len(symbol_windows.get("mark_price", ()))
    book_ticker_count = len(symbol_windows.get("book_ticker", ()))

    update_symbol_state(
        symbol,
        stream_kline_events_60s=kline_count,
        stream_mark_price_events_60s=mark_price_count,
        stream_book_ticker_events_60s=book_ticker_count,
        stream_event_window_seconds=int(SYMBOL_EVENT_WINDOW_SECONDS),
    )


def append_live_kline(symbol: str, event: dict) -> int:
    if not symbol:
        return 0

    event_time = int(event.get("event_time") or event.get("close_time") or 0)
    if event_time <= 0:
        event_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    window = LIVE_KLINE_WINDOWS.setdefault(symbol, deque())
    window.append(
        {
            "event_time": event_time,
            "open_time": int(event.get("open_time") or 0),
            "close_time": int(event.get("close_time") or 0),
            "open": float(event.get("open") or 0.0),
            "high": float(event.get("high") or 0.0),
            "low": float(event.get("low") or 0.0),
            "close": float(event.get("close") or 0.0),
            "volume": float(event.get("volume") or 0.0),
            "quote_volume": float(event.get("quote_volume") or 0.0),
            "trades": int(event.get("trades") or 0),
            "closed": bool(event.get("closed")),
            "interval": event.get("interval"),
        }
    )

    cutoff = event_time - LIVE_KLINE_WINDOW_MS
    while window and int(window[0].get("event_time") or 0) < cutoff:
        window.popleft()

    return len(window)


def get_live_kline_window(symbol: str) -> list[dict]:
    return deepcopy(list(LIVE_KLINE_WINDOWS.get(symbol, ())))


def update_candle_loader(**values) -> None:
    RUNTIME_STATE["candle_loader"].update(values)
    RUNTIME_STATE["candle_loader"]["updated_at"] = _stamp()


def get_candle_loader_state() -> dict:
    return deepcopy(RUNTIME_STATE["candle_loader"])


def snapshot_runtime_state() -> dict:
    return deepcopy(RUNTIME_STATE)


def record_network_ingress(byte_count: int) -> None:
    global NETWORK_BYTES_WINDOW_TOTAL

    now = monotonic()
    safe_bytes = max(0, int(byte_count))
    NETWORK_SAMPLES.append((now, safe_bytes))
    NETWORK_BYTES_WINDOW_TOTAL += safe_bytes

    cutoff = now - NETWORK_AVERAGE_WINDOW_SECONDS
    while NETWORK_SAMPLES and NETWORK_SAMPLES[0][0] < cutoff:
        _, expired_bytes = NETWORK_SAMPLES.popleft()
        NETWORK_BYTES_WINDOW_TOTAL -= expired_bytes

    window_seconds = min(NETWORK_AVERAGE_WINDOW_SECONDS, max(1.0, now - NETWORK_SAMPLES[0][0] if NETWORK_SAMPLES else 1.0))
    avg_bytes_per_second = NETWORK_BYTES_WINDOW_TOTAL / window_seconds
    avg_kilobits_per_second = round((avg_bytes_per_second * 8) / 1024, 2)
    RUNTIME_STATE["overview"]["network_rx_kbps"] = avg_kilobits_per_second
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()


def record_queue_backlog(queue_backlog: int) -> None:
    now = monotonic()
    safe_backlog = max(0, int(queue_backlog))
    QUEUE_SAMPLES.append((now, safe_backlog))

    cutoff = now - QUEUE_PEAK_WINDOW_SECONDS
    while QUEUE_SAMPLES and QUEUE_SAMPLES[0][0] < cutoff:
        QUEUE_SAMPLES.popleft()

    queue_peak = max((sample for _, sample in QUEUE_SAMPLES), default=safe_backlog)
    RUNTIME_STATE["overview"]["queue_size"] = safe_backlog
    RUNTIME_STATE["overview"]["queue_peak"] = queue_peak
    RUNTIME_STATE["overview"]["updated_at"] = _stamp()
