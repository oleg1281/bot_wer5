from __future__ import annotations

import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from runtime_state import get_live_kline_window, snapshot_runtime_state

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
HISTORY_DB_PATH = DATA_DIR / "futures_1m_history.sqlite3"
TIMEFRAME_TO_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
}


@dataclass
class OverviewSnapshot:
    bot_status: str
    exchange_status: str
    tracked_pairs: int
    weak_pairs: int
    blocked_pairs: int
    queue_size: int
    queue_peak: int
    book_ws_queue_limit: int
    book_ws_real_queue: int
    book_ws_real_peak: int
    book_ws_overflow_count: int
    book_ws_restart_count: int
    book_ws_sampled_out: int
    network_rx_kbps: float
    open_positions: int
    open_orders: int
    account_equity: float
    available_balance: float
    unrealized_pnl: float
    realized_pnl_today: float
    user_events: int
    stream_events_total: int
    stream_kline_events: int
    stream_mark_price_events: int
    stream_book_ticker_events: int
    stream_user_events: int
    updated_at: str


def get_overview_snapshot() -> dict:
    runtime = snapshot_runtime_state()
    overview = runtime["overview"]
    snapshot = OverviewSnapshot(
        bot_status=overview.get("bot_status", "starting"),
        exchange_status=overview.get("exchange_status", "connecting"),
        tracked_pairs=int(overview.get("tracked_pairs", 0)),
        weak_pairs=int(overview.get("weak_pairs", 0)),
        blocked_pairs=int(overview.get("blocked_pairs", 0)),
        queue_size=int(overview.get("queue_size", 0)),
        queue_peak=int(overview.get("queue_peak", 0)),
        book_ws_queue_limit=int(overview.get("book_ws_queue_limit", 0)),
        book_ws_real_queue=int(overview.get("book_ws_real_queue", 0)),
        book_ws_real_peak=int(overview.get("book_ws_real_peak", 0)),
        book_ws_overflow_count=int(overview.get("book_ws_overflow_count", 0)),
        book_ws_restart_count=int(overview.get("book_ws_restart_count", 0)),
        book_ws_sampled_out=int(overview.get("book_ws_sampled_out", 0)),
        network_rx_kbps=float(overview.get("network_rx_kbps", 0.0)),
        open_positions=int(overview.get("open_positions", len(runtime.get("positions", [])))),
        open_orders=int(overview.get("open_orders", len(runtime.get("orders", [])))),
        account_equity=float(overview.get("account_equity", 0.0)),
        available_balance=float(overview.get("available_balance", 0.0)),
        unrealized_pnl=float(overview.get("unrealized_pnl", 0.0)),
        realized_pnl_today=float(overview.get("realized_pnl_today", 0.0)),
        user_events=int(overview.get("user_events", 0)),
        stream_events_total=int(overview.get("stream_events_total", 0)),
        stream_kline_events=int(overview.get("stream_kline_events", 0)),
        stream_mark_price_events=int(overview.get("stream_mark_price_events", 0)),
        stream_book_ticker_events=int(overview.get("stream_book_ticker_events", 0)),
        stream_user_events=int(overview.get("stream_user_events", 0)),
        updated_at=overview.get("updated_at", datetime.now(timezone.utc).isoformat()),
    )
    return asdict(snapshot)


def get_condition_catalog() -> list[dict]:
    return [
        {
            "group": "Price",
            "items": [
                "Price Change %",
                "Close > Open",
                "Close > Previous High",
                "Gap %",
                "Breakout Above Level",
            ],
        },
        {
            "group": "Trend",
            "items": [
                "SMA",
                "EMA",
                "WMA",
                "MA Cross",
                "Price > SMA",
            ],
        },
        {
            "group": "Momentum",
            "items": [
                "RSI",
                "MACD",
                "Stochastic RSI",
                "Momentum %",
                "ROC",
            ],
        },
        {
            "group": "Volume",
            "items": [
                "Volume Spike",
                "Volume > SMA",
                "Taker Buy Imbalance",
                "Quote Volume Burst",
            ],
        },
        {
            "group": "Volatility",
            "items": [
                "ATR",
                "Bollinger Band Width",
                "Range %",
                "Candle Body %",
            ],
        },
        {
            "group": "Order Book",
            "items": [
                "Spread %",
                "Bid/Ask Imbalance",
                "Best Bid > Threshold",
                "Best Ask < Threshold",
            ],
        },
        {
            "group": "Futures",
            "items": [
                "Funding Rate",
                "Mark Price Deviation",
                "Open Interest",
                "Long/Short Ratio",
                "Liquidation Spike",
            ],
        },
        {
            "group": "Risk",
            "items": [
                "Cooldown Passed",
                "Max Daily Loss Not Hit",
                "Position Count < Limit",
                "Symbol Exposure < Limit",
            ],
        },
    ]


def get_strategy_builder_state() -> dict:
    runtime = snapshot_runtime_state()
    tracked_symbols = runtime.get("tracked_symbols", [])
    symbol_states = runtime.get("symbol_states", {})
    stream_batches = runtime.get("stream_batches", {})
    tracked_symbol_states = [
        {
            "symbol": symbol,
            **symbol_states.get(symbol, {}),
        }
        for symbol in tracked_symbols
    ]
    pending_symbol_states = [
        {
            "symbol": symbol,
            **symbol_states.get(symbol, {}),
        }
        for symbol in runtime.get("pending_symbols", [])
    ]
    return {
        "strategy": {
            "id": "breakout-rsi-confirmation",
            "name": "Breakout + RSI Confirmation",
            "mode": "long_entry",
            "status": "active",
            "selected_symbol": tracked_symbols[0] if tracked_symbols else "BTCUSDT",
            "selected_timeframe": "1m",
        },
        "strategies": [
            {"id": "breakout-rsi-confirmation", "name": "Breakout + RSI Confirmation", "status": "active"},
            {"id": "mean-revert-short", "name": "Mean Revert Short", "status": "paused"},
            {"id": "volume-expansion-long", "name": "Volume Expansion Long", "status": "active"},
        ],
        "rule_tree": {
            "type": "group",
            "operator": "ALL",
            "children": [
                {
                    "type": "condition",
                    "label": "Price Change %",
                    "timeframe": "1m",
                    "operator": ">",
                    "value": "20",
                    "note": "Detect vertical breakout impulse",
                },
                {
                    "type": "group",
                    "operator": "ALL",
                    "children": [
                        {
                            "type": "condition",
                            "label": "Close > SMA(30)",
                            "timeframe": "1m",
                            "operator": ">",
                            "value": "SMA30",
                            "note": "Trend filter",
                        },
                        {
                            "type": "condition",
                            "label": "RSI(14)",
                            "timeframe": "1m",
                            "operator": ">",
                            "value": "70",
                            "note": "Momentum confirmation",
                        },
                    ],
                },
                {
                    "type": "condition",
                    "label": "Spread %",
                    "timeframe": "live",
                    "operator": "<",
                    "value": "0.15",
                    "note": "Execution quality guard",
                },
            ],
        },
        "action": {
            "type": "open_long",
            "order_type": "market",
            "size_mode": "risk_percent",
            "risk_value": 0.5,
            "stop_loss": "ATR(14) * 1.5",
            "take_profit": "RR 2.5",
        },
        "evaluation": [
            {"label": "Price Change % > 20", "result": True, "value": "22.4"},
            {"label": "Close > SMA30", "result": True, "value": "true"},
            {"label": "RSI(14) > 70", "result": False, "value": "68.9"},
            {"label": "Spread % < 0.15", "result": True, "value": "0.04"},
        ],
        "catalog": get_condition_catalog(),
        "recent_user_events": runtime.get("recent_user_events", []),
        "positions": runtime.get("positions", []),
        "orders": runtime.get("orders", []),
        "tracked_symbols": tracked_symbols,
        "pending_symbols": runtime.get("pending_symbols", []),
        "tracked_symbol_states": tracked_symbol_states,
        "pending_symbol_states": pending_symbol_states,
        "stream_batches": {
            "market": stream_batches.get("market", []),
            "book": stream_batches.get("book", []),
            "weak": stream_batches.get("weak", []),
        },
        "candle_loader": runtime.get("candle_loader", {}),
    }


def _to_iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _load_recent_db_rows(symbol: str, required_rows: int) -> list[dict]:
    if not HISTORY_DB_PATH.exists():
        return []

    with sqlite3.connect(HISTORY_DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT open_time, close_time, open, high, low, close, volume
            FROM futures_klines_1m
            WHERE symbol = ?
            ORDER BY open_time DESC
            LIMIT ?
            """,
            (symbol, required_rows),
        ).fetchall()

    normalized_rows = [
        {
            "open_time": int(row[0]),
            "close_time": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
        }
        for row in reversed(rows)
    ]
    return normalized_rows


def _merge_live_rows(rows: list[dict], symbol: str) -> list[dict]:
    merged: dict[int, dict] = {int(row["open_time"]): dict(row) for row in rows}
    for item in get_live_kline_window(symbol):
        open_time = int(item.get("open_time") or 0)
        close_time = int(item.get("close_time") or 0)
        if open_time <= 0 or close_time <= 0:
            continue
        merged[open_time] = {
            "open_time": open_time,
            "close_time": close_time,
            "open": float(item.get("open") or 0.0),
            "high": float(item.get("high") or 0.0),
            "low": float(item.get("low") or 0.0),
            "close": float(item.get("close") or 0.0),
            "volume": float(item.get("volume") or 0.0),
        }
    return [merged[key] for key in sorted(merged)]


def _aggregate_rows(rows: list[dict], timeframe_minutes: int) -> list[dict]:
    if timeframe_minutes <= 1:
        return rows

    bucket_ms = timeframe_minutes * 60_000
    buckets: list[dict] = []
    current_bucket: dict | None = None

    for row in rows:
        bucket_open_time = (int(row["open_time"]) // bucket_ms) * bucket_ms
        if current_bucket is None or current_bucket["open_time"] != bucket_open_time:
            current_bucket = {
                "open_time": bucket_open_time,
                "close_time": int(row["close_time"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            buckets.append(current_bucket)
            continue

        current_bucket["close_time"] = int(row["close_time"])
        current_bucket["high"] = max(float(current_bucket["high"]), float(row["high"]))
        current_bucket["low"] = min(float(current_bucket["low"]), float(row["low"]))
        current_bucket["close"] = float(row["close"])
        current_bucket["volume"] = float(current_bucket["volume"]) + float(row["volume"])

    return buckets


def _build_sma_series(candles: list[dict], period: int) -> list[dict]:
    closes = [float(candle["close"]) for candle in candles]
    series: list[dict] = []
    for index in range(period - 1, len(candles)):
        window = closes[index + 1 - period:index + 1]
        series.append(
            {
                "time": candles[index]["time"],
                "value": round(sum(window) / len(window), 6),
            }
        )
    return series


def _build_rsi_series(candles: list[dict], period: int) -> list[dict]:
    if len(candles) < 2:
        return []

    closes = [float(candle["close"]) for candle in candles]
    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, len(closes)):
        delta = closes[index] - closes[index - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    if len(gains) < period:
        return []

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    series: list[dict] = []

    def _rsi_value(current_gain: float, current_loss: float) -> float:
        if current_loss == 0 and current_gain == 0:
            return 50.0
        if current_loss == 0:
            return 100.0
        rs = current_gain / current_loss
        return 100 - (100 / (1 + rs))

    series.append(
        {
            "time": candles[period]["time"],
            "value": round(_rsi_value(avg_gain, avg_loss), 2),
        }
    )

    for index in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[index]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[index]) / period
        series.append(
            {
                "time": candles[index + 1]["time"],
                "value": round(_rsi_value(avg_gain, avg_loss), 2),
            }
        )

    return series


def get_sample_chart_data(symbol: str = "BTCUSDT", timeframe: str = "1m", bars: int = 120) -> dict:
    normalized_symbol = symbol.upper()
    timeframe_key = timeframe if timeframe in TIMEFRAME_TO_MINUTES else "1m"
    timeframe_minutes = TIMEFRAME_TO_MINUTES[timeframe_key]
    required_base_rows = max(180, (bars + 40) * timeframe_minutes)

    base_rows = _load_recent_db_rows(normalized_symbol, required_base_rows)
    merged_rows = _merge_live_rows(base_rows, normalized_symbol)
    aggregated_rows = _aggregate_rows(merged_rows, timeframe_minutes)
    selected_rows = aggregated_rows[-bars:]

    candles = [
        {
            "time": _to_iso_from_ms(int(row["open_time"])),
            "open": round(float(row["open"]), 8),
            "high": round(float(row["high"]), 8),
            "low": round(float(row["low"]), 8),
            "close": round(float(row["close"]), 8),
            "volume": round(float(row["volume"]), 4),
        }
        for row in selected_rows
    ]

    return {
        "symbol": normalized_symbol,
        "timeframe": timeframe_key,
        "candles": candles,
        "series": {
            "sma_30": _build_sma_series(candles, 30),
            "rsi_14": _build_rsi_series(candles, 14),
        },
        "signals": [],
    }
