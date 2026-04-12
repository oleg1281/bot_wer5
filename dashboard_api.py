from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import Body
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from dashboard_state import (
    get_condition_catalog,
    get_overview_snapshot,
    get_sample_chart_data,
    get_strategy_builder_state,
)
from candle_loader import candle_loader_service
from manual_trading import get_manual_order_context, place_manual_order
from platform_control import get_supervisor_status, start_supervisor_task, stop_supervisor_task
from runtime_state import get_live_kline_window, snapshot_runtime_state

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "dashboard_static"
OVERVIEW_STREAM_INTERVAL_SECONDS = 0.25

app = FastAPI(title="Bot Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/system/overview")
async def api_system_overview():
    return get_overview_snapshot()


@app.get("/api/system/bot")
async def api_system_bot_status():
    return get_supervisor_status()


@app.post("/api/system/bot/start")
async def api_system_bot_start():
    started = await start_supervisor_task()
    return {
        "started": started,
        "state": get_supervisor_status(),
    }


@app.post("/api/system/bot/stop")
async def api_system_bot_stop():
    stopped = await stop_supervisor_task()
    return {
        "stopped": stopped,
        "state": get_supervisor_status(),
    }


@app.get("/api/strategy/builder")
async def api_strategy_builder():
    return get_strategy_builder_state()


@app.get("/api/strategy/catalog")
async def api_strategy_catalog():
    return {"catalog": get_condition_catalog()}


@app.get("/api/account/state")
async def api_account_state():
    runtime = snapshot_runtime_state()
    return {
        "positions": runtime.get("positions", []),
        "orders": runtime.get("orders", []),
        "balances": runtime.get("balances", []),
        "recent_user_events": runtime.get("recent_user_events", []),
    }


@app.get("/api/market/symbols")
async def api_market_symbols():
    runtime = snapshot_runtime_state()
    symbol_states = runtime.get("symbol_states", {})
    return {
        "tracked_symbols": runtime.get("tracked_symbols", []),
        "pending_symbols": runtime.get("pending_symbols", []),
        "tracked_symbol_states": [
            {"symbol": symbol, **symbol_states.get(symbol, {})}
            for symbol in runtime.get("tracked_symbols", [])
        ],
        "pending_symbol_states": [
            {"symbol": symbol, **symbol_states.get(symbol, {})}
            for symbol in runtime.get("pending_symbols", [])
        ],
    }


@app.get("/api/market/chart")
async def api_market_chart(symbol: str = "BTCUSDT", timeframe: str = "1m", bars: int = 120):
    bounded_bars = max(30, min(500, bars))
    return get_sample_chart_data(symbol=symbol.upper(), timeframe=timeframe, bars=bounded_bars)


@app.get("/api/market/live-window")
async def api_market_live_window(symbol: str = "BTCUSDT"):
    normalized_symbol = symbol.upper()
    runtime = snapshot_runtime_state()
    symbol_state = runtime.get("symbol_states", {}).get(normalized_symbol, {})
    live_window = get_live_kline_window(normalized_symbol)
    return {
        "symbol": normalized_symbol,
        "points": len(live_window),
        "window_seconds": 300,
        "symbol_state": symbol_state,
        "data": live_window,
    }


@app.get("/api/trading/manual/context")
async def api_trading_manual_context(symbol: str = "BTCUSDT"):
    try:
        return await get_manual_order_context(symbol)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/trading/manual/order")
async def api_trading_manual_order(payload: dict = Body(default={})):
    try:
        return await place_manual_order(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/loader/candles")
async def api_loader_candles_state():
    runtime = snapshot_runtime_state()
    return runtime.get("candle_loader", {})


@app.post("/api/loader/candles/settings")
async def api_loader_candles_settings(
    payload: dict = Body(default={}),
):
    start_delay = float(payload.get("start_delay_seconds", 0.7))
    return candle_loader_service.update_settings(start_delay)


@app.post("/api/loader/candles/start")
async def api_loader_candles_start():
    started = candle_loader_service.start()
    runtime = snapshot_runtime_state()
    return {
        "started": started,
        "state": runtime.get("candle_loader", {}),
    }


@app.post("/api/loader/candles/stop")
async def api_loader_candles_stop():
    stopped = candle_loader_service.stop()
    runtime = snapshot_runtime_state()
    return {
        "stopped": stopped,
        "state": runtime.get("candle_loader", {}),
    }


@app.websocket("/ws/overview")
async def ws_overview(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(
                {
                    "type": "overview_update",
                    "payload": get_overview_snapshot(),
                }
            )
            await asyncio.sleep(OVERVIEW_STREAM_INTERVAL_SECONDS)
    except WebSocketDisconnect:
        return
