from __future__ import annotations

import asyncio

from binance.ws.reconnecting_websocket import ReconnectingWebsocket
from config import ENABLE_WS_QUEUE_METRICS
from runtime_state import clear_book_ws_socket_metrics, record_book_ws_socket_metrics


class InstrumentedQueue(asyncio.Queue):
    def __init__(self, owner, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._owner = owner
        self._sync_owner_metrics()

    def _sync_owner_metrics(self) -> None:
        if self._owner is None:
            return
        size = max(0, int(super().qsize()))
        peak = max(int(getattr(self._owner, "_codex_ws_queue_peak", 0)), size)
        setattr(self._owner, "_codex_ws_queue_size", size)
        setattr(self._owner, "_codex_ws_queue_peak", peak)
        socket_name = str(getattr(self._owner, "_codex_socket_name", "") or "")
        if socket_name:
            record_book_ws_socket_metrics(socket_name, size, peak)

    async def put(self, item):
        await super().put(item)
        self._sync_owner_metrics()

    def put_nowait(self, item):
        super().put_nowait(item)
        self._sync_owner_metrics()

    async def get(self):
        item = await super().get()
        self._sync_owner_metrics()
        return item

    def get_nowait(self):
        item = super().get_nowait()
        self._sync_owner_metrics()
        return item


def _install_queue_metrics(socket_obj) -> None:
    queue = getattr(socket_obj, "_queue", None)
    if isinstance(queue, InstrumentedQueue):
        queue._sync_owner_metrics()
        return

    instrumented_queue = InstrumentedQueue(socket_obj)
    if queue is not None:
        try:
            while True:
                instrumented_queue.put_nowait(queue.get_nowait())
        except asyncio.QueueEmpty:
            pass
    socket_obj._queue = instrumented_queue
    socket_obj._codex_ws_queue_size = instrumented_queue.qsize()
    socket_obj._codex_ws_queue_peak = instrumented_queue.qsize()


def apply_ws_queue_metrics_patch() -> None:
    if not ENABLE_WS_QUEUE_METRICS:
        return
    if getattr(ReconnectingWebsocket, "_codex_metrics_patch_applied", False):
        return

    original_init = ReconnectingWebsocket.__init__
    original_aexit = ReconnectingWebsocket.__aexit__

    def instrumented_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._codex_ws_queue_size = 0
        self._codex_ws_queue_peak = 0
        self._codex_socket_name = ""
        _install_queue_metrics(self)

    async def instrumented_aexit(self, exc_type, exc_val, exc_tb):
        socket_name = str(getattr(self, "_codex_socket_name", "") or "")
        if socket_name:
            clear_book_ws_socket_metrics(socket_name)
        return await original_aexit(self, exc_type, exc_val, exc_tb)

    ReconnectingWebsocket.__init__ = instrumented_init
    ReconnectingWebsocket.__aexit__ = instrumented_aexit
    ReconnectingWebsocket._codex_metrics_patch_applied = True


def attach_ws_metrics_target(socket_obj, socket_name: str) -> None:
    if not ENABLE_WS_QUEUE_METRICS:
        return
    socket_obj._codex_socket_name = str(socket_name or "")
    record_book_ws_socket_metrics(
        socket_obj._codex_socket_name,
        int(getattr(socket_obj, "_codex_ws_queue_size", 0)),
        int(getattr(socket_obj, "_codex_ws_queue_peak", 0)),
    )
