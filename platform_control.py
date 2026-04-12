from __future__ import annotations

import asyncio

from logger_setup import setup_logger
from runtime_state import update_overview

logger = setup_logger()

_PLATFORM_LOOP: asyncio.AbstractEventLoop | None = None
_SUPERVISOR_TASK: asyncio.Task | None = None


def _handle_supervisor_done(task: asyncio.Task) -> None:
    global _SUPERVISOR_TASK

    if _SUPERVISOR_TASK is task:
        _SUPERVISOR_TASK = None

    if task.cancelled():
        update_overview(bot_status="stopped", exchange_status="disconnected")
        return

    exception = task.exception()
    if exception is None:
        update_overview(bot_status="stopped", exchange_status="disconnected")
    else:
        logger.error(f"platform-control: supervisor stopped with error: {exception}")
        update_overview(bot_status="stopped", exchange_status="disconnected")


def register_platform_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _PLATFORM_LOOP
    _PLATFORM_LOOP = loop


def get_supervisor_status() -> dict:
    running = bool(_SUPERVISOR_TASK and not _SUPERVISOR_TASK.done())
    return {
        "running": running,
        "task_exists": _SUPERVISOR_TASK is not None,
    }


async def start_supervisor_task() -> bool:
    global _SUPERVISOR_TASK

    if _SUPERVISOR_TASK and not _SUPERVISOR_TASK.done():
        return False

    if _PLATFORM_LOOP is None:
        raise RuntimeError("Platform loop is not registered.")

    from supervisor import main as supervisor_main

    _SUPERVISOR_TASK = _PLATFORM_LOOP.create_task(supervisor_main())
    _SUPERVISOR_TASK.add_done_callback(_handle_supervisor_done)
    update_overview(bot_status="starting", exchange_status="connecting")
    logger.info("platform-control: supervisor start requested")
    return True


async def stop_supervisor_task() -> bool:
    global _SUPERVISOR_TASK

    if _SUPERVISOR_TASK is None or _SUPERVISOR_TASK.done():
        update_overview(bot_status="stopped", exchange_status="disconnected")
        return False

    task = _SUPERVISOR_TASK
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        if _SUPERVISOR_TASK is task:
            _SUPERVISOR_TASK = None

    update_overview(bot_status="stopped", exchange_status="disconnected")
    logger.info("platform-control: supervisor stopped")
    return True


async def wait_for_supervisor() -> None:
    if _SUPERVISOR_TASK is not None:
        await _SUPERVISOR_TASK
