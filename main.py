import asyncio

from platform_control import register_platform_loop, start_supervisor_task, wait_for_supervisor


async def run_platform():
    register_platform_loop(asyncio.get_running_loop())
    await start_supervisor_task()
    dashboard_task = None

    try:
        from dashboard_server import run_dashboard_server

        dashboard_task = asyncio.create_task(run_dashboard_server())
    except Exception as exc:
        print(f"Dashboard disabled: {exc}")

    if dashboard_task is not None:
        await dashboard_task
    else:
        await wait_for_supervisor()


if __name__ == "__main__":
    asyncio.run(run_platform())
