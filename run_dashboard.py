import asyncio

from dashboard_server import run_dashboard_server

if __name__ == "__main__":
    try:
        asyncio.run(run_dashboard_server())
    except RuntimeError as exc:
        print(exc)
