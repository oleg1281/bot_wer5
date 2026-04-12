from __future__ import annotations

async def run_dashboard_server(host: str = "0.0.0.0", port: int = 8001):
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dashboard dependencies are not installed. Install 'uvicorn' and 'fastapi' first."
        ) from exc

    config = uvicorn.Config("dashboard_api:app", host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()
