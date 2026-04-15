from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REGISTRY_DB_PATH = DATA_DIR / "symbols_registry.sqlite3"
PROJECT_STATUS_ACTIVE = "ACTIVE"
PROJECT_STATUS_DISABLED = "DISABLED"
ALLOWED_PROJECT_STATUSES = {
    PROJECT_STATUS_ACTIVE,
    PROJECT_STATUS_DISABLED,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(REGISTRY_DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def ensure_symbols_registry() -> None:
    with _connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS symbols_registry (
                symbol TEXT PRIMARY KEY,
                quote_asset TEXT,
                contract_type TEXT,
                onboard_time INTEGER NOT NULL DEFAULT 0,
                exchange_status TEXT NOT NULL DEFAULT 'MISSING',
                project_status TEXT NOT NULL DEFAULT 'ACTIVE',
                is_missing_on_exchange INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_on_exchange_at TEXT
            )
            """
        )


def sync_symbols_registry(symbol_metadata: dict[str, dict]) -> dict[str, int]:
    ensure_symbols_registry()
    now_iso = _utc_now_iso()
    fresh_symbols = set(symbol_metadata.keys())
    missing_symbols: list[str] = []

    with _connect() as connection:
        existing_rows = connection.execute(
            """
            SELECT symbol
            FROM symbols_registry
            """
        ).fetchall()
        existing_symbols = {row["symbol"] for row in existing_rows}

        added_count = 0
        updated_count = 0

        for symbol, meta in sorted(symbol_metadata.items()):
            if symbol in existing_symbols:
                connection.execute(
                    """
                    UPDATE symbols_registry
                    SET quote_asset = ?,
                        contract_type = ?,
                        onboard_time = ?,
                        exchange_status = ?,
                        is_missing_on_exchange = 0,
                        updated_at = ?,
                        last_seen_on_exchange_at = ?
                    WHERE symbol = ?
                    """,
                    (
                        meta.get("quote_asset"),
                        meta.get("contract_type"),
                        int(meta.get("onboard_time") or 0),
                        meta.get("status", "UNKNOWN"),
                        now_iso,
                        now_iso,
                        symbol,
                    ),
                )
                updated_count += 1
                continue

            connection.execute(
                """
                INSERT INTO symbols_registry (
                    symbol,
                    quote_asset,
                    contract_type,
                    onboard_time,
                    exchange_status,
                    project_status,
                    is_missing_on_exchange,
                    created_at,
                    updated_at,
                    last_seen_on_exchange_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    symbol,
                    meta.get("quote_asset"),
                    meta.get("contract_type"),
                    int(meta.get("onboard_time") or 0),
                    meta.get("status", "UNKNOWN"),
                    PROJECT_STATUS_ACTIVE,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
            added_count += 1

        for row in existing_rows:
            symbol = row["symbol"]
            if symbol not in fresh_symbols:
                missing_symbols.append(symbol)

        if missing_symbols:
            connection.executemany(
                """
                UPDATE symbols_registry
                SET exchange_status = 'MISSING',
                    is_missing_on_exchange = 1,
                    updated_at = ?
                WHERE symbol = ?
                """,
                [(now_iso, symbol) for symbol in missing_symbols],
            )

    return {
        "added": added_count,
        "updated": updated_count,
        "missing": len(missing_symbols),
        "total": len(symbol_metadata),
    }


def list_registry_symbols() -> list[dict]:
    ensure_symbols_registry()
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT
                symbol,
                quote_asset,
                contract_type,
                onboard_time,
                exchange_status,
                project_status,
                is_missing_on_exchange,
                created_at,
                updated_at,
                last_seen_on_exchange_at
            FROM symbols_registry
            ORDER BY symbol ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_active_trading_symbols() -> list[str]:
    ensure_symbols_registry()
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT symbol
            FROM symbols_registry
            WHERE exchange_status = 'TRADING'
              AND project_status = ?
            ORDER BY symbol ASC
            """,
            (PROJECT_STATUS_ACTIVE,),
        ).fetchall()
    return [row["symbol"] for row in rows]


def get_pending_registry_symbols() -> list[str]:
    ensure_symbols_registry()
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT symbol
            FROM symbols_registry
            WHERE project_status = ?
              AND exchange_status != 'TRADING'
            ORDER BY symbol ASC
            """,
            (PROJECT_STATUS_ACTIVE,),
        ).fetchall()
    return [row["symbol"] for row in rows]


def update_symbol_project_status(symbol: str, project_status: str) -> dict:
    ensure_symbols_registry()

    symbol_name = str(symbol or "").strip().upper()
    if not symbol_name:
        raise ValueError("Symbol is required")

    normalized_status = str(project_status or "").strip().upper()
    if normalized_status not in ALLOWED_PROJECT_STATUSES:
        raise ValueError(f"Unsupported project status: {project_status}")

    now_iso = _utc_now_iso()
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE symbols_registry
            SET project_status = ?,
                updated_at = ?
            WHERE symbol = ?
            """,
            (normalized_status, now_iso, symbol_name),
        )
        if cursor.rowcount <= 0:
            raise ValueError(f"Symbol is not registered: {symbol_name}")

        row = connection.execute(
            """
            SELECT
                symbol,
                quote_asset,
                contract_type,
                onboard_time,
                exchange_status,
                project_status,
                is_missing_on_exchange,
                created_at,
                updated_at,
                last_seen_on_exchange_at
            FROM symbols_registry
            WHERE symbol = ?
            """,
            (symbol_name,),
        ).fetchone()

    return dict(row)
