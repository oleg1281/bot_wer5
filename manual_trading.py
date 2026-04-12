from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any

from binance import AsyncClient

from config import api_key, api_secret
from logger_setup import setup_logger
from runtime_state import append_user_event, snapshot_runtime_state

logger = setup_logger()

SUPPORTED_ORDER_TYPES = {
    "MARKET",
    "LIMIT",
    "STOP_MARKET",
    "TAKE_PROFIT_MARKET",
}


@dataclass
class SymbolFilters:
    symbol: str
    status: str
    price_tick_size: Decimal
    quantity_step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    quantity_precision: int
    price_precision: int
    trigger_protect: Decimal | None
    max_leverage: int | None


async def _get_position_mode(client: AsyncClient) -> dict[str, Any]:
    try:
        response = await client.futures_get_position_mode()
    except Exception:
        return {
            "hedge_mode": False,
            "mode_label": "ONE_WAY",
            "allowed_position_sides": ["BOTH"],
        }

    hedge_mode = bool(response.get("dualSidePosition"))
    return {
        "hedge_mode": hedge_mode,
        "mode_label": "HEDGE" if hedge_mode else "ONE_WAY",
        "allowed_position_sides": ["LONG", "SHORT"] if hedge_mode else ["BOTH"],
    }


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def _format_decimal(value: Decimal, precision: int | None = None) -> str:
    if precision is not None and precision >= 0:
        quant = Decimal("1").scaleb(-precision)
        return format(value.quantize(quant, rounding=ROUND_DOWN), "f")
    return format(value.normalize(), "f")


async def _create_client() -> AsyncClient:
    return await AsyncClient.create(
        api_key=api_key.strip() if api_key else None,
        api_secret=api_secret.strip() if api_secret else None,
    )


def _extract_symbol_filters(exchange_info: dict, symbol: str) -> SymbolFilters:
    for symbol_info in exchange_info.get("symbols", []):
        if symbol_info.get("symbol") != symbol:
            continue

        filters = {item.get("filterType"): item for item in symbol_info.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        min_notional_filter = filters.get("MIN_NOTIONAL", {})
        leverage_filter = filters.get("MAX_NUM_ORDERS", {})

        return SymbolFilters(
            symbol=symbol,
            status=symbol_info.get("status", "UNKNOWN"),
            price_tick_size=_decimal(price_filter.get("tickSize"), "0.0001"),
            quantity_step_size=_decimal(lot_filter.get("stepSize"), "0.001"),
            min_qty=_decimal(lot_filter.get("minQty"), "0.001"),
            min_notional=_decimal(min_notional_filter.get("notional"), "5"),
            quantity_precision=int(symbol_info.get("quantityPrecision") or 3),
            price_precision=int(symbol_info.get("pricePrecision") or 4),
            trigger_protect=(
                _decimal(symbol_info.get("triggerProtect"))
                if symbol_info.get("triggerProtect") is not None
                else None
            ),
            max_leverage=int(leverage_filter.get("limit") or 0) or None,
        )

    raise ValueError(f"Symbol {symbol} was not found in futures exchange info")


def _account_snapshot_for_symbol(symbol: str) -> dict[str, Any]:
    runtime = snapshot_runtime_state()
    positions = runtime.get("positions", [])
    balances = runtime.get("balances", [])
    symbol_position = next((item for item in positions if item.get("s") == symbol), None)
    usdt_balance = next((item for item in balances if item.get("a") == "USDT"), None)
    return {
        "position": symbol_position,
        "usdt_balance": usdt_balance,
    }


async def get_manual_order_context(symbol: str) -> dict[str, Any]:
    normalized_symbol = symbol.upper()
    client = await _create_client()
    try:
        exchange_info = await client.futures_exchange_info()
        mark_price = await client.futures_mark_price(symbol=normalized_symbol)
        try:
            commission_rate = await client.futures_commission_rate(symbol=normalized_symbol)
        except Exception:
            commission_rate = {}
        try:
            leverage_bracket = await client.futures_leverage_bracket(symbol=normalized_symbol)
        except Exception:
            leverage_bracket = []
        filters = _extract_symbol_filters(exchange_info, normalized_symbol)
        bracket_info = leverage_bracket[0] if leverage_bracket else {}
        brackets = bracket_info.get("brackets", [])
        max_initial_leverage = max(
            (int(item.get("initialLeverage") or 0) for item in brackets),
            default=20,
        )
        account_snapshot = _account_snapshot_for_symbol(normalized_symbol)
        position_mode = await _get_position_mode(client)
        maker_commission_rate = _decimal(commission_rate.get("makerCommissionRate"))
        taker_commission_rate = _decimal(commission_rate.get("takerCommissionRate"))

        return {
            "symbol": normalized_symbol,
            "status": filters.status,
            "mark_price": float(mark_price.get("markPrice") or 0.0),
            "price_tick_size": _format_decimal(filters.price_tick_size, filters.price_precision),
            "quantity_step_size": _format_decimal(filters.quantity_step_size, filters.quantity_precision),
            "min_qty": _format_decimal(filters.min_qty, filters.quantity_precision),
            "min_notional": _format_decimal(filters.min_notional, 2),
            "price_precision": filters.price_precision,
            "quantity_precision": filters.quantity_precision,
            "trigger_protect": (
                _format_decimal(filters.trigger_protect, 6)
                if filters.trigger_protect is not None
                else None
            ),
            "max_leverage": max_initial_leverage,
            "maker_commission_rate": _format_decimal(maker_commission_rate, 6),
            "taker_commission_rate": _format_decimal(taker_commission_rate, 6),
            "supported_order_types": sorted(SUPPORTED_ORDER_TYPES),
            **position_mode,
            **account_snapshot,
        }
    finally:
        await client.close_connection()


def _validate_and_build_payload(
    payload: dict[str, Any],
    filters: SymbolFilters,
    fallback_reference_price: Decimal | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    side = str(payload.get("side") or "").upper()
    order_type = str(payload.get("order_type") or "").upper()
    position_side = str(payload.get("position_side") or "").upper()
    quantity = _decimal(payload.get("quantity"))
    price = _decimal(payload.get("price"))
    stop_price = _decimal(payload.get("stop_price"))
    leverage = int(payload.get("leverage") or 0) if payload.get("leverage") not in (None, "") else None
    reduce_only = bool(payload.get("reduce_only"))
    post_only = bool(payload.get("post_only"))
    hedge_mode = bool(payload.get("hedge_mode"))

    if side not in {"BUY", "SELL"}:
        raise ValueError("Side must be BUY or SELL")
    if order_type not in SUPPORTED_ORDER_TYPES:
        raise ValueError(f"Unsupported order type: {order_type}")
    if hedge_mode:
        if position_side not in {"LONG", "SHORT"}:
            raise ValueError("In Hedge Mode position side must be LONG or SHORT")
    else:
        position_side = "BOTH"
    if quantity <= 0:
        raise ValueError("Quantity must be greater than zero")
    if quantity < filters.min_qty:
        raise ValueError(f"Quantity is below minimum {filters.min_qty}")

    normalized_quantity = _floor_to_step(quantity, filters.quantity_step_size)
    if normalized_quantity <= 0:
        raise ValueError("Quantity becomes zero after exchange step normalization")

    order_payload: dict[str, Any] = {
        "symbol": filters.symbol,
        "side": side,
        "type": order_type,
        "quantity": _format_decimal(normalized_quantity, filters.quantity_precision),
        "newOrderRespType": "RESULT",
    }
    if hedge_mode:
        order_payload["positionSide"] = position_side

    normalized_price: Decimal | None = None
    normalized_stop_price: Decimal | None = None

    if order_type == "LIMIT":
        if price <= 0:
            raise ValueError("Price is required for LIMIT order")
        normalized_price = _floor_to_step(price, filters.price_tick_size)
        order_payload["price"] = _format_decimal(normalized_price, filters.price_precision)
        order_payload["timeInForce"] = "GTX" if post_only else "GTC"
    elif order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
        if stop_price <= 0:
            raise ValueError("Stop price is required for trigger order")
        normalized_stop_price = _floor_to_step(stop_price, filters.price_tick_size)
        order_payload["stopPrice"] = _format_decimal(normalized_stop_price, filters.price_precision)
        order_payload["workingType"] = "MARK_PRICE"

    reference_price = normalized_price or normalized_stop_price or price or stop_price or fallback_reference_price
    notional_price = reference_price if reference_price and reference_price > 0 else None
    if notional_price is not None:
        estimated_notional = normalized_quantity * notional_price
        if estimated_notional < filters.min_notional:
            raise ValueError(
                f"Estimated notional {estimated_notional} is below minimum {filters.min_notional}"
            )
    else:
        estimated_notional = None

    # Binance Futures rejects reduceOnly in Hedge Mode.
    if reduce_only and not hedge_mode:
        order_payload["reduceOnly"] = "true"

    preview = {
        "symbol": filters.symbol,
        "side": side,
        "order_type": order_type,
        "quantity": _format_decimal(normalized_quantity, filters.quantity_precision),
        "position_side": position_side,
        "price": _format_decimal(normalized_price, filters.price_precision) if normalized_price else None,
        "stop_price": (
            _format_decimal(normalized_stop_price, filters.price_precision)
            if normalized_stop_price
            else None
        ),
        "reduce_only": reduce_only and not hedge_mode,
        "post_only": post_only if order_type == "LIMIT" else False,
        "leverage": leverage,
        "estimated_notional": (
            _format_decimal(estimated_notional, 2) if estimated_notional is not None else None
        ),
    }
    return order_payload, preview


async def place_manual_order(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        raise ValueError("Symbol is required")

    client = await _create_client()
    try:
        exchange_info = await client.futures_exchange_info()
        filters = _extract_symbol_filters(exchange_info, symbol)
        if filters.status != "TRADING":
            raise ValueError(f"Symbol {symbol} is not in TRADING status")

        position_mode = await _get_position_mode(client)
        mark_price = await client.futures_mark_price(symbol=symbol)
        reference_mark_price = _decimal(mark_price.get("markPrice"))
        order_payload, preview = _validate_and_build_payload(
            {
                **payload,
                "hedge_mode": position_mode["hedge_mode"],
            },
            filters,
            fallback_reference_price=reference_mark_price,
        )
        leverage = preview.get("leverage")
        leverage_result = None

        if leverage:
            leverage_result = await client.futures_change_leverage(symbol=symbol, leverage=int(leverage))

        order_result = await client.futures_create_order(**order_payload)
        append_user_event(
            {
                "event_type": "manual_order_submit",
                "symbol": symbol,
                "side": preview.get("side"),
                "position_side": preview.get("position_side"),
                "order_type": preview.get("order_type"),
                "quantity": preview.get("quantity"),
                "price": preview.get("price"),
                "stop_price": preview.get("stop_price"),
            }
        )

        return {
            "ok": True,
            "symbol": symbol,
            "preview": preview,
            "position_mode": position_mode,
            "leverage_result": leverage_result,
            "order": order_result,
        }
    except Exception as exc:
        logger.error(f"manual-trading: {type(exc).__name__}: {exc}")
        raise
    finally:
        await client.close_connection()
