from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

from app.models.execution import OrderIntent


_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[\.A-Z:/a-z0-9_-]{1,36}$")


@dataclass(frozen=True)
class BinanceSymbolTradingRules:
    symbol: str
    allowed_order_types: frozenset[str]
    price_tick_size: Decimal | None = None
    lot_size_min_qty: Decimal | None = None
    lot_size_step_size: Decimal | None = None
    market_lot_size_min_qty: Decimal | None = None
    market_lot_size_step_size: Decimal | None = None
    min_notional: Decimal | None = None


@dataclass(frozen=True)
class BinanceNormalizationResult:
    normalized_quantity: Decimal | None
    normalized_price: Decimal | None
    normalization_applied: bool
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class BinanceOrderValidationResult:
    accepted: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    normalized_quantity: Decimal | None
    normalized_price: Decimal | None
    final_client_order_id: str
    normalization_applied: bool


def parse_binance_exchange_info_symbol_rules(exchange_info: dict[str, Any], symbol: str) -> BinanceSymbolTradingRules | None:
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        return None

    target_symbol = symbol.upper()
    payload: dict[str, Any] | None = None
    for item in symbols:
        if isinstance(item, dict) and str(item.get("symbol", "")).upper() == target_symbol:
            payload = item
            break
    if payload is None:
        return None

    filters = payload.get("filters") if isinstance(payload.get("filters"), list) else []
    filter_map: dict[str, dict[str, Any]] = {}
    for rule in filters:
        if not isinstance(rule, dict):
            continue
        filter_type = str(rule.get("filterType", "")).upper()
        if filter_type:
            filter_map[filter_type] = rule

    allowed_order_types = frozenset(str(item).upper() for item in (payload.get("orderTypes") or []) if item)

    price_filter = filter_map.get("PRICE_FILTER", {})
    lot_size_filter = filter_map.get("LOT_SIZE", {})
    market_lot_size_filter = filter_map.get("MARKET_LOT_SIZE", {})
    min_notional_filter = filter_map.get("MIN_NOTIONAL", {})

    return BinanceSymbolTradingRules(
        symbol=target_symbol,
        allowed_order_types=allowed_order_types,
        price_tick_size=_decimal_or_none(price_filter.get("tickSize")),
        lot_size_min_qty=_decimal_or_none(lot_size_filter.get("minQty")),
        lot_size_step_size=_decimal_or_none(lot_size_filter.get("stepSize")),
        market_lot_size_min_qty=_decimal_or_none(market_lot_size_filter.get("minQty")),
        market_lot_size_step_size=_decimal_or_none(market_lot_size_filter.get("stepSize")),
        min_notional=_decimal_or_none(min_notional_filter.get("notional") or min_notional_filter.get("minNotional")),
    )


def validate_and_normalize_order_intent(intent: OrderIntent, rules: BinanceSymbolTradingRules) -> BinanceOrderValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    order_type = str(intent.order_type or "market").upper()
    if rules.allowed_order_types and order_type not in rules.allowed_order_types:
        errors.append("unsupported_order_type")

    final_client_order_id = str(intent.client_order_id or "").strip()
    if not final_client_order_id:
        final_client_order_id = _generate_client_order_id(intent)
        warnings.append("client_order_id_generated")

    if len(final_client_order_id) > 36:
        errors.append("client_order_id_too_long")
    elif not _CLIENT_ORDER_ID_PATTERN.fullmatch(final_client_order_id):
        errors.append("client_order_id_invalid")

    quantity_raw = _decimal_or_none(intent.quantity)
    price_raw = _decimal_or_none(intent.price)

    if quantity_raw is None:
        errors.append("quantity_required")

    quantity_step = rules.lot_size_step_size
    min_qty = rules.lot_size_min_qty
    if order_type == "MARKET":
        quantity_step = rules.market_lot_size_step_size or rules.lot_size_step_size
        min_qty = rules.market_lot_size_min_qty or rules.lot_size_min_qty

    normalized_quantity = quantity_raw
    normalization_applied = False
    if normalized_quantity is not None and quantity_step is not None and quantity_step > 0:
        floored = _floor_to_step(normalized_quantity, quantity_step)
        if floored != normalized_quantity:
            warnings.append("quantity_step_misaligned")
            normalized_quantity = floored
            normalization_applied = True

    if normalized_quantity is not None and min_qty is not None and normalized_quantity < min_qty:
        errors.append("quantity_below_min_qty")

    normalized_price = price_raw
    if order_type == "LIMIT":
        if normalized_price is None:
            errors.append("price_required_for_limit")
        elif rules.price_tick_size is not None and rules.price_tick_size > 0:
            floored = _floor_to_step(normalized_price, rules.price_tick_size)
            if floored != normalized_price:
                warnings.append("price_tick_misaligned")
                normalized_price = floored
                normalization_applied = True
            if normalized_price <= 0:
                errors.append("price_tick_misaligned")

    if rules.min_notional is not None and normalized_quantity is not None:
        effective_price = normalized_price if normalized_price is not None else price_raw
        if effective_price is not None and effective_price > 0:
            if (effective_price * normalized_quantity) < rules.min_notional:
                errors.append("min_notional_not_met")

    return BinanceOrderValidationResult(
        accepted=not errors,
        errors=tuple(errors),
        warnings=tuple(warnings),
        normalized_quantity=normalized_quantity,
        normalized_price=normalized_price,
        final_client_order_id=final_client_order_id,
        normalization_applied=normalization_applied,
    )


def _generate_client_order_id(intent: OrderIntent) -> str:
    seed = "|".join(
        [
            str(intent.route_key or ""),
            str(intent.symbol or "").upper(),
            str(intent.side or "").lower(),
            str(intent.order_type or "market").lower(),
            str(intent.quantity if intent.quantity is not None else ""),
            str(intent.price if intent.price is not None else ""),
        ]
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:18]
    return f"arbp-{digest}"


def _decimal_or_none(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return result


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    units = (value / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return units * step
