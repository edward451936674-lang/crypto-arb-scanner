from __future__ import annotations

from app.models.execution import ExecutionCandidate, OrderIntent
from app.services.execution_quantity_resolver import quantity_resolver


def candidate_to_order_intents(candidate: ExecutionCandidate) -> list[OrderIntent]:
    quantity_resolution = quantity_resolver.resolve(candidate)
    unresolved_legs = [
        leg
        for leg, resolved_qty in (
            ("long", quantity_resolution.resolved_quantity_long),
            ("short", quantity_resolution.resolved_quantity_short),
        )
        if resolved_qty is None
    ]
    base_notes = (
        "paper_only_translation_v1"
        if candidate.is_executable_now
        else f"paper_only_translation_v1_not_executable:{candidate.why_not_executable or 'unknown'}"
    )
    resolution_note = (
        f"quantity_resolution:{quantity_resolution.quantity_resolution_status}"
        f":{quantity_resolution.quantity_resolution_source}"
    )
    if unresolved_legs:
        resolution_note = f"{resolution_note}:unresolved_{','.join(unresolved_legs)}"

    long_intent = OrderIntent(
        venue_id=candidate.long_exchange,
        symbol=candidate.symbol,
        side="buy",
        order_type="market",
        quantity=quantity_resolution.resolved_quantity_long,
        price=candidate.entry_reference_price_long,
        time_in_force=None,
        reduce_only=False,
        client_order_id=f"{candidate.route_key}:long",
        route_key=candidate.route_key,
        target_position_pct=candidate.target_position_pct,
        target_notional_usd=candidate.target_notional_usd,
        max_slippage_bps=candidate.max_slippage_bps,
        max_order_age_ms=candidate.max_order_age_ms,
        metadata={
            "leg": "long",
            "opportunity_type": candidate.opportunity_type,
            "execution_mode": candidate.execution_mode,
            "is_test": candidate.is_test,
            "quantity_resolution_status": quantity_resolution.quantity_resolution_status,
            "quantity_resolution_source": quantity_resolution.quantity_resolution_source,
            "quantity_resolution_warnings": quantity_resolution.warnings,
            "quantity_unresolved_legs": unresolved_legs,
        },
        notes=f"{base_notes};{resolution_note}",
        is_live=False,
    )

    short_intent = OrderIntent(
        venue_id=candidate.short_exchange,
        symbol=candidate.symbol,
        side="sell",
        order_type="market",
        quantity=quantity_resolution.resolved_quantity_short,
        price=candidate.entry_reference_price_short,
        time_in_force=None,
        reduce_only=False,
        client_order_id=f"{candidate.route_key}:short",
        route_key=candidate.route_key,
        target_position_pct=candidate.target_position_pct,
        target_notional_usd=candidate.target_notional_usd,
        max_slippage_bps=candidate.max_slippage_bps,
        max_order_age_ms=candidate.max_order_age_ms,
        metadata={
            "leg": "short",
            "opportunity_type": candidate.opportunity_type,
            "execution_mode": candidate.execution_mode,
            "is_test": candidate.is_test,
            "quantity_resolution_status": quantity_resolution.quantity_resolution_status,
            "quantity_resolution_source": quantity_resolution.quantity_resolution_source,
            "quantity_resolution_warnings": quantity_resolution.warnings,
            "quantity_unresolved_legs": unresolved_legs,
        },
        notes=f"{base_notes};{resolution_note}",
        is_live=False,
    )

    return [long_intent, short_intent]


def candidates_to_order_intents(candidates: list[ExecutionCandidate]) -> list[OrderIntent]:
    intents: list[OrderIntent] = []
    for candidate in candidates:
        intents.extend(candidate_to_order_intents(candidate))
    return intents
