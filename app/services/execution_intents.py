from __future__ import annotations

from app.models.execution import ExecutionCandidate, OrderIntent


def candidate_to_order_intents(candidate: ExecutionCandidate) -> list[OrderIntent]:
    target_qty = candidate.target_position_pct

    base_notes = (
        "paper_only_translation_v1"
        if candidate.is_executable_now
        else f"paper_only_translation_v1_not_executable:{candidate.why_not_executable or 'unknown'}"
    )

    long_intent = OrderIntent(
        venue_id=candidate.long_exchange,
        symbol=candidate.symbol,
        side="buy",
        order_type="market",
        quantity=float(target_qty or 0.0),
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
        },
        notes=base_notes,
        is_live=False,
    )

    short_intent = OrderIntent(
        venue_id=candidate.short_exchange,
        symbol=candidate.symbol,
        side="sell",
        order_type="market",
        quantity=float(target_qty or 0.0),
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
        },
        notes=base_notes,
        is_live=False,
    )

    return [long_intent, short_intent]


def candidates_to_order_intents(candidates: list[ExecutionCandidate]) -> list[OrderIntent]:
    intents: list[OrderIntent] = []
    for candidate in candidates:
        intents.extend(candidate_to_order_intents(candidate))
    return intents
