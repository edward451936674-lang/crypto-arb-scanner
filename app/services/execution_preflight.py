from __future__ import annotations

from dataclasses import dataclass

from app.execution_adapters.registry import get_execution_adapter
from app.models.execution import (
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionLegPreflight,
    ExecutionPreflightBlocker,
    OrderIntent,
    VenueTranslationResult,
)
from app.services.execution_intents import candidate_to_order_intents


@dataclass(frozen=True)
class _IntentPreviewPair:
    intent: OrderIntent
    translation: VenueTranslationResult | None
    supported_venue: bool


def _to_leg_preflight(pair: _IntentPreviewPair) -> ExecutionLegPreflight:
    metadata = pair.intent.metadata or {}
    translation = pair.translation
    preview = translation.preview if translation is not None else None
    validation_errors = list(preview.validation_errors) if preview is not None else []
    validation_warnings = list(preview.validation_warnings) if preview is not None else []
    quantity_resolution_status = str(metadata.get("quantity_resolution_status") or "unavailable")
    is_ready = (
        pair.supported_venue
        and pair.intent.quantity is not None
        and preview is not None
        and not validation_errors
    )
    return ExecutionLegPreflight(
        venue_id=pair.intent.venue_id,
        side=pair.intent.side,
        symbol=pair.intent.symbol,
        route_key=str(pair.intent.route_key or ""),
        quantity=pair.intent.quantity,
        quantity_resolution_status=quantity_resolution_status,
        request_preview_available=preview is not None,
        validation_errors=validation_errors,
        validation_warnings=validation_warnings,
        supported_venue=pair.supported_venue,
        is_ready=is_ready,
    )


def _build_blockers(*, long_leg: ExecutionLegPreflight, short_leg: ExecutionLegPreflight, route_key: str) -> list[ExecutionPreflightBlocker]:
    blockers: list[ExecutionPreflightBlocker] = []
    if not route_key.strip():
        blockers.append("missing_route_key")
    if long_leg.quantity is None:
        blockers.append("long_quantity_unresolved")
    if short_leg.quantity is None:
        blockers.append("short_quantity_unresolved")
    if long_leg.validation_errors:
        blockers.append("long_validation_error")
    if short_leg.validation_errors:
        blockers.append("short_validation_error")
    if not long_leg.supported_venue or not short_leg.supported_venue:
        blockers.append("unsupported_venue")
    return blockers


async def evaluate_execution_bundle_preflight(candidate: ExecutionCandidate) -> ExecutionBundlePreflight:
    intents = candidate_to_order_intents(candidate)
    intents_by_side = {intent.side: intent for intent in intents}

    pairs_by_side: dict[str, _IntentPreviewPair] = {}
    for side in ("buy", "sell"):
        intent = intents_by_side[side]
        try:
            adapter = get_execution_adapter(intent.venue_id)
        except ValueError:
            pairs_by_side[side] = _IntentPreviewPair(intent=intent, translation=None, supported_venue=False)
            continue
        result = await adapter.place_order(intent)
        pairs_by_side[side] = _IntentPreviewPair(
            intent=intent,
            translation=result.translation,
            supported_venue=True,
        )

    long_leg = _to_leg_preflight(pairs_by_side["buy"])
    short_leg = _to_leg_preflight(pairs_by_side["sell"])

    blockers = _build_blockers(long_leg=long_leg, short_leg=short_leg, route_key=candidate.route_key)
    warnings = sorted({*long_leg.validation_warnings, *short_leg.validation_warnings})

    bundle_status = "ready" if not blockers and long_leg.is_ready and short_leg.is_ready else "blocked"

    return ExecutionBundlePreflight(
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_leg=long_leg,
        short_leg=short_leg,
        bundle_status=bundle_status,
        blockers=blockers,
        warnings=warnings,
        is_executable_bundle=bundle_status == "ready",
        preview_only=True,
        is_live=False,
    )


async def evaluate_execution_preflight_bundles(candidates: list[ExecutionCandidate]) -> list[ExecutionBundlePreflight]:
    bundles: list[ExecutionBundlePreflight] = []
    for candidate in candidates:
        bundles.append(await evaluate_execution_bundle_preflight(candidate))
    return bundles
