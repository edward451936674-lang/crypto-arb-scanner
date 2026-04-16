from __future__ import annotations

import time

from app.execution_adapters.registry import get_execution_adapter
from app.models.execution import (
    DryRunExecutionAttempt,
    DryRunExecutionFailureReason,
    DryRunExecutionLegAttempt,
    ExecutionCandidate,
    ExecutionLegPreflight,
)
from app.services.execution_intents import candidate_to_order_intents
from app.services.execution_preflight import evaluate_execution_bundle_preflight

_SUBMIT_SEQUENCE_BY_SIDE: dict[str, tuple[int, int, str]] = {
    "buy": (0, 1, "first"),
    "sell": (1, 2, "second"),
}


def _map_preflight_blockers_to_failure_reasons(blockers: list[str]) -> list[DryRunExecutionFailureReason]:
    reasons: list[DryRunExecutionFailureReason] = ["preflight_blocked"]
    for blocker in blockers:
        if blocker in {"long_quantity_unresolved", "short_quantity_unresolved"}:
            reasons.append("quantity_unresolved")
        elif blocker in {"long_validation_error", "short_validation_error"}:
            reasons.append("validation_error")
        elif blocker == "unsupported_venue":
            reasons.append("unsupported_venue")
        elif blocker == "missing_route_key":
            reasons.append("missing_route_key")
    return sorted(set(reasons))


def _leg_attempt_from_preflight(leg: ExecutionLegPreflight) -> DryRunExecutionLegAttempt:
    leg_index, submit_sequence, submit_order = _SUBMIT_SEQUENCE_BY_SIDE[leg.side]
    return DryRunExecutionLegAttempt(
        venue_id=leg.venue_id,
        side=leg.side,
        symbol=leg.symbol,
        route_key=leg.route_key,
        leg_index=leg_index,
        submit_sequence=submit_sequence,
        submit_order=submit_order,
        quantity=leg.quantity,
        request_preview=None,
        submit_status="skipped",
        submit_message="skipped_due_to_preflight_blocked",
        accepted=False,
        supported_venue=leg.supported_venue,
        validation_errors=list(leg.validation_errors),
        validation_warnings=list(leg.validation_warnings),
    )


async def simulate_dry_run_execution_attempt(candidate: ExecutionCandidate) -> DryRunExecutionAttempt:
    created_at_ms = int(time.time() * 1000)
    preflight = await evaluate_execution_bundle_preflight(candidate)

    long_leg_attempt = _leg_attempt_from_preflight(preflight.long_leg)
    short_leg_attempt = _leg_attempt_from_preflight(preflight.short_leg)

    if preflight.bundle_status == "blocked":
        failure_reasons = _map_preflight_blockers_to_failure_reasons(preflight.blockers)
        return DryRunExecutionAttempt(
            attempt_id=f"dryrun:{candidate.route_key}:{created_at_ms}",
            route_key=candidate.route_key,
            symbol=candidate.symbol,
            long_leg=long_leg_attempt,
            short_leg=short_leg_attempt,
            bundle_status="blocked",
            failure_reasons=failure_reasons,
            submitted_leg_count=0,
            accepted_leg_count=0,
            preview_only=True,
            is_live=False,
            created_at_ms=created_at_ms,
        )

    intents = candidate_to_order_intents(candidate)
    intents_by_side = {intent.side: intent for intent in intents}

    failure_reasons: list[DryRunExecutionFailureReason] = []
    leg_attempts: dict[str, DryRunExecutionLegAttempt] = {}

    for side in ("buy", "sell"):
        leg_index, submit_sequence, submit_order = _SUBMIT_SEQUENCE_BY_SIDE[side]
        intent = intents_by_side[side]
        try:
            adapter = get_execution_adapter(intent.venue_id)
        except ValueError:
            leg_attempts[side] = DryRunExecutionLegAttempt(
                venue_id=intent.venue_id,
                side=intent.side,
                symbol=intent.symbol,
                route_key=str(intent.route_key or ""),
                leg_index=leg_index,
                submit_sequence=submit_sequence,
                submit_order=submit_order,
                quantity=intent.quantity,
                request_preview=None,
                submit_status="rejected",
                submit_message="unsupported_venue",
                accepted=False,
                supported_venue=False,
                validation_errors=["unsupported_venue"],
                validation_warnings=[],
            )
            failure_reasons.append("unsupported_venue")
            continue

        submit_result = await adapter.place_order(intent)
        preview = submit_result.translation.preview if submit_result.translation is not None else None
        validation_errors = list(preview.validation_errors) if preview is not None else []
        validation_warnings = list(preview.validation_warnings) if preview is not None else []
        leg_attempts[side] = DryRunExecutionLegAttempt(
            venue_id=intent.venue_id,
            side=intent.side,
            symbol=intent.symbol,
            route_key=str(intent.route_key or ""),
            leg_index=leg_index,
            submit_sequence=submit_sequence,
            submit_order=submit_order,
            quantity=intent.quantity,
            request_preview=preview,
            submit_status="accepted" if submit_result.accepted else "rejected",
            submit_message=submit_result.message,
            accepted=submit_result.accepted,
            supported_venue=True,
            validation_errors=validation_errors,
            validation_warnings=validation_warnings,
        )

    long_leg_attempt = leg_attempts["buy"]
    short_leg_attempt = leg_attempts["sell"]

    submitted_leg_count = sum(1 for item in (long_leg_attempt, short_leg_attempt) if item.submit_status != "skipped")
    accepted_leg_count = sum(1 for item in (long_leg_attempt, short_leg_attempt) if item.accepted)

    if long_leg_attempt.accepted and short_leg_attempt.accepted:
        bundle_status = "accepted"
    else:
        bundle_status = "failed"
        if not long_leg_attempt.accepted:
            failure_reasons.append("long_leg_submit_rejected")
        if not short_leg_attempt.accepted:
            failure_reasons.append("short_leg_submit_rejected")
        if long_leg_attempt.validation_errors or short_leg_attempt.validation_errors:
            failure_reasons.append("validation_error")

    return DryRunExecutionAttempt(
        attempt_id=f"dryrun:{candidate.route_key}:{created_at_ms}",
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_leg=long_leg_attempt,
        short_leg=short_leg_attempt,
        bundle_status=bundle_status,
        failure_reasons=sorted(set(failure_reasons)),
        submitted_leg_count=submitted_leg_count,
        accepted_leg_count=accepted_leg_count,
        preview_only=True,
        is_live=False,
        created_at_ms=created_at_ms,
    )


async def simulate_dry_run_execution_attempts(candidates: list[ExecutionCandidate]) -> list[DryRunExecutionAttempt]:
    attempts: list[DryRunExecutionAttempt] = []
    for candidate in candidates:
        attempts.append(await simulate_dry_run_execution_attempt(candidate))
    return attempts
