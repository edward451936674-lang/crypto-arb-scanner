from __future__ import annotations

from app.core.config import Settings, get_settings
from app.execution_adapters.registry import get_execution_adapter_capability
from app.models.execution import (
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionCredentialReadinessDecision,
    ExecutionPolicyDecision,
    LiveExecutionEntryConfigSnapshot,
    LiveExecutionEntryResult,
)
from app.venues.registry import list_venue_definitions


def _normalize_venues(items: list[str]) -> list[str]:
    return sorted({item.strip().lower() for item in items if item and item.strip()})


def resolve_live_execution_entry_config_snapshot(settings: Settings | None = None) -> LiveExecutionEntryConfigSnapshot:
    resolved_settings = settings or get_settings()
    return LiveExecutionEntryConfigSnapshot(
        live_execution_enabled=bool(resolved_settings.live_execution_enabled),
        live_execution_allowed_venues=_normalize_venues(list(resolved_settings.live_execution_allowed_venues)),
    )


def _is_adapter_stub_only(venue_id: str) -> bool:
    return get_execution_adapter_capability(venue_id).stub_only


def evaluate_live_execution_entry_decision(
    *,
    candidate: ExecutionCandidate,
    preflight: ExecutionBundlePreflight,
    policy_decision: ExecutionPolicyDecision,
    credential_readiness_decision: ExecutionCredentialReadinessDecision,
    config: LiveExecutionEntryConfigSnapshot,
) -> LiveExecutionEntryResult:
    block_reasons: list[str] = []
    warnings = sorted(
        {
            *preflight.warnings,
            *policy_decision.warnings,
            *credential_readiness_decision.warnings,
            *(f"credential_readiness:{reason}" for reason in credential_readiness_decision.block_reasons),
        }
    )

    if not config.live_execution_enabled:
        block_reasons.append("live_execution_not_enabled")

    if policy_decision.policy_status != "allowed":
        block_reasons.append("policy_blocked")

    venue_by_id = {item.venue_id.value: item for item in list_venue_definitions()}
    allowed_venue_set = set(config.live_execution_allowed_venues)
    live_venues = {
        candidate.long_exchange.lower(): venue_by_id.get(candidate.long_exchange.lower()),
        candidate.short_exchange.lower(): venue_by_id.get(candidate.short_exchange.lower()),
    }

    for venue_id, venue_definition in live_venues.items():
        venue_is_live_supported = bool(venue_definition and venue_definition.capabilities.live_supported_now)
        venue_is_allowlisted = not allowed_venue_set or venue_id in allowed_venue_set
        if not venue_is_live_supported or not venue_is_allowlisted:
            block_reasons.append("venue_not_live_enabled")
            if not venue_is_allowlisted:
                warnings.append(f"venue_not_in_live_allowlist:{venue_id}")

    if _is_adapter_stub_only(candidate.long_exchange) or _is_adapter_stub_only(candidate.short_exchange):
        block_reasons.append("adapter_is_stub_only")

    if credential_readiness_decision.credential_readiness_status != "allowed":
        block_reasons.append("credential_readiness_blocked")

    unique_block_reasons = sorted(set(block_reasons))
    entry_status = "allowed" if not unique_block_reasons else "blocked"

    return LiveExecutionEntryResult(
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        policy_status=policy_decision.policy_status,
        entry_status=entry_status,
        allowed_to_enter_live_path=entry_status == "allowed",
        block_reasons=unique_block_reasons,
        warnings=sorted(set(warnings)),
        preview_only=True,
        is_live=False,
    )


def evaluate_live_execution_entry_decisions(
    *,
    candidates: list[ExecutionCandidate],
    preflight_bundles: list[ExecutionBundlePreflight],
    policy_decisions: list[ExecutionPolicyDecision],
    credential_readiness_decisions: list[ExecutionCredentialReadinessDecision],
    config: LiveExecutionEntryConfigSnapshot,
) -> list[LiveExecutionEntryResult]:
    return [
        evaluate_live_execution_entry_decision(
            candidate=candidate,
            preflight=preflight,
            policy_decision=policy_decision,
            credential_readiness_decision=credential_readiness_decision,
            config=config,
        )
        for candidate, preflight, policy_decision, credential_readiness_decision in zip(
            candidates,
            preflight_bundles,
            policy_decisions,
            credential_readiness_decisions,
            strict=False,
        )
    ]
