from __future__ import annotations

from app.core.config import Settings, get_settings
from app.models.execution import (
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionPolicyConfigSnapshot,
    ExecutionPolicyDecision,
)


def _normalize_venues(items: list[str]) -> list[str]:
    return sorted({item.strip().lower() for item in items if item and item.strip()})


def _normalize_symbols(items: list[str]) -> list[str]:
    return sorted({item.strip().upper() for item in items if item and item.strip()})


def resolve_execution_policy_config_snapshot(settings: Settings | None = None) -> ExecutionPolicyConfigSnapshot:
    resolved_settings = settings or get_settings()
    return ExecutionPolicyConfigSnapshot(
        execution_enabled=bool(resolved_settings.execution_policy_execution_enabled),
        allow_test_execution=bool(resolved_settings.execution_policy_allow_test_execution),
        allowed_venues=_normalize_venues(list(resolved_settings.execution_policy_allowed_venues)),
        allowed_symbols=_normalize_symbols(list(resolved_settings.execution_policy_allowed_symbols)),
        blocked_symbols=_normalize_symbols(list(resolved_settings.execution_policy_blocked_symbols)),
        max_target_notional_usd=resolved_settings.execution_policy_max_target_notional_usd,
    )


def evaluate_execution_policy_decision(
    *,
    candidate: ExecutionCandidate,
    preflight: ExecutionBundlePreflight,
    config: ExecutionPolicyConfigSnapshot,
) -> ExecutionPolicyDecision:
    block_reasons: list[str] = []

    if not config.execution_enabled:
        block_reasons.append("execution_globally_disabled")
    if preflight.bundle_status != "ready":
        block_reasons.append("preflight_not_ready")
    if candidate.is_test and not config.allow_test_execution:
        block_reasons.append("test_bundle_not_allowed")

    allowed_venues = set(config.allowed_venues)
    if candidate.long_exchange.lower() not in allowed_venues:
        block_reasons.append("long_venue_not_allowed")
    if candidate.short_exchange.lower() not in allowed_venues:
        block_reasons.append("short_venue_not_allowed")

    allowed_symbols = set(config.allowed_symbols)
    symbol = candidate.symbol.upper()
    if allowed_symbols and symbol not in allowed_symbols:
        block_reasons.append("symbol_not_allowed")

    blocked_symbols = set(config.blocked_symbols)
    if symbol in blocked_symbols:
        block_reasons.append("symbol_explicitly_blocked")

    if candidate.target_notional_usd is None:
        block_reasons.append("target_notional_missing")
    if (
        config.max_target_notional_usd is not None
        and candidate.target_notional_usd is not None
        and candidate.target_notional_usd > config.max_target_notional_usd
    ):
        block_reasons.append("target_notional_exceeds_limit")

    unique_reasons = sorted(set(block_reasons))
    return ExecutionPolicyDecision(
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        bundle_status_from_preflight=preflight.bundle_status,
        policy_status="allowed" if not unique_reasons else "blocked",
        allowed=not unique_reasons,
        block_reasons=unique_reasons,
        warnings=list(preflight.warnings),
        preview_only=True,
        is_live=False,
    )


def evaluate_execution_policy_decisions(
    *,
    candidates: list[ExecutionCandidate],
    preflight_bundles: list[ExecutionBundlePreflight],
    config: ExecutionPolicyConfigSnapshot,
) -> list[ExecutionPolicyDecision]:
    return [
        evaluate_execution_policy_decision(candidate=candidate, preflight=bundle, config=config)
        for candidate, bundle in zip(candidates, preflight_bundles, strict=False)
    ]
