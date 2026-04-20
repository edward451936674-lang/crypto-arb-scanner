from __future__ import annotations

from app.core.config import Settings, get_settings
from app.models.execution import (
    ExecutionCandidate,
    ExecutionCredentialReadinessConfigSnapshot,
    ExecutionCredentialReadinessDecision,
)


def _normalize_fixture(items: dict[str, bool]) -> dict[str, bool]:
    normalized: dict[str, bool] = {}
    for key, value in items.items():
        venue_id = str(key).strip().lower()
        if not venue_id:
            continue
        if isinstance(value, bool):
            normalized[venue_id] = value
    return normalized


def resolve_execution_credential_readiness_config_snapshot(
    settings: Settings | None = None,
) -> ExecutionCredentialReadinessConfigSnapshot:
    resolved_settings = settings or get_settings()
    return ExecutionCredentialReadinessConfigSnapshot(
        execution_credential_readiness_enabled=bool(resolved_settings.execution_credential_readiness_enabled),
        execution_credential_fixture_configured_venues=_normalize_fixture(
            dict(resolved_settings.execution_credential_fixture_configured_venues)
        ),
    )


def evaluate_execution_credential_readiness_decision(
    *,
    candidate: ExecutionCandidate,
    config: ExecutionCredentialReadinessConfigSnapshot,
) -> ExecutionCredentialReadinessDecision:
    block_reasons: list[str] = []

    long_exchange_key = candidate.long_exchange.lower()
    short_exchange_key = candidate.short_exchange.lower()

    long_credentials_configured = config.execution_credential_fixture_configured_venues.get(long_exchange_key)
    short_credentials_configured = config.execution_credential_fixture_configured_venues.get(short_exchange_key)

    if not config.execution_credential_readiness_enabled:
        block_reasons.append("credential_readiness_disabled")

    if long_credentials_configured is None:
        block_reasons.append("long_credentials_status_unknown")
    elif not long_credentials_configured:
        block_reasons.append("long_credentials_missing")

    if short_credentials_configured is None:
        block_reasons.append("short_credentials_status_unknown")
    elif not short_credentials_configured:
        block_reasons.append("short_credentials_missing")

    unique_block_reasons = sorted(set(block_reasons))
    return ExecutionCredentialReadinessDecision(
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        credential_readiness_status="allowed" if not unique_block_reasons else "blocked",
        allowed=not unique_block_reasons,
        block_reasons=unique_block_reasons,
        warnings=[],
        long_credentials_configured=long_credentials_configured,
        short_credentials_configured=short_credentials_configured,
        preview_only=True,
        is_live=False,
    )


def evaluate_execution_credential_readiness_decisions(
    *,
    candidates: list[ExecutionCandidate],
    config: ExecutionCredentialReadinessConfigSnapshot,
) -> list[ExecutionCredentialReadinessDecision]:
    return [
        evaluate_execution_credential_readiness_decision(candidate=candidate, config=config)
        for candidate in candidates
    ]
