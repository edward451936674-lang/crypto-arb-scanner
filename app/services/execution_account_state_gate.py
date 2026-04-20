from __future__ import annotations

from app.core.config import Settings, get_settings
from app.models.execution import (
    ExecutionAccountStateDecision,
    ExecutionAccountStateSnapshot,
    ExecutionCandidate,
)


def _normalize_symbol_caps(items: dict[str, float]) -> dict[str, float]:
    return {str(key).strip().upper(): float(value) for key, value in items.items() if str(key).strip()}


def _normalize_exchange_caps(items: dict[str, float]) -> dict[str, float]:
    return {str(key).strip().lower(): float(value) for key, value in items.items() if str(key).strip()}


def resolve_execution_account_state_config_snapshot(settings: Settings | None = None) -> ExecutionAccountStateSnapshot:
    resolved_settings = settings or get_settings()
    return ExecutionAccountStateSnapshot(
        execution_account_state_enabled=bool(resolved_settings.execution_account_state_enabled),
        execution_account_state_fixture_total_notional_usd=resolved_settings.execution_account_state_fixture_total_notional_usd,
        execution_account_state_fixture_remaining_total_notional_usd=(
            resolved_settings.execution_account_state_fixture_remaining_total_notional_usd
        ),
        execution_account_state_fixture_remaining_symbol_notional_usd=_normalize_symbol_caps(
            dict(resolved_settings.execution_account_state_fixture_remaining_symbol_notional_usd)
        ),
        execution_account_state_fixture_remaining_long_exchange_notional_usd=_normalize_exchange_caps(
            dict(resolved_settings.execution_account_state_fixture_remaining_long_exchange_notional_usd)
        ),
        execution_account_state_fixture_remaining_short_exchange_notional_usd=_normalize_exchange_caps(
            dict(resolved_settings.execution_account_state_fixture_remaining_short_exchange_notional_usd)
        ),
    )


def evaluate_execution_account_state_decision(
    *,
    candidate: ExecutionCandidate,
    config: ExecutionAccountStateSnapshot,
) -> ExecutionAccountStateDecision:
    block_reasons: list[str] = []

    symbol_key = candidate.symbol.upper()
    long_exchange_key = candidate.long_exchange.lower()
    short_exchange_key = candidate.short_exchange.lower()

    remaining_global_notional_usd = config.execution_account_state_fixture_remaining_total_notional_usd
    remaining_symbol_notional_usd = config.execution_account_state_fixture_remaining_symbol_notional_usd.get(symbol_key)
    remaining_long_exchange_notional_usd = config.execution_account_state_fixture_remaining_long_exchange_notional_usd.get(
        long_exchange_key
    )
    remaining_short_exchange_notional_usd = config.execution_account_state_fixture_remaining_short_exchange_notional_usd.get(
        short_exchange_key
    )

    if not config.execution_account_state_enabled:
        block_reasons.append("execution_account_state_disabled")

    if candidate.target_notional_usd is None:
        block_reasons.append("target_notional_missing")

    if remaining_global_notional_usd is None:
        block_reasons.append("global_capacity_missing")
    if remaining_symbol_notional_usd is None:
        block_reasons.append("symbol_capacity_missing")
    if remaining_long_exchange_notional_usd is None:
        block_reasons.append("long_exchange_capacity_missing")
    if remaining_short_exchange_notional_usd is None:
        block_reasons.append("short_exchange_capacity_missing")

    if candidate.target_notional_usd is not None:
        if (
            remaining_global_notional_usd is not None
            and candidate.target_notional_usd > remaining_global_notional_usd
        ):
            block_reasons.append("target_notional_exceeds_global_capacity")
        if (
            remaining_symbol_notional_usd is not None
            and candidate.target_notional_usd > remaining_symbol_notional_usd
        ):
            block_reasons.append("target_notional_exceeds_symbol_capacity")
        if (
            remaining_long_exchange_notional_usd is not None
            and candidate.target_notional_usd > remaining_long_exchange_notional_usd
        ):
            block_reasons.append("target_notional_exceeds_long_exchange_capacity")
        if (
            remaining_short_exchange_notional_usd is not None
            and candidate.target_notional_usd > remaining_short_exchange_notional_usd
        ):
            block_reasons.append("target_notional_exceeds_short_exchange_capacity")

    unique_block_reasons = sorted(set(block_reasons))
    return ExecutionAccountStateDecision(
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        target_notional_usd=candidate.target_notional_usd,
        account_state_status="allowed" if not unique_block_reasons else "blocked",
        allowed=not unique_block_reasons,
        block_reasons=unique_block_reasons,
        warnings=[],
        remaining_global_notional_usd=remaining_global_notional_usd,
        remaining_symbol_notional_usd=remaining_symbol_notional_usd,
        remaining_long_exchange_notional_usd=remaining_long_exchange_notional_usd,
        remaining_short_exchange_notional_usd=remaining_short_exchange_notional_usd,
        preview_only=True,
        is_live=False,
    )


def evaluate_execution_account_state_decisions(
    *,
    candidates: list[ExecutionCandidate],
    config: ExecutionAccountStateSnapshot,
) -> list[ExecutionAccountStateDecision]:
    return [
        evaluate_execution_account_state_decision(candidate=candidate, config=config)
        for candidate in candidates
    ]
