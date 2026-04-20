from __future__ import annotations

import time

from app.core.config import Settings, get_settings
from app.models.execution import (
    ExecutionAccountStateDecision,
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionCredentialReadinessDecision,
    ExecutionLegPreflight,
    ExecutionPolicyDecision,
    LiveExecutionEntryResult,
    LiveSubmitAttempt,
    LiveSubmitConfigSnapshot,
    LiveSubmitLegAttempt,
)
from app.services.execution_account_state_gate import (
    evaluate_execution_account_state_decisions,
    resolve_execution_account_state_config_snapshot,
)
from app.services.execution_credential_readiness import (
    evaluate_execution_credential_readiness_decisions,
    resolve_execution_credential_readiness_config_snapshot,
)
from app.services.execution_policy import (
    evaluate_execution_policy_decisions,
    resolve_execution_policy_config_snapshot,
)
from app.services.execution_preflight import evaluate_execution_preflight_bundles
from app.services.live_execution_entry import (
    evaluate_live_execution_entry_decisions,
    resolve_live_execution_entry_config_snapshot,
)

_SUBMIT_SEQUENCE_BY_SIDE: dict[str, tuple[int, int, str]] = {
    "buy": (0, 1, "first"),
    "sell": (1, 2, "second"),
}


def resolve_live_submit_config_snapshot(settings: Settings | None = None) -> LiveSubmitConfigSnapshot:
    resolved_settings = settings or get_settings()
    return LiveSubmitConfigSnapshot(
        guarded_live_submit_enabled=bool(resolved_settings.guarded_live_submit_enabled),
        guarded_live_submit_require_arm_token=bool(resolved_settings.guarded_live_submit_require_arm_token),
        guarded_live_submit_arm_token=str(resolved_settings.guarded_live_submit_arm_token or ""),
        guarded_live_submit_persist_attempts=bool(resolved_settings.guarded_live_submit_persist_attempts),
    )


def _leg_attempt_from_preflight(leg: ExecutionLegPreflight, *, submit_status: str, submit_message: str) -> LiveSubmitLegAttempt:
    leg_index, submit_sequence, submit_order = _SUBMIT_SEQUENCE_BY_SIDE[leg.side]
    return LiveSubmitLegAttempt(
        venue_id=leg.venue_id,
        side=leg.side,
        symbol=leg.symbol,
        route_key=leg.route_key,
        quantity=leg.quantity,
        leg_index=leg_index,
        submit_sequence=submit_sequence,
        submit_order=submit_order,
        supported_venue=leg.supported_venue,
        attempted_live_submit=False,
        submit_status=submit_status,
        submit_message=submit_message,
        accepted=False,
        validation_errors=list(leg.validation_errors),
        validation_warnings=list(leg.validation_warnings),
    )


def _build_attempt(
    *,
    candidate: ExecutionCandidate,
    preflight: ExecutionBundlePreflight,
    policy_decision: ExecutionPolicyDecision,
    account_state_decision: ExecutionAccountStateDecision,
    credential_readiness_decision: ExecutionCredentialReadinessDecision,
    live_entry_result: LiveExecutionEntryResult,
    config: LiveSubmitConfigSnapshot,
    request_arm_token: str,
) -> LiveSubmitAttempt:
    created_at_ms = int(time.time() * 1000)
    block_reasons: list[str] = []
    warnings: list[str] = [
        *preflight.warnings,
        *policy_decision.warnings,
        *account_state_decision.warnings,
        *credential_readiness_decision.warnings,
        *live_entry_result.warnings,
    ]

    if preflight.bundle_status != "ready":
        block_reasons.append("preflight_blocked")
    if policy_decision.policy_status != "allowed":
        block_reasons.append("policy_blocked")
    if account_state_decision.account_state_status != "allowed":
        block_reasons.append("account_state_blocked")
    if credential_readiness_decision.credential_readiness_status != "allowed":
        block_reasons.append("credential_readiness_blocked")
    if live_entry_result.entry_status != "allowed":
        block_reasons.append("live_entry_blocked")

    if not config.guarded_live_submit_enabled:
        block_reasons.append("guarded_live_submit_disabled")

    if config.guarded_live_submit_require_arm_token:
        if not request_arm_token:
            block_reasons.append("arm_token_required")
        elif request_arm_token != config.guarded_live_submit_arm_token:
            block_reasons.append("arm_token_mismatch")

    if not block_reasons:
        block_reasons.append("no_live_adapter_implemented")

    status = "blocked" if block_reasons else "submitted"
    message = "blocked" if block_reasons else "submitted"
    if "no_live_adapter_implemented" in block_reasons:
        message = "no_live_adapter_implemented"

    long_leg = _leg_attempt_from_preflight(preflight.long_leg, submit_status=status, submit_message=message)
    short_leg = _leg_attempt_from_preflight(preflight.short_leg, submit_status=status, submit_message=message)

    return LiveSubmitAttempt(
        attempt_id=f"livesubmit:{candidate.route_key}:{created_at_ms}",
        route_key=candidate.route_key,
        symbol=candidate.symbol,
        long_leg=long_leg,
        short_leg=short_leg,
        live_submit_status="blocked" if block_reasons else "submitted",
        block_reasons=sorted(set(block_reasons)),
        warnings=sorted(set(warnings)),
        submitted_leg_count=0,
        accepted_leg_count=0,
        preview_only=False,
        is_live=False,
        created_at_ms=created_at_ms,
    )


async def run_guarded_live_submit(
    *,
    candidates: list[ExecutionCandidate],
    request_arm_token: str,
    settings: Settings | None = None,
) -> tuple[list[LiveSubmitAttempt], LiveSubmitConfigSnapshot]:
    resolved_settings = settings or get_settings()
    preflight_bundles = await evaluate_execution_preflight_bundles(candidates)
    policy_config = resolve_execution_policy_config_snapshot(resolved_settings)
    policy_decisions = evaluate_execution_policy_decisions(
        candidates=candidates,
        preflight_bundles=preflight_bundles,
        config=policy_config,
    )
    account_state_config = resolve_execution_account_state_config_snapshot(resolved_settings)
    account_state_decisions = evaluate_execution_account_state_decisions(candidates=candidates, config=account_state_config)
    credential_config = resolve_execution_credential_readiness_config_snapshot(resolved_settings)
    credential_decisions = evaluate_execution_credential_readiness_decisions(candidates=candidates, config=credential_config)
    live_entry_config = resolve_live_execution_entry_config_snapshot(resolved_settings)
    live_entry_results = evaluate_live_execution_entry_decisions(
        candidates=candidates,
        preflight_bundles=preflight_bundles,
        policy_decisions=policy_decisions,
        credential_readiness_decisions=credential_decisions,
        config=live_entry_config,
    )
    submit_config = resolve_live_submit_config_snapshot(resolved_settings)

    attempts: list[LiveSubmitAttempt] = []
    for candidate, preflight, policy_decision, account_state_decision, credential_decision, live_entry_result in zip(
        candidates,
        preflight_bundles,
        policy_decisions,
        account_state_decisions,
        credential_decisions,
        live_entry_results,
        strict=False,
    ):
        attempts.append(
            _build_attempt(
                candidate=candidate,
                preflight=preflight,
                policy_decision=policy_decision,
                account_state_decision=account_state_decision,
                credential_readiness_decision=credential_decision,
                live_entry_result=live_entry_result,
                config=submit_config,
                request_arm_token=request_arm_token,
            )
        )

    return attempts, submit_config
