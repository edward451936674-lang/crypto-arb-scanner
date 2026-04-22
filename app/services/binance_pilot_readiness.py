from __future__ import annotations

from app.core.config import Settings, get_settings
from app.models.execution import ExecutionCandidate
from app.services.binance_pilot import (
    evaluate_arm_token_for_environment,
    evaluate_binance_environment_block_reasons,
    resolve_binance_environment_mode,
    resolve_binance_pilot_symbol_allowlist,
    resolve_binance_trade_base_url,
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


async def build_binance_pilot_readiness_preview(
    *,
    candidates: list[ExecutionCandidate],
    settings: Settings | None = None,
    request_arm_token: str = "",
) -> dict[str, object]:
    resolved_settings = settings or get_settings()
    preflight_bundles = await evaluate_execution_preflight_bundles(candidates)
    policy_config = resolve_execution_policy_config_snapshot(resolved_settings)
    policy_decisions = evaluate_execution_policy_decisions(
        candidates=candidates,
        preflight_bundles=preflight_bundles,
        config=policy_config,
    )
    account_config = resolve_execution_account_state_config_snapshot(resolved_settings)
    account_decisions = evaluate_execution_account_state_decisions(candidates=candidates, config=account_config)
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

    env_mode = resolve_binance_environment_mode(resolved_settings)
    env_block_reasons = evaluate_binance_environment_block_reasons(resolved_settings)
    arm_token_reasons = evaluate_arm_token_for_environment(settings=resolved_settings, request_arm_token=request_arm_token)
    symbol_allowlist = resolve_binance_pilot_symbol_allowlist(resolved_settings)

    route_shape_blocked_count = 0
    symbol_allowlist_blocked_count = 0
    mixed_venue_blocked_count = 0
    unsupported_route_shape_blocked_count = 0
    all_reasons: list[str] = [*env_block_reasons, *arm_token_reasons]

    for candidate in candidates:
        is_mixed = candidate.long_exchange.lower() != candidate.short_exchange.lower()
        is_binance_pair = candidate.long_exchange.lower() == "binance" and candidate.short_exchange.lower() == "binance"
        if is_mixed:
            mixed_venue_blocked_count += 1
            route_shape_blocked_count += 1
            all_reasons.append("mixed_live_venue_path_not_supported_yet")
        elif not is_binance_pair:
            unsupported_route_shape_blocked_count += 1
            route_shape_blocked_count += 1
            all_reasons.append("unsupported_live_submit_path")

        if symbol_allowlist and candidate.symbol.upper() not in set(symbol_allowlist):
            symbol_allowlist_blocked_count += 1
            all_reasons.append("binance_symbol_not_in_pilot_allowlist")

    policy_blocked_count = sum(1 for item in policy_decisions if item.policy_status != "allowed")
    account_state_blocked_count = sum(1 for item in account_decisions if item.account_state_status != "allowed")
    credential_blocked_count = sum(1 for item in credential_decisions if item.credential_readiness_status != "allowed")
    exchange_info_ready_count = sum(1 for item in preflight_bundles if item.bundle_status == "ready")
    live_entry_blocked_count = sum(1 for item in live_entry_results if item.entry_status != "allowed")

    checklist_items = [
        {
            "item": "execution_globally_enabled",
            "status": "pass" if bool(resolved_settings.execution_policy_execution_enabled) else "fail",
            "block_reasons": [] if bool(resolved_settings.execution_policy_execution_enabled) else ["execution_globally_disabled"],
        },
        {
            "item": "live_execution_enabled_if_applicable",
            "status": "pass" if env_mode == "testnet" or bool(resolved_settings.live_execution_enabled) else "fail",
            "block_reasons": [] if env_mode == "testnet" or bool(resolved_settings.live_execution_enabled) else ["binance_live_environment_not_enabled"],
        },
        {
            "item": "environment_mode_resolved",
            "status": "pass" if env_mode in {"testnet", "live"} and not env_block_reasons else "fail",
            "block_reasons": list(env_block_reasons),
            "metadata": {"mode": env_mode, "trade_base_url": resolve_binance_trade_base_url(resolved_settings)},
        },
        {
            "item": "arm_token_ready",
            "status": "pass" if not arm_token_reasons else "fail",
            "block_reasons": list(arm_token_reasons),
        },
        {
            "item": "credential_readiness",
            "status": "pass" if credential_blocked_count == 0 else "fail",
            "block_reasons": [] if credential_blocked_count == 0 else ["credential_readiness_blocked"],
            "metadata": {"blocked_count": credential_blocked_count},
        },
        {
            "item": "account_state_readiness",
            "status": "pass" if account_state_blocked_count == 0 else "fail",
            "block_reasons": [] if account_state_blocked_count == 0 else ["account_state_blocked"],
            "metadata": {"blocked_count": account_state_blocked_count},
        },
        {
            "item": "allowed_venue_symbol_policy",
            "status": "pass" if policy_blocked_count == 0 else "fail",
            "block_reasons": [] if policy_blocked_count == 0 else ["policy_blocked"],
            "metadata": {"blocked_count": policy_blocked_count},
        },
        {
            "item": "exchange_info_rule_readiness",
            "status": "pass" if exchange_info_ready_count == len(candidates) else "fail",
            "block_reasons": [] if exchange_info_ready_count == len(candidates) else ["preflight_blocked"],
            "metadata": {"ready_count": exchange_info_ready_count, "total": len(candidates)},
        },
        {
            "item": "binance_symbol_allowlist_readiness",
            "status": "pass" if symbol_allowlist_blocked_count == 0 else "fail",
            "block_reasons": [] if symbol_allowlist_blocked_count == 0 else ["binance_symbol_not_in_pilot_allowlist"],
            "metadata": {"allowlist": symbol_allowlist, "blocked_count": symbol_allowlist_blocked_count},
        },
        {
            "item": "route_shape_allowed",
            "status": "pass" if route_shape_blocked_count == 0 and live_entry_blocked_count == 0 else "fail",
            "block_reasons": [
                *([] if route_shape_blocked_count == 0 else ["unsupported_live_submit_path"]),
                *([] if mixed_venue_blocked_count == 0 else ["mixed_live_venue_path_not_supported_yet"]),
                *([] if live_entry_blocked_count == 0 else ["live_entry_blocked"]),
            ],
            "metadata": {
                "route_shape_blocked_count": route_shape_blocked_count,
                "mixed_venue_blocked_count": mixed_venue_blocked_count,
                "unsupported_route_shape_blocked_count": unsupported_route_shape_blocked_count,
                "live_entry_blocked_count": live_entry_blocked_count,
            },
        },
    ]

    overall_ready = all(item["status"] == "pass" for item in checklist_items)

    return {
        "status": "ready" if overall_ready else "blocked",
        "ready": overall_ready,
        "candidate_count": len(candidates),
        "environment_mode": env_mode,
        "block_reasons": sorted(set([*all_reasons, *(reason for i in checklist_items for reason in i.get("block_reasons", []))])),
        "checklist_items": checklist_items,
    }
