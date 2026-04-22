from __future__ import annotations

from app.core.config import Settings, get_settings

BINANCE_ENVIRONMENTS = {"testnet", "live"}


def normalize_symbols(items: list[str]) -> list[str]:
    return sorted({str(item).strip().upper() for item in items if str(item).strip()})


def resolve_binance_environment_mode(settings: Settings | None = None) -> str:
    resolved_settings = settings or get_settings()
    mode = str(getattr(resolved_settings, "binance_execution_environment", "testnet") or "testnet").strip().lower()
    return mode if mode in BINANCE_ENVIRONMENTS else "invalid"


def resolve_binance_live_enabled_for_environment(settings: Settings | None = None) -> bool:
    resolved_settings = settings or get_settings()
    mode = resolve_binance_environment_mode(resolved_settings)
    if mode == "testnet":
        return True
    if mode == "live":
        return bool(resolved_settings.live_execution_enabled)
    return False


def resolve_binance_trade_base_url(settings: Settings | None = None) -> str:
    resolved_settings = settings or get_settings()
    mode = resolve_binance_environment_mode(resolved_settings)
    if mode == "testnet":
        return str(getattr(resolved_settings, "binance_testnet_base_url", "") or "https://testnet.binancefuture.com").rstrip("/")
    return str(getattr(resolved_settings, "binance_live_base_url", "") or resolved_settings.binance_base_url).rstrip("/")


def evaluate_binance_environment_block_reasons(settings: Settings | None = None) -> list[str]:
    resolved_settings = settings or get_settings()
    reasons: list[str] = []
    mode = resolve_binance_environment_mode(resolved_settings)
    if mode == "invalid":
        reasons.append("binance_environment_mode_invalid")
        return reasons
    if mode == "live" and not bool(resolved_settings.live_execution_enabled):
        reasons.append("binance_live_environment_not_enabled")

    resolved_url = resolve_binance_trade_base_url(resolved_settings)
    live_url = str(getattr(resolved_settings, "binance_live_base_url", "") or resolved_settings.binance_base_url).rstrip("/")
    if mode == "testnet" and resolved_url == live_url:
        reasons.append("binance_testnet_resolved_to_live_endpoint")
    return sorted(set(reasons))


def resolve_binance_pilot_symbol_allowlist(settings: Settings | None = None) -> list[str]:
    resolved_settings = settings or get_settings()
    return normalize_symbols(list(getattr(resolved_settings, "binance_pilot_allowed_symbols", [])))


def evaluate_arm_token_for_environment(*, settings: Settings | None = None, request_arm_token: str) -> list[str]:
    resolved_settings = settings or get_settings()
    mode = resolve_binance_environment_mode(resolved_settings)
    provided = str(request_arm_token or "")
    reasons: list[str] = []
    if not bool(resolved_settings.guarded_live_submit_require_arm_token):
        return reasons
    if not provided:
        return ["arm_token_required"]

    fallback = str(getattr(resolved_settings, "guarded_live_submit_arm_token", "") or "")
    expected_testnet = str(getattr(resolved_settings, "guarded_live_submit_arm_token_testnet", "") or fallback)
    expected_live = str(getattr(resolved_settings, "guarded_live_submit_arm_token_live", "") or fallback)
    expected = expected_testnet if mode == "testnet" else expected_live

    if expected and provided == expected:
        return reasons

    other_expected = expected_live if mode == "testnet" else expected_testnet
    if other_expected and provided == other_expected:
        reasons.append("arm_token_environment_mismatch")
    else:
        reasons.append("arm_token_mismatch")
    return reasons
