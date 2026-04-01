from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.services.execution_sizing_policy import build_execution_account_inputs, resolve_execution_policy_profile


def test_build_execution_account_inputs_uses_settings_and_opportunity_caps() -> None:
    settings = Settings(
        execution_extended_size_up_enabled=False,
        execution_live_target_leverage=1.3,
        execution_live_max_allowed_leverage=1.8,
        execution_live_required_liquidation_buffer_pct=33.0,
        execution_live_remaining_total_cap_pct=0.4,
        execution_live_remaining_symbol_cap_pct=0.3,
        execution_live_remaining_long_exchange_cap_pct=0.2,
        execution_live_remaining_short_exchange_cap_pct=0.1,
    )
    opportunity = SimpleNamespace(
        remaining_total_cap_pct=0.11,
        remaining_symbol_cap_pct=0.12,
        remaining_long_exchange_cap_pct=0.13,
        remaining_short_exchange_cap_pct=0.14,
    )

    inputs = build_execution_account_inputs(settings, opportunity)

    assert inputs.extended_size_up_enabled is False
    assert inputs.live_target_leverage == 1.3
    assert inputs.live_max_allowed_leverage == 1.8
    assert inputs.live_required_liquidation_buffer_pct == 33.0
    assert inputs.live_remaining_total_cap_pct == 0.11
    assert inputs.live_remaining_symbol_cap_pct == 0.12
    assert inputs.live_remaining_long_exchange_cap_pct == 0.13
    assert inputs.live_remaining_short_exchange_cap_pct == 0.14


def test_build_execution_account_inputs_falls_back_to_settings_caps_for_zero_values() -> None:
    settings = Settings(
        execution_live_remaining_total_cap_pct=0.21,
        execution_live_remaining_symbol_cap_pct=0.22,
        execution_live_remaining_long_exchange_cap_pct=0.23,
        execution_live_remaining_short_exchange_cap_pct=0.24,
    )
    opportunity = SimpleNamespace(
        remaining_total_cap_pct=0.0,
        remaining_symbol_cap_pct=0.0,
        remaining_long_exchange_cap_pct=0.0,
        remaining_short_exchange_cap_pct=0.0,
    )

    inputs = build_execution_account_inputs(settings, opportunity)

    assert inputs.live_remaining_total_cap_pct == 0.21
    assert inputs.live_remaining_symbol_cap_pct == 0.22
    assert inputs.live_remaining_long_exchange_cap_pct == 0.23
    assert inputs.live_remaining_short_exchange_cap_pct == 0.24


def test_resolve_execution_policy_profile_paper_conservative() -> None:
    profile = resolve_execution_policy_profile(Settings(execution_policy_profile="paper_conservative"))

    assert profile.extended_size_up_enabled is False
    assert profile.live_target_leverage == 1.0
    assert profile.live_max_allowed_leverage == 1.5
    assert profile.live_required_liquidation_buffer_pct == 30.0
    assert profile.live_remaining_total_cap_pct == 0.05
    assert profile.live_remaining_symbol_cap_pct == 0.05
    assert profile.live_remaining_long_exchange_cap_pct == 0.05
    assert profile.live_remaining_short_exchange_cap_pct == 0.05


def test_resolve_execution_policy_profile_live_conservative() -> None:
    profile = resolve_execution_policy_profile(Settings(execution_policy_profile="live_conservative"))

    assert profile.extended_size_up_enabled is False
    assert profile.live_target_leverage == 1.0
    assert profile.live_max_allowed_leverage == 1.5
    assert profile.live_required_liquidation_buffer_pct == 35.0
    assert profile.live_remaining_total_cap_pct == 0.05
    assert profile.live_remaining_symbol_cap_pct == 0.03
    assert profile.live_remaining_long_exchange_cap_pct == 0.05
    assert profile.live_remaining_short_exchange_cap_pct == 0.05


def test_resolve_execution_policy_profile_dev_default_uses_flat_settings_values() -> None:
    settings = Settings(
        execution_policy_profile="dev_default",
        execution_extended_size_up_enabled=False,
        execution_live_target_leverage=1.3,
        execution_live_max_allowed_leverage=1.7,
        execution_live_required_liquidation_buffer_pct=34.0,
        execution_live_remaining_total_cap_pct=0.2,
        execution_live_remaining_symbol_cap_pct=0.19,
        execution_live_remaining_long_exchange_cap_pct=0.18,
        execution_live_remaining_short_exchange_cap_pct=0.17,
    )

    profile = resolve_execution_policy_profile(settings)

    assert profile.extended_size_up_enabled is False
    assert profile.live_target_leverage == 1.3
    assert profile.live_max_allowed_leverage == 1.7
    assert profile.live_required_liquidation_buffer_pct == 34.0
    assert profile.live_remaining_total_cap_pct == 0.2
    assert profile.live_remaining_symbol_cap_pct == 0.19
    assert profile.live_remaining_long_exchange_cap_pct == 0.18
    assert profile.live_remaining_short_exchange_cap_pct == 0.17


def test_build_execution_account_inputs_uses_positive_opportunity_caps_over_profile_defaults() -> None:
    settings = Settings(execution_policy_profile="paper_conservative")
    opportunity = SimpleNamespace(
        remaining_total_cap_pct=0.11,
        remaining_symbol_cap_pct=0.12,
        remaining_long_exchange_cap_pct=0.13,
        remaining_short_exchange_cap_pct=0.14,
    )

    inputs = build_execution_account_inputs(settings, opportunity)

    assert inputs.live_remaining_total_cap_pct == 0.11
    assert inputs.live_remaining_symbol_cap_pct == 0.12
    assert inputs.live_remaining_long_exchange_cap_pct == 0.13
    assert inputs.live_remaining_short_exchange_cap_pct == 0.14


def test_resolve_execution_policy_profile_unknown_profile_raises() -> None:
    with pytest.raises(ValueError, match="Unknown execution policy profile: unknown_profile"):
        resolve_execution_policy_profile(Settings(execution_policy_profile="unknown_profile"))
