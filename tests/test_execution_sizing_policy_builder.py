from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.services.execution_sizing_policy import (
    build_execution_account_inputs,
    resolve_execution_policy_profile,
)


@pytest.mark.parametrize(
    ("profile_name", "expected"),
    [
        (
            "dev_default",
            {
                "extended_size_up_enabled": True,
                "live_target_leverage": 1.5,
                "live_max_allowed_leverage": 2.0,
                "live_required_liquidation_buffer_pct": 28.0,
                "live_remaining_total_cap_pct": 0.08,
                "live_remaining_symbol_cap_pct": 0.08,
                "live_remaining_long_exchange_cap_pct": 0.08,
                "live_remaining_short_exchange_cap_pct": 0.08,
            },
        ),
        (
            "paper_conservative",
            {
                "extended_size_up_enabled": False,
                "live_target_leverage": 1.0,
                "live_max_allowed_leverage": 1.5,
                "live_required_liquidation_buffer_pct": 30.0,
                "live_remaining_total_cap_pct": 0.05,
                "live_remaining_symbol_cap_pct": 0.05,
                "live_remaining_long_exchange_cap_pct": 0.05,
                "live_remaining_short_exchange_cap_pct": 0.05,
            },
        ),
        (
            "live_conservative",
            {
                "extended_size_up_enabled": False,
                "live_target_leverage": 1.0,
                "live_max_allowed_leverage": 1.5,
                "live_required_liquidation_buffer_pct": 35.0,
                "live_remaining_total_cap_pct": 0.05,
                "live_remaining_symbol_cap_pct": 0.03,
                "live_remaining_long_exchange_cap_pct": 0.05,
                "live_remaining_short_exchange_cap_pct": 0.05,
            },
        ),
    ],
)
def test_resolve_execution_policy_profile_named_defaults(profile_name: str, expected: dict[str, float]) -> None:
    settings = Settings(execution_policy_profile=profile_name)

    profile = resolve_execution_policy_profile(settings)

    for field_name, expected_value in expected.items():
        assert getattr(profile, field_name) == expected_value


def test_build_execution_account_inputs_uses_profile_defaults() -> None:
    settings = Settings(execution_policy_profile="paper_conservative")
    opportunity = SimpleNamespace(
        remaining_total_cap_pct=0.0,
        remaining_symbol_cap_pct=0.0,
        remaining_long_exchange_cap_pct=0.0,
        remaining_short_exchange_cap_pct=0.0,
    )

    inputs = build_execution_account_inputs(settings, opportunity)

    assert inputs.extended_size_up_enabled is False
    assert inputs.live_target_leverage == 1.0
    assert inputs.live_max_allowed_leverage == 1.5
    assert inputs.live_required_liquidation_buffer_pct == 30.0
    assert inputs.live_remaining_total_cap_pct == 0.05
    assert inputs.live_remaining_symbol_cap_pct == 0.05
    assert inputs.live_remaining_long_exchange_cap_pct == 0.05
    assert inputs.live_remaining_short_exchange_cap_pct == 0.05


def test_build_execution_account_inputs_positive_caps_override_profile_defaults() -> None:
    settings = Settings(execution_policy_profile="live_conservative")
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


def test_unknown_execution_policy_profile_raises_clear_error() -> None:
    settings = Settings(execution_policy_profile="not_a_profile")

    with pytest.raises(ValueError, match="Unknown execution_policy_profile='not_a_profile'"):
        resolve_execution_policy_profile(settings)
