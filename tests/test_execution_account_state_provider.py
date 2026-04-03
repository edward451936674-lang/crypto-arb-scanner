import pytest

from app.core.config import Settings
from app.services.execution_account_state import (
    FixedExecutionAccountStateProvider,
    NullExecutionAccountStateProvider,
    get_execution_account_state_provider,
)
from app.services.execution_sizing_policy import ExecutionAccountState


def test_fixed_provider_returns_same_state_for_any_opportunity() -> None:
    fixed_state = ExecutionAccountState(remaining_total_cap_pct=0.03)
    provider = FixedExecutionAccountStateProvider(fixed_state)

    assert provider.get_account_state(object()) is fixed_state
    assert provider.get_account_state(object()) is fixed_state


def test_resolver_returns_null_provider_by_default() -> None:
    provider = get_execution_account_state_provider(Settings())

    assert isinstance(provider, NullExecutionAccountStateProvider)


def test_resolver_returns_fixed_fixture_provider_when_configured() -> None:
    provider = get_execution_account_state_provider(
        Settings(
            execution_account_state_provider="fixed_fixture",
            execution_account_state_fixture_remaining_total_cap_pct=0.02,
            execution_account_state_fixture_remaining_symbol_cap_pct=0.03,
            execution_account_state_fixture_remaining_long_exchange_cap_pct=0.04,
            execution_account_state_fixture_remaining_short_exchange_cap_pct=0.05,
        )
    )

    assert isinstance(provider, FixedExecutionAccountStateProvider)
    state = provider.get_account_state(object())
    assert state is not None
    assert state.remaining_total_cap_pct == 0.02
    assert state.remaining_symbol_cap_pct_by_symbol["BTC"] == 0.03
    assert state.remaining_long_exchange_cap_pct_by_exchange["binance"] == 0.04
    assert state.remaining_short_exchange_cap_pct_by_exchange["okx"] == 0.05


def test_fixed_fixture_roomy_scenario_resolves_expected_state() -> None:
    provider = get_execution_account_state_provider(
        Settings(
            execution_account_state_provider="fixed_fixture",
            execution_account_state_fixture_scenario="roomy",
        )
    )

    assert isinstance(provider, FixedExecutionAccountStateProvider)
    state = provider.get_account_state(object())
    assert state is not None
    assert state.remaining_total_cap_pct == 0.10
    assert state.remaining_symbol_cap_pct_by_symbol["BTC"] == 0.10
    assert state.remaining_long_exchange_cap_pct_by_exchange["binance"] == 0.10
    assert state.remaining_short_exchange_cap_pct_by_exchange["okx"] == 0.10


def test_fixed_fixture_tight_scenario_reduces_execution_cap() -> None:
    provider = get_execution_account_state_provider(
        Settings(
            execution_account_state_provider="fixed_fixture",
            execution_account_state_fixture_scenario="tight",
        )
    )

    state = provider.get_account_state(object())
    assert state is not None
    assert state.remaining_total_cap_pct == 0.04


def test_fixed_fixture_exhausted_scenario_sets_zero_execution_cap() -> None:
    provider = get_execution_account_state_provider(
        Settings(
            execution_account_state_provider="fixed_fixture",
            execution_account_state_fixture_scenario="exhausted",
        )
    )

    state = provider.get_account_state(object())
    assert state is not None
    assert state.remaining_total_cap_pct == 0.0
    assert state.remaining_symbol_cap_pct_by_symbol["ETH"] == 0.0


def test_fixed_fixture_raw_values_override_named_scenario() -> None:
    provider = get_execution_account_state_provider(
        Settings(
            execution_account_state_provider="fixed_fixture",
            execution_account_state_fixture_scenario="exhausted",
            execution_account_state_fixture_remaining_total_cap_pct=0.03,
            execution_account_state_fixture_remaining_symbol_cap_pct=0.02,
            execution_account_state_fixture_remaining_long_exchange_cap_pct=0.01,
            execution_account_state_fixture_remaining_short_exchange_cap_pct=0.04,
        )
    )

    state = provider.get_account_state(object())
    assert state is not None
    assert state.remaining_total_cap_pct == 0.03
    assert state.remaining_symbol_cap_pct_by_symbol["SOL"] == 0.02
    assert state.remaining_long_exchange_cap_pct_by_exchange["hyperliquid"] == 0.01
    assert state.remaining_short_exchange_cap_pct_by_exchange["lighter"] == 0.04


def test_resolver_raises_clear_error_for_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown execution account state provider"):
        get_execution_account_state_provider(Settings(execution_account_state_provider="mystery_mode"))
