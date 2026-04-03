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
