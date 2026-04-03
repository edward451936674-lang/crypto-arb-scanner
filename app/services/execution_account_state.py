from __future__ import annotations

from app.core.config import Settings
from app.models.market import Opportunity
from app.services.execution_sizing_policy import ExecutionAccountState


class ExecutionAccountStateProvider:
    def get_account_state(self, opportunity: Opportunity) -> ExecutionAccountState | None:
        raise NotImplementedError


class NullExecutionAccountStateProvider(ExecutionAccountStateProvider):
    def get_account_state(self, opportunity: Opportunity) -> ExecutionAccountState | None:
        return None


class FixedExecutionAccountStateProvider(ExecutionAccountStateProvider):
    def __init__(self, account_state: ExecutionAccountState) -> None:
        self._account_state = account_state

    def get_account_state(self, opportunity: Opportunity) -> ExecutionAccountState | None:
        return self._account_state


def resolve_execution_account_state_provider_name(settings: Settings) -> str:
    return str(getattr(settings, "execution_account_state_provider", "null")).strip().lower()


def get_execution_account_state_provider(settings: Settings) -> ExecutionAccountStateProvider:
    provider_name = resolve_execution_account_state_provider_name(settings)

    if provider_name in {"", "null"}:
        return NullExecutionAccountStateProvider()

    if provider_name == "fixed_fixture":
        fixture_state = ExecutionAccountState(
            remaining_total_cap_pct=settings.execution_account_state_fixture_remaining_total_cap_pct,
            remaining_symbol_cap_pct_by_symbol={
                "BTC": settings.execution_account_state_fixture_remaining_symbol_cap_pct,
                "ETH": settings.execution_account_state_fixture_remaining_symbol_cap_pct,
                "SOL": settings.execution_account_state_fixture_remaining_symbol_cap_pct,
            },
            remaining_long_exchange_cap_pct_by_exchange={
                "binance": settings.execution_account_state_fixture_remaining_long_exchange_cap_pct,
                "okx": settings.execution_account_state_fixture_remaining_long_exchange_cap_pct,
                "hyperliquid": settings.execution_account_state_fixture_remaining_long_exchange_cap_pct,
                "lighter": settings.execution_account_state_fixture_remaining_long_exchange_cap_pct,
            },
            remaining_short_exchange_cap_pct_by_exchange={
                "binance": settings.execution_account_state_fixture_remaining_short_exchange_cap_pct,
                "okx": settings.execution_account_state_fixture_remaining_short_exchange_cap_pct,
                "hyperliquid": settings.execution_account_state_fixture_remaining_short_exchange_cap_pct,
                "lighter": settings.execution_account_state_fixture_remaining_short_exchange_cap_pct,
            },
        )
        return FixedExecutionAccountStateProvider(fixture_state)

    raise ValueError(
        "Unknown execution account state provider "
        f"'{provider_name}'. Supported providers: null, fixed_fixture"
    )
