from __future__ import annotations

from app.core.config import Settings
from app.models.market import Opportunity
from app.services.execution_sizing_policy import ExecutionAccountState

FIXTURE_SCENARIO_REMAINING_CAP_PCT = {
    "roomy": 0.10,
    "tight": 0.04,
    "exhausted": 0.00,
}


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


def resolve_execution_account_state_fixture_scenario(settings: Settings) -> str:
    scenario_name = str(getattr(settings, "execution_account_state_fixture_scenario", "roomy")).strip().lower()
    if scenario_name not in FIXTURE_SCENARIO_REMAINING_CAP_PCT:
        raise ValueError(
            "Unknown execution account state fixture scenario "
            f"'{scenario_name}'. Supported scenarios: {', '.join(FIXTURE_SCENARIO_REMAINING_CAP_PCT.keys())}"
        )
    return scenario_name


def resolve_fixed_fixture_remaining_caps(settings: Settings) -> dict[str, float]:
    scenario_name = resolve_execution_account_state_fixture_scenario(settings)
    scenario_cap_pct = FIXTURE_SCENARIO_REMAINING_CAP_PCT[scenario_name]
    return {
        "remaining_total_cap_pct": (
            settings.execution_account_state_fixture_remaining_total_cap_pct
            if settings.execution_account_state_fixture_remaining_total_cap_pct is not None
            else scenario_cap_pct
        ),
        "remaining_symbol_cap_pct": (
            settings.execution_account_state_fixture_remaining_symbol_cap_pct
            if settings.execution_account_state_fixture_remaining_symbol_cap_pct is not None
            else scenario_cap_pct
        ),
        "remaining_long_exchange_cap_pct": (
            settings.execution_account_state_fixture_remaining_long_exchange_cap_pct
            if settings.execution_account_state_fixture_remaining_long_exchange_cap_pct is not None
            else scenario_cap_pct
        ),
        "remaining_short_exchange_cap_pct": (
            settings.execution_account_state_fixture_remaining_short_exchange_cap_pct
            if settings.execution_account_state_fixture_remaining_short_exchange_cap_pct is not None
            else scenario_cap_pct
        ),
    }


def _build_fixed_fixture_execution_account_state(settings: Settings) -> ExecutionAccountState:
    resolved_caps = resolve_fixed_fixture_remaining_caps(settings)

    return ExecutionAccountState(
        remaining_total_cap_pct=resolved_caps["remaining_total_cap_pct"],
        remaining_symbol_cap_pct_by_symbol={
            "BTC": resolved_caps["remaining_symbol_cap_pct"],
            "ETH": resolved_caps["remaining_symbol_cap_pct"],
            "SOL": resolved_caps["remaining_symbol_cap_pct"],
        },
        remaining_long_exchange_cap_pct_by_exchange={
            "binance": resolved_caps["remaining_long_exchange_cap_pct"],
            "okx": resolved_caps["remaining_long_exchange_cap_pct"],
            "hyperliquid": resolved_caps["remaining_long_exchange_cap_pct"],
            "lighter": resolved_caps["remaining_long_exchange_cap_pct"],
        },
        remaining_short_exchange_cap_pct_by_exchange={
            "binance": resolved_caps["remaining_short_exchange_cap_pct"],
            "okx": resolved_caps["remaining_short_exchange_cap_pct"],
            "hyperliquid": resolved_caps["remaining_short_exchange_cap_pct"],
            "lighter": resolved_caps["remaining_short_exchange_cap_pct"],
        },
    )


def get_execution_account_state_provider(settings: Settings) -> ExecutionAccountStateProvider:
    provider_name = resolve_execution_account_state_provider_name(settings)

    if provider_name in {"", "null"}:
        return NullExecutionAccountStateProvider()

    if provider_name == "fixed_fixture":
        return FixedExecutionAccountStateProvider(_build_fixed_fixture_execution_account_state(settings))

    raise ValueError(
        "Unknown execution account state provider "
        f"'{provider_name}'. Supported providers: null, fixed_fixture"
    )
