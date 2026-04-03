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


def get_execution_account_state_provider(settings: Settings) -> ExecutionAccountStateProvider:
    provider_name = str(getattr(settings, "execution_account_state_provider", "null")).strip().lower()
    if provider_name == "null":
        return NullExecutionAccountStateProvider()
    return NullExecutionAccountStateProvider()
