from app.models.market import Opportunity
from app.services.execution_sizing_policy import ExecutionAccountState


class ExecutionAccountStateProvider:
    def get_account_state(self, opportunity: Opportunity) -> ExecutionAccountState | None:
        raise NotImplementedError


class NullExecutionAccountStateProvider(ExecutionAccountStateProvider):
    def get_account_state(self, opportunity: Opportunity) -> ExecutionAccountState | None:
        return None
