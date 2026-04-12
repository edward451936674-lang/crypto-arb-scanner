from __future__ import annotations

from abc import ABC, abstractmethod

from app.models.execution import (
    AdapterExecutionResult,
    BalanceSnapshot,
    CancelIntent,
    OrderIntent,
    OrderStatusSnapshot,
    PositionSnapshot,
)


class BaseExecutionAdapter(ABC):
    venue_id: str

    @abstractmethod
    async def place_order(self, intent: OrderIntent) -> AdapterExecutionResult:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, intent: CancelIntent) -> AdapterExecutionResult:
        raise NotImplementedError

    @abstractmethod
    async def get_order_status(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
        symbol: str | None = None,
    ) -> OrderStatusSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def get_position(self, *, symbol: str) -> PositionSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def get_balance(self, *, asset: str) -> BalanceSnapshot:
        raise NotImplementedError
