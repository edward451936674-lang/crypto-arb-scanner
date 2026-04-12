from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseExecutionAdapter(ABC):
    venue_id: str

    @abstractmethod
    async def place_order(self, *, symbol: str, side: str, quantity: float, price: float | None = None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, *, order_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_order_status(self, *, order_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_position(self, *, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_balance(self, *, asset: str) -> dict[str, Any]:
        raise NotImplementedError
