from __future__ import annotations

import hashlib
from typing import Any

from app.execution_adapters.base import BaseExecutionAdapter


class PaperExecutionAdapter(BaseExecutionAdapter):
    venue_id = "paper"

    def _deterministic_order_id(self, *, symbol: str, side: str, quantity: float, price: float | None) -> str:
        payload = f"{symbol}|{side}|{quantity:.8f}|{price if price is not None else 'mkt'}"
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
        return f"paper-{digest}"

    async def place_order(self, *, symbol: str, side: str, quantity: float, price: float | None = None) -> dict[str, Any]:
        order_id = self._deterministic_order_id(symbol=symbol, side=side, quantity=quantity, price=price)
        return {
            "order_id": order_id,
            "venue": self.venue_id,
            "status": "accepted",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "is_live": False,
        }

    async def cancel_order(self, *, order_id: str) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "venue": self.venue_id,
            "status": "cancelled",
            "is_live": False,
        }

    async def get_order_status(self, *, order_id: str) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "venue": self.venue_id,
            "status": "filled",
            "filled_qty": 1.0,
            "remaining_qty": 0.0,
            "is_live": False,
        }

    async def get_position(self, *, symbol: str) -> dict[str, Any]:
        return {
            "venue": self.venue_id,
            "symbol": symbol,
            "size": 0.0,
            "entry_price": None,
            "is_live": False,
        }

    async def get_balance(self, *, asset: str) -> dict[str, Any]:
        return {
            "venue": self.venue_id,
            "asset": asset,
            "free": 100000.0,
            "locked": 0.0,
            "is_live": False,
        }
