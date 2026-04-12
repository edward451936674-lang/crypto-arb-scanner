from __future__ import annotations

import hashlib

from app.execution_adapters.base import BaseExecutionAdapter
from app.models.execution import (
    AdapterExecutionResult,
    BalanceSnapshot,
    CancelIntent,
    OrderIntent,
    OrderStatusSnapshot,
    PositionSnapshot,
)


class PaperExecutionAdapter(BaseExecutionAdapter):
    venue_id = "paper"

    def _deterministic_order_id(self, *, intent: OrderIntent) -> str:
        quantity_key = "na" if intent.quantity is None else f"{intent.quantity:.8f}"
        payload = (
            f"{intent.venue_id}|{intent.symbol}|{intent.side}|{quantity_key}|"
            f"{intent.price if intent.price is not None else 'mkt'}|{intent.client_order_id or ''}"
        )
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
        return f"paper-{digest}"

    async def place_order(self, intent: OrderIntent) -> AdapterExecutionResult:
        order_id = self._deterministic_order_id(intent=intent)
        return AdapterExecutionResult(
            venue_id=self.venue_id,
            operation="place_order",
            accepted=True,
            order_status=OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=intent.client_order_id,
                symbol=intent.symbol,
                side=intent.side,
                order_type=intent.order_type,
                status="accepted",
                quantity=intent.quantity,
                filled_qty=0.0,
                remaining_qty=intent.quantity,
                average_fill_price=None,
                reduce_only=intent.reduce_only,
                time_in_force=intent.time_in_force,
                route_key=intent.route_key,
                metadata={"paper": True},
                is_live=False,
            ),
            is_live=False,
        )

    async def cancel_order(self, intent: CancelIntent) -> AdapterExecutionResult:
        return AdapterExecutionResult(
            venue_id=self.venue_id,
            operation="cancel_order",
            accepted=True,
            order_status=OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=intent.order_id,
                client_order_id=intent.client_order_id,
                symbol=intent.symbol,
                status="cancelled",
                route_key=intent.route_key,
                metadata={"paper": True},
                is_live=False,
            ),
            is_live=False,
        )

    async def get_order_status(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
        symbol: str | None = None,
    ) -> OrderStatusSnapshot:
        return OrderStatusSnapshot(
            venue_id=self.venue_id,
            order_id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            status="filled",
            filled_qty=1.0,
            remaining_qty=0.0,
            metadata={"paper": True},
            is_live=False,
        )

    async def get_position(self, *, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(
            venue_id=self.venue_id,
            symbol=symbol,
            size=0.0,
            entry_price=None,
            metadata={"paper": True},
            is_live=False,
        )

    async def get_balance(self, *, asset: str) -> BalanceSnapshot:
        return BalanceSnapshot(
            venue_id=self.venue_id,
            asset=asset,
            free=100000.0,
            locked=0.0,
            metadata={"paper": True},
            is_live=False,
        )
