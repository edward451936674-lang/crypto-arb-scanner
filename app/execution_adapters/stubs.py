from __future__ import annotations

from typing import Any

from app.execution_adapters.base import BaseExecutionAdapter
from app.models.execution import (
    AdapterExecutionResult,
    BalanceSnapshot,
    CancelIntent,
    OrderIntent,
    OrderStatusSnapshot,
    PositionSnapshot,
    VenueRequestPreview,
    VenueTranslationResult,
)


class _ExecutionAdapterStubBase(BaseExecutionAdapter):
    venue_id: str = "unknown"
    style: str = "stub"

    def _base_result(
        self,
        *,
        operation: str,
        accepted: bool,
        message: str,
        translation: VenueTranslationResult,
    ) -> AdapterExecutionResult:
        return AdapterExecutionResult(
            venue_id=self.venue_id,
            operation=operation,
            accepted=accepted,
            message=message,
            metadata={
                "stub": True,
                "preview_only": True,
                "not_live": True,
                "execution_style": self.style,
            },
            notes="stub_preview_only_non_live",
            translation=translation,
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
            status="unknown",
            metadata={"stub": True, "preview_only": True, "not_live": True},
            notes="order_status_not_implemented_for_stub",
            is_live=False,
        )

    async def get_position(self, *, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(
            venue_id=self.venue_id,
            symbol=symbol,
            metadata={"stub": True, "preview_only": True, "not_live": True},
            notes="position_not_implemented_for_stub",
            is_live=False,
        )

    async def get_balance(self, *, asset: str) -> BalanceSnapshot:
        return BalanceSnapshot(
            venue_id=self.venue_id,
            asset=asset,
            metadata={"stub": True, "preview_only": True, "not_live": True},
            notes="balance_not_implemented_for_stub",
            is_live=False,
        )


class _ClassicExecutionAdapterStub(_ExecutionAdapterStubBase):
    style = "classic_rest_preview"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        raise NotImplementedError

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        raise NotImplementedError

    def _validate_place(self, intent: OrderIntent) -> tuple[list[str], list[str]]:
        errors: list[str] = []
        warnings: list[str] = []
        if not intent.symbol:
            errors.append("symbol_required")
        if intent.order_type is None:
            errors.append("order_type_required")
        if intent.quantity is None:
            errors.append("quantity_required")
        if intent.order_type == "limit" and intent.price is None:
            errors.append("price_required_for_limit")
        if intent.time_in_force is None and intent.order_type == "limit":
            warnings.append("time_in_force_missing_for_limit")
        return errors, warnings

    async def place_order(self, intent: OrderIntent) -> AdapterExecutionResult:
        errors, warnings = self._validate_place(intent)
        preview = VenueRequestPreview(
            venue_id=self.venue_id,
            operation="place_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id,
            payload=self._translate_place_payload(intent),
            validation_errors=errors,
            validation_warnings=warnings,
            metadata={"intent_symbol": intent.symbol, "intent_side": intent.side, **intent.metadata},
            is_live=False,
        )
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="place_order",
            normalized_intent_id=intent.client_order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not errors,
            is_live=False,
        )
        return self._base_result(
            operation="place_order",
            accepted=not errors,
            message="stub_preview_only_no_network_classic",
            translation=translation,
        )

    async def cancel_order(self, intent: CancelIntent) -> AdapterExecutionResult:
        errors: list[str] = []
        if not intent.order_id and not intent.client_order_id:
            errors.append("order_id_or_client_order_id_required")
        preview = VenueRequestPreview(
            venue_id=self.venue_id,
            operation="cancel_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id or intent.order_id,
            payload=self._translate_cancel_payload(intent),
            validation_errors=errors,
            validation_warnings=[],
            metadata={"symbol": intent.symbol, **intent.metadata},
            is_live=False,
        )
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="cancel_order",
            normalized_intent_id=intent.client_order_id or intent.order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not errors,
            is_live=False,
        )
        return self._base_result(
            operation="cancel_order",
            accepted=not errors,
            message="stub_preview_only_no_network_classic",
            translation=translation,
        )


class BinanceExecutionAdapterStub(_ClassicExecutionAdapterStub):
    venue_id = "binance"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": intent.symbol,
            "side": intent.side.upper(),
            "type": intent.order_type.upper() if intent.order_type else None,
            "quantity": intent.quantity,
        }
        if intent.price is not None:
            payload["price"] = intent.price
        if intent.time_in_force is not None:
            payload["timeInForce"] = intent.time_in_force.upper()
        if intent.reduce_only is not None:
            payload["reduceOnly"] = intent.reduce_only
        if intent.client_order_id is not None:
            payload["newClientOrderId"] = intent.client_order_id
        return payload

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        payload: dict[str, Any] = {"symbol": intent.symbol}
        if intent.order_id is not None:
            payload["orderId"] = intent.order_id
        if intent.client_order_id is not None:
            payload["origClientOrderId"] = intent.client_order_id
        return payload


class OkxExecutionAdapterStub(_ClassicExecutionAdapterStub):
    venue_id = "okx"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "instId": intent.symbol,
            "side": intent.side,
            "ordType": intent.order_type,
            "sz": intent.quantity,
        }
        if intent.price is not None:
            payload["px"] = intent.price
        if intent.time_in_force is not None:
            payload["tif"] = intent.time_in_force
        if intent.reduce_only is not None:
            payload["reduceOnly"] = intent.reduce_only
        if intent.client_order_id is not None:
            payload["clOrdId"] = intent.client_order_id
        return payload

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        payload: dict[str, Any] = {"instId": intent.symbol}
        if intent.order_id is not None:
            payload["ordId"] = intent.order_id
        if intent.client_order_id is not None:
            payload["clOrdId"] = intent.client_order_id
        return payload


class _SignedActionExecutionAdapterStub(_ExecutionAdapterStubBase):
    style = "signed_action_preview"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        raise NotImplementedError

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        raise NotImplementedError

    def _validate_place(self, intent: OrderIntent) -> tuple[list[str], list[str]]:
        errors: list[str] = []
        warnings: list[str] = []
        if not intent.symbol:
            errors.append("symbol_required")
        if intent.order_type is None:
            warnings.append("order_type_missing_defaults_not_applied")
        if intent.quantity is None:
            errors.append("quantity_required")
        if intent.order_type == "limit" and intent.price is None:
            errors.append("price_required_for_limit")
        warnings.append("signature_not_implemented")
        warnings.append("payload_not_directly_sendable")
        return errors, warnings

    async def place_order(self, intent: OrderIntent) -> AdapterExecutionResult:
        errors, warnings = self._validate_place(intent)
        preview = VenueRequestPreview(
            venue_id=self.venue_id,
            operation="place_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id,
            payload=self._translate_place_payload(intent),
            validation_errors=errors,
            validation_warnings=warnings,
            metadata={"intent_symbol": intent.symbol, "intent_side": intent.side, **intent.metadata},
            is_live=False,
        )
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="place_order",
            normalized_intent_id=intent.client_order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not errors,
            is_live=False,
        )
        return self._base_result(
            operation="place_order",
            accepted=not errors,
            message="stub_preview_only_no_signing_no_network",
            translation=translation,
        )

    async def cancel_order(self, intent: CancelIntent) -> AdapterExecutionResult:
        errors: list[str] = []
        warnings = ["signature_not_implemented", "payload_not_directly_sendable"]
        if not intent.order_id and not intent.client_order_id:
            errors.append("order_id_or_client_order_id_required")
        preview = VenueRequestPreview(
            venue_id=self.venue_id,
            operation="cancel_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id or intent.order_id,
            payload=self._translate_cancel_payload(intent),
            validation_errors=errors,
            validation_warnings=warnings,
            metadata={"symbol": intent.symbol, **intent.metadata},
            is_live=False,
        )
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="cancel_order",
            normalized_intent_id=intent.client_order_id or intent.order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not errors,
            is_live=False,
        )
        return self._base_result(
            operation="cancel_order",
            accepted=not errors,
            message="stub_preview_only_no_signing_no_network",
            translation=translation,
        )


class HyperliquidExecutionAdapterStub(_SignedActionExecutionAdapterStub):
    venue_id = "hyperliquid"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        return {
            "action": {
                "type": "order",
                "orders": [
                    {
                        "coin": intent.symbol,
                        "isBuy": intent.side == "buy",
                        "sz": intent.quantity,
                        "limitPx": intent.price,
                        "orderType": intent.order_type,
                        "reduceOnly": intent.reduce_only,
                        "tif": intent.time_in_force,
                        "cloid": intent.client_order_id,
                    }
                ],
            },
            "signature": "not_implemented",
            "nonce": "not_implemented",
        }

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        return {
            "action": {
                "type": "cancel",
                "coin": intent.symbol,
                "oid": intent.order_id,
                "cloid": intent.client_order_id,
            },
            "signature": "not_implemented",
            "nonce": "not_implemented",
        }


class LighterExecutionAdapterStub(_SignedActionExecutionAdapterStub):
    venue_id = "lighter"

    def _translate_place_payload(self, intent: OrderIntent) -> dict[str, Any]:
        return {
            "tx": {
                "kind": "place_order",
                "market": intent.symbol,
                "side": intent.side,
                "order": {
                    "type": intent.order_type,
                    "quantity": intent.quantity,
                    "price": intent.price,
                    "timeInForce": intent.time_in_force,
                    "reduceOnly": intent.reduce_only,
                },
                "clientOrderId": intent.client_order_id,
            },
            "auth": {
                "signature": "not_implemented",
                "nonce": "not_implemented",
                "api_key": "not_required_for_preview",
            },
        }

    def _translate_cancel_payload(self, intent: CancelIntent) -> dict[str, Any]:
        return {
            "tx": {
                "kind": "cancel_order",
                "market": intent.symbol,
                "orderId": intent.order_id,
                "clientOrderId": intent.client_order_id,
            },
            "auth": {
                "signature": "not_implemented",
                "nonce": "not_implemented",
                "api_key": "not_required_for_preview",
            },
        }
