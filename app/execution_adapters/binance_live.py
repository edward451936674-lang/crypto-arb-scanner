from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Protocol
from urllib.parse import urlencode

import httpx

from app.core.config import Settings, get_settings
from app.execution_adapters.binance_rules import (
    BinanceSymbolTradingRules,
    parse_binance_exchange_info_symbol_rules,
    validate_and_normalize_order_intent,
)
from app.execution_adapters.base import BaseExecutionAdapter
from app.models.execution import (
    AdapterExecutionResult,
    BalanceSnapshot,
    CancelIntent,
    CredentialReadinessSignal,
    OrderIntent,
    OrderStatusSnapshot,
    PositionSnapshot,
    VenueRequestPreview,
    VenueTranslationResult,
)
from app.services.binance_pilot import resolve_binance_environment_mode, resolve_binance_trade_base_url


class BinanceTransport(Protocol):
    async def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any],
    ) -> dict[str, Any]: ...


class BinanceRuleLoader(Protocol):
    async def load_exchange_info(self) -> dict[str, Any] | None: ...


class HttpxBinanceTransport:
    async def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any],
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.request(method=method, url=url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {"raw": payload}


class ExchangeInfoBinanceRuleLoader:
    def __init__(self, *, base_url: str, transport: BinanceTransport) -> None:
        self.base_url = base_url.rstrip("/")
        self.transport = transport

    async def load_exchange_info(self) -> dict[str, Any] | None:
        payload = await self.transport.request(
            method="GET",
            url=f"{self.base_url}/fapi/v1/exchangeInfo",
            headers={},
            params={},
        )
        return payload


@dataclass(frozen=True)
class BinanceCredentials:
    api_key: str
    api_secret: str


def _env_or_empty(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def load_binance_credentials(settings: Settings | None = None) -> tuple[BinanceCredentials | None, CredentialReadinessSignal]:
    resolved_settings = settings or get_settings()
    api_key = str(getattr(resolved_settings, "binance_api_key", "") or _env_or_empty("ARB_BINANCE_API_KEY")).strip()
    api_secret = str(getattr(resolved_settings, "binance_api_secret", "") or _env_or_empty("ARB_BINANCE_API_SECRET")).strip()

    reasons: list[str] = []
    if not api_key:
        reasons.append("api_key_missing")
    if not api_secret:
        reasons.append("api_secret_missing")

    if reasons:
        return (
            None,
            CredentialReadinessSignal(
                venue_id="binance",
                credential_type="binance_api_key_secret",
                status="missing",
                reasons=reasons,
                metadata={"source": "settings_or_env"},
            ),
        )

    if len(api_key) < 8:
        reasons.append("api_key_too_short")
    if len(api_secret) < 16:
        reasons.append("api_secret_too_short")

    if reasons:
        return (
            None,
            CredentialReadinessSignal(
                venue_id="binance",
                credential_type="binance_api_key_secret",
                status="malformed",
                reasons=reasons,
                metadata={"source": "settings_or_env"},
            ),
        )

    return (
        BinanceCredentials(api_key=api_key, api_secret=api_secret),
        CredentialReadinessSignal(
            venue_id="binance",
            credential_type="binance_api_key_secret",
            status="present",
            reasons=[],
            metadata={"source": "settings_or_env"},
        ),
    )


def _format_decimal(value: float | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)).normalize(), "f")


def _format_decimal_from_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _build_signed_params(*, params: dict[str, Any], api_secret: str) -> dict[str, Any]:
    clean_params = {k: v for k, v in params.items() if v is not None}
    query = urlencode(clean_params)
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return {**clean_params, "signature": signature}


def _map_binance_order_status(status: str | None) -> str:
    normalized = str(status or "").upper()
    mapping = {
        "NEW": "open",
        "PARTIALLY_FILLED": "partially_filled",
        "FILLED": "filled",
        "CANCELED": "cancelled",
        "EXPIRED": "cancelled",
        "REJECTED": "rejected",
    }
    return mapping.get(normalized, "unknown")


class BinanceExecutionAdapterLive(BaseExecutionAdapter):
    venue_id = "binance"

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        transport: BinanceTransport | None = None,
        rule_loader: BinanceRuleLoader | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.transport = transport or HttpxBinanceTransport()
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.environment_mode = resolve_binance_environment_mode(self.settings)
        self.base_url = resolve_binance_trade_base_url(self.settings)
        self.recv_window = int(getattr(self.settings, "binance_recv_window_ms", 5000) or 5000)
        self.rule_loader = rule_loader or ExchangeInfoBinanceRuleLoader(base_url=self.base_url, transport=self.transport)

    def _signed_headers(self, api_key: str) -> dict[str, str]:
        return {"X-MBX-APIKEY": api_key}

    def _build_place_preview(self, intent: OrderIntent) -> VenueRequestPreview:
        payload = {
            "symbol": intent.symbol,
            "side": intent.side.upper(),
            "type": (intent.order_type or "market").upper(),
            "quantity": _format_decimal(intent.quantity),
            "price": _format_decimal(intent.price),
            "timeInForce": intent.time_in_force.upper() if intent.time_in_force else None,
            "reduceOnly": intent.reduce_only,
            "newClientOrderId": intent.client_order_id,
        }
        errors: list[str] = []
        if not intent.symbol:
            errors.append("symbol_required")
        if intent.quantity is None:
            errors.append("quantity_required")
        if intent.order_type == "limit" and intent.price is None:
            errors.append("price_required_for_limit")

        return VenueRequestPreview(
            venue_id=self.venue_id,
            operation="place_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id,
            payload={k: v for k, v in payload.items() if v is not None},
            validation_errors=errors,
            validation_warnings=[],
            metadata={"pilot_adapter": "binance_live_v1"},
            notes="binance_futures_place_order_signed_rest",
            is_live=True,
        )

    async def place_order(self, intent: OrderIntent) -> AdapterExecutionResult:
        credentials, readiness = load_binance_credentials(self.settings)
        preview = self._build_place_preview(intent)
        validation_errors = list(preview.validation_errors)
        validation_warnings = list(preview.validation_warnings)
        metadata = dict(preview.metadata)

        symbol_rules: BinanceSymbolTradingRules | None = None
        if readiness.status == "present" and not validation_errors:
            try:
                exchange_info = await self.rule_loader.load_exchange_info()
            except Exception:  # noqa: BLE001
                exchange_info = None
            if not isinstance(exchange_info, dict):
                validation_errors.append("binance_symbol_rules_unavailable")
            else:
                symbol_rules = parse_binance_exchange_info_symbol_rules(exchange_info, intent.symbol)
                if symbol_rules is None:
                    validation_errors.append("binance_symbol_not_found")

        normalized_payload = dict(preview.payload)
        normalization_applied = False
        final_client_order_id = str(intent.client_order_id or "").strip() or None
        if symbol_rules is not None and not validation_errors:
            validation_result = validate_and_normalize_order_intent(intent, symbol_rules)
            validation_errors.extend(validation_result.errors)
            validation_warnings.extend(validation_result.warnings)
            normalization_applied = validation_result.normalization_applied
            final_client_order_id = validation_result.final_client_order_id
            normalized_payload["newClientOrderId"] = validation_result.final_client_order_id
            normalized_payload["quantity"] = _format_decimal_from_decimal(validation_result.normalized_quantity)
            normalized_payload["price"] = _format_decimal_from_decimal(validation_result.normalized_price)

        preview.payload = {k: v for k, v in normalized_payload.items() if v is not None}
        preview.validation_errors = sorted(set(validation_errors))
        preview.validation_warnings = sorted(set(validation_warnings))
        metadata.update(
            {
                "environment_mode": self.environment_mode,
                "normalization_applied": normalization_applied,
                "final_quantity": preview.payload.get("quantity"),
                "final_price": preview.payload.get("price"),
                "final_client_order_id": final_client_order_id,
            }
        )
        preview.metadata = metadata
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="place_order",
            normalized_intent_id=intent.client_order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not preview.validation_errors,
            is_live=True,
        )

        if readiness.status != "present":
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="place_order",
                accepted=False,
                message=f"credentials_not_ready:{readiness.status}",
                metadata={"credential_readiness": readiness.model_dump()},
                translation=translation,
                notes="binance_live_v1_blocked_credentials",
                is_live=False,
            )
        if preview.validation_errors:
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="place_order",
                accepted=False,
                message="validation_failed",
                metadata={"credential_readiness": readiness.model_dump(), "validation_errors": preview.validation_errors},
                translation=translation,
                notes="binance_live_v1_validation_failed",
                is_live=False,
            )

        params = {
            **preview.payload,
            "timestamp": self.clock_ms(),
            "recvWindow": self.recv_window,
        }
        signed_params = _build_signed_params(params=params, api_secret=credentials.api_secret)
        try:
            response_payload = await self.transport.request(
                method="POST",
                url=f"{self.base_url}/fapi/v1/order",
                headers=self._signed_headers(credentials.api_key),
                params=signed_params,
            )
        except Exception as exc:  # noqa: BLE001
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="place_order",
                accepted=False,
                message=f"request_failed:{type(exc).__name__}",
                metadata={"credential_readiness": readiness.model_dump()},
                translation=translation,
                notes="binance_live_v1_request_failed",
                is_live=False,
            )

        status = _map_binance_order_status(response_payload.get("status"))
        order_status = OrderStatusSnapshot(
            venue_id=self.venue_id,
            order_id=str(response_payload.get("orderId")) if response_payload.get("orderId") is not None else None,
            client_order_id=response_payload.get("clientOrderId"),
            symbol=response_payload.get("symbol"),
            side=intent.side,
            order_type=intent.order_type,
            status=status,
            quantity=float(response_payload.get("origQty")) if response_payload.get("origQty") is not None else intent.quantity,
            filled_qty=float(response_payload.get("executedQty")) if response_payload.get("executedQty") is not None else None,
            metadata={"raw_status": response_payload.get("status")},
            is_live=True,
        )
        return AdapterExecutionResult(
            venue_id=self.venue_id,
            operation="place_order",
            accepted=status in {"accepted", "open", "partially_filled", "filled"},
            message=f"binance_status:{response_payload.get('status', 'UNKNOWN')}",
            order_status=order_status,
            metadata={
                "credential_readiness": readiness.model_dump(),
                "final_quantity": preview.payload.get("quantity"),
                "final_price": preview.payload.get("price"),
                "final_client_order_id": preview.payload.get("newClientOrderId"),
                "validation_errors": preview.validation_errors,
                "validation_warnings": preview.validation_warnings,
                "normalization_applied": preview.metadata.get("normalization_applied", False),
            },
            translation=translation,
            notes="binance_live_v1_submitted",
            is_live=True,
        )

    async def cancel_order(self, intent: CancelIntent) -> AdapterExecutionResult:
        credentials, readiness = load_binance_credentials(self.settings)
        errors: list[str] = []
        warnings: list[str] = []
        if not intent.symbol:
            errors.append("symbol_required")
        if not intent.order_id and not intent.client_order_id:
            errors.append("order_id_or_client_order_id_required")
        final_client_order_id = str(intent.client_order_id or "").strip() or None
        if final_client_order_id is not None:
            if len(final_client_order_id) > 36:
                errors.append("client_order_id_too_long")
            elif not all(ch.isalnum() or ch in "._-:/" for ch in final_client_order_id):
                errors.append("client_order_id_invalid")
        preview = VenueRequestPreview(
            venue_id=self.venue_id,
            operation="cancel_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id or intent.order_id,
            payload={
                "symbol": intent.symbol,
                "orderId": intent.order_id,
                "origClientOrderId": final_client_order_id,
            },
            validation_errors=errors,
            validation_warnings=warnings,
            metadata={"pilot_adapter": "binance_live_v1", "final_client_order_id": final_client_order_id},
            is_live=True,
        )
        translation = VenueTranslationResult(
            venue_id=self.venue_id,
            operation="cancel_order",
            normalized_intent_id=intent.client_order_id or intent.order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=not errors,
            is_live=True,
        )
        if readiness.status != "present":
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="cancel_order",
                accepted=False,
                message=f"credentials_not_ready:{readiness.status}",
                metadata={"credential_readiness": readiness.model_dump()},
                translation=translation,
                notes="binance_live_v1_blocked_credentials",
                is_live=False,
            )
        if errors:
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="cancel_order",
                accepted=False,
                message="validation_failed",
                metadata={"credential_readiness": readiness.model_dump()},
                translation=translation,
                notes="binance_live_v1_validation_failed",
                is_live=False,
            )

        params = {
            "symbol": intent.symbol,
            "orderId": intent.order_id,
            "origClientOrderId": intent.client_order_id,
            "timestamp": self.clock_ms(),
            "recvWindow": self.recv_window,
        }
        signed_params = _build_signed_params(params=params, api_secret=credentials.api_secret)
        try:
            response_payload = await self.transport.request(
                method="DELETE",
                url=f"{self.base_url}/fapi/v1/order",
                headers=self._signed_headers(credentials.api_key),
                params=signed_params,
            )
        except Exception as exc:  # noqa: BLE001
            return AdapterExecutionResult(
                venue_id=self.venue_id,
                operation="cancel_order",
                accepted=False,
                message=f"request_failed:{type(exc).__name__}",
                metadata={"credential_readiness": readiness.model_dump()},
                translation=translation,
                notes="binance_live_v1_request_failed",
                is_live=False,
            )

        status = _map_binance_order_status(response_payload.get("status"))
        return AdapterExecutionResult(
            venue_id=self.venue_id,
            operation="cancel_order",
            accepted=status in {"cancelled", "filled", "open", "partially_filled"},
            message=f"binance_status:{response_payload.get('status', 'UNKNOWN')}",
            metadata={"credential_readiness": readiness.model_dump(), "raw_status": response_payload.get("status")},
            translation=translation,
            notes="binance_live_v1_cancel_submitted",
            is_live=True,
        )

    async def get_order_status(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
        symbol: str | None = None,
    ) -> OrderStatusSnapshot:
        credentials, readiness = load_binance_credentials(self.settings)
        if readiness.status != "present":
            return OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                status="unknown",
                metadata={"credential_readiness": readiness.model_dump()},
                notes=f"credentials_not_ready:{readiness.status}",
                is_live=False,
            )

        if client_order_id is not None and len(client_order_id) > 36:
            return OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                status="unknown",
                metadata={"validation_error": "client_order_id_too_long"},
                notes="client_order_id_too_long",
                is_live=False,
            )
        if client_order_id is not None and not all(ch.isalnum() or ch in "._-:/" for ch in client_order_id):
            return OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                status="unknown",
                metadata={"validation_error": "client_order_id_invalid"},
                notes="client_order_id_invalid",
                is_live=False,
            )
        if not symbol or (not order_id and not client_order_id):
            return OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                status="unknown",
                metadata={"validation_error": True},
                notes="symbol_and_order_id_or_client_order_id_required",
                is_live=False,
            )

        params = {
            "symbol": symbol,
            "orderId": order_id,
            "origClientOrderId": client_order_id,
            "timestamp": self.clock_ms(),
            "recvWindow": self.recv_window,
        }
        signed_params = _build_signed_params(params=params, api_secret=credentials.api_secret)
        try:
            response_payload = await self.transport.request(
                method="GET",
                url=f"{self.base_url}/fapi/v1/order",
                headers=self._signed_headers(credentials.api_key),
                params=signed_params,
            )
        except Exception as exc:  # noqa: BLE001
            return OrderStatusSnapshot(
                venue_id=self.venue_id,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                status="unknown",
                metadata={"request_failed": type(exc).__name__},
                notes="binance_live_v1_request_failed",
                is_live=False,
            )

        mapped_status = _map_binance_order_status(response_payload.get("status"))
        filled_qty = float(response_payload.get("executedQty")) if response_payload.get("executedQty") is not None else None
        quantity = float(response_payload.get("origQty")) if response_payload.get("origQty") is not None else None
        remaining_qty = None
        if quantity is not None and filled_qty is not None:
            remaining_qty = max(quantity - filled_qty, 0.0)

        return OrderStatusSnapshot(
            venue_id=self.venue_id,
            order_id=str(response_payload.get("orderId")) if response_payload.get("orderId") is not None else order_id,
            client_order_id=response_payload.get("clientOrderId") or client_order_id,
            symbol=response_payload.get("symbol") or symbol,
            side=(str(response_payload.get("side") or "").lower() or None),
            order_type=(str(response_payload.get("type") or "").lower() or None),
            status=mapped_status,
            quantity=quantity,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            average_fill_price=float(response_payload.get("avgPrice")) if response_payload.get("avgPrice") not in (None, "") else None,
            metadata={"raw_status": response_payload.get("status")},
            is_live=True,
        )

    async def get_position(self, *, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(
            venue_id=self.venue_id,
            symbol=symbol,
            notes="position_not_implemented_in_binance_live_pilot_v1",
            metadata={"pilot_scope": "place_cancel_order_status_only"},
            is_live=False,
        )

    async def get_balance(self, *, asset: str) -> BalanceSnapshot:
        return BalanceSnapshot(
            venue_id=self.venue_id,
            asset=asset,
            notes="balance_not_implemented_in_binance_live_pilot_v1",
            metadata={"pilot_scope": "place_cancel_order_status_only"},
            is_live=False,
        )
