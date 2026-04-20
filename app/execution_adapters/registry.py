from __future__ import annotations

from app.execution_adapters.base import BaseExecutionAdapter
from app.models.execution import ExecutionAdapterCapability
from app.execution_adapters.paper import PaperExecutionAdapter
from app.execution_adapters.stubs import (
    BinanceExecutionAdapterStub,
    HyperliquidExecutionAdapterStub,
    LighterExecutionAdapterStub,
    OkxExecutionAdapterStub,
)


ADAPTER_REGISTRY: dict[str, type[BaseExecutionAdapter]] = {
    "binance": BinanceExecutionAdapterStub,
    "okx": OkxExecutionAdapterStub,
    "hyperliquid": HyperliquidExecutionAdapterStub,
    "lighter": LighterExecutionAdapterStub,
    "paper": PaperExecutionAdapter,
}

ADAPTER_CAPABILITIES: dict[str, ExecutionAdapterCapability] = {
    "binance": ExecutionAdapterCapability(
        venue_id="binance",
        supports_live_submit_now=True,
        supports_cancel_now=True,
        supports_order_status_now=True,
        credential_type="binance_api_key_secret",
        sandbox_or_testnet_supported=True,
        stub_only=False,
    ),
    "okx": ExecutionAdapterCapability(
        venue_id="okx",
        credential_type="okx_api_key_secret_passphrase",
    ),
    "hyperliquid": ExecutionAdapterCapability(
        venue_id="hyperliquid",
        credential_type="hyperliquid_wallet_signature",
    ),
    "lighter": ExecutionAdapterCapability(
        venue_id="lighter",
        credential_type="lighter_api_key_and_signature",
    ),
    "paper": ExecutionAdapterCapability(
        venue_id="paper",
        credential_type="none",
        stub_only=False,
    ),
}


def get_execution_adapter(venue_id: str) -> BaseExecutionAdapter:
    normalized_venue_id = str(venue_id).lower()
    adapter_cls = ADAPTER_REGISTRY.get(normalized_venue_id)
    if adapter_cls is None:
        raise ValueError(f"unsupported_execution_venue:{normalized_venue_id}")
    return adapter_cls()


def get_execution_adapter_capability(venue_id: str) -> ExecutionAdapterCapability:
    normalized_venue_id = str(venue_id).lower()
    capability = ADAPTER_CAPABILITIES.get(normalized_venue_id)
    if capability is None:
        raise ValueError(f"unsupported_execution_venue:{normalized_venue_id}")
    return capability.model_copy(deep=True)


def list_execution_adapter_capabilities() -> list[ExecutionAdapterCapability]:
    return [item.model_copy(deep=True) for item in ADAPTER_CAPABILITIES.values()]
