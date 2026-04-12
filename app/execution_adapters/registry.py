from __future__ import annotations

from app.execution_adapters.base import BaseExecutionAdapter
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


def get_execution_adapter(venue_id: str) -> BaseExecutionAdapter:
    adapter_cls = ADAPTER_REGISTRY.get(str(venue_id).lower(), PaperExecutionAdapter)
    return adapter_cls()
