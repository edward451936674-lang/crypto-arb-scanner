from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal

import httpx

from app.core.config import Settings
from app.core.symbols import SymbolSpec
from app.models.market import MarketSnapshot


class ExchangeClientError(RuntimeError):
    pass


class ExchangeClient(ABC):
    name: str
    venue_type: Literal["cex", "dex"] = "cex"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.http = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            headers={"User-Agent": "crypto-arb-scanner/0.1"},
        )

    async def aclose(self) -> None:
        await self.http.aclose()

    @abstractmethod
    async def fetch_snapshots(self, specs: list[SymbolSpec]) -> list[MarketSnapshot]:
        raise NotImplementedError

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        if value == "":
            return None
        return float(value)

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None:
            return None
        if value == "":
            return None
        return int(value)

    @staticmethod
    def _require_list(payload: Any, field_name: str) -> list[Any]:
        if not isinstance(payload, dict):
            raise ExchangeClientError(f"Expected dict payload for {field_name}, got {type(payload).__name__}")
        data = payload.get(field_name)
        if not isinstance(data, list):
            raise ExchangeClientError(f"Expected list payload field '{field_name}'")
        return data
