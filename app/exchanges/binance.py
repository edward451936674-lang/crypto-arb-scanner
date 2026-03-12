from __future__ import annotations

import asyncio

from app.core.symbols import SymbolSpec
from app.models.market import MarketSnapshot

from .base import ExchangeClient, ExchangeClientError

HOUR_TO_MS = 60 * 60 * 1000
BINANCE_FUNDING_PERIOD_HOURS = 8


class BinanceClient(ExchangeClient):
    name = "binance"
    venue_type = "cex"

    async def fetch_snapshots(self, specs: list[SymbolSpec]) -> list[MarketSnapshot]:
        tasks = [self._fetch_one(spec) for spec in specs]
        return list(await asyncio.gather(*tasks))

    async def _fetch_one(self, spec: SymbolSpec) -> MarketSnapshot:
        response = await self.http.get(
            f"{self.settings.binance_base_url}/fapi/v1/premiumIndex",
            params={"symbol": spec.binance_symbol},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ExchangeClientError("Unexpected Binance payload shape")

        next_funding_time_ms = self._to_int(payload.get("nextFundingTime"))
        funding_time_ms = None
        if funding_time_ms is None and next_funding_time_ms is not None:
            # Binance premiumIndex does not provide current funding settlement time directly.
            # Derive current settlement timestamp from next settlement minus one funding period.
            funding_time_ms = next_funding_time_ms - (BINANCE_FUNDING_PERIOD_HOURS * HOUR_TO_MS)

        return MarketSnapshot(
            exchange=self.name,
            venue_type=self.venue_type,
            base_symbol=spec.base_symbol,
            normalized_symbol=spec.normalized_symbol,
            instrument_id=str(payload["symbol"]),
            mark_price=float(payload["markPrice"]),
            index_price=self._to_float(payload.get("indexPrice")),
            funding_rate=self._to_float(payload.get("lastFundingRate")),
            funding_rate_source="latest_reported",
            funding_time_ms=funding_time_ms,
            next_funding_time_ms=next_funding_time_ms,
            funding_period_hours=BINANCE_FUNDING_PERIOD_HOURS,
            timestamp_ms=int(payload["time"]),
            raw=payload,
        )
