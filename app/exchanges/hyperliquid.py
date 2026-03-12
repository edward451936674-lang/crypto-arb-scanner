from __future__ import annotations

import time
from typing import Any

from app.core.symbols import SymbolSpec
from app.models.market import MarketSnapshot

from .base import ExchangeClient, ExchangeClientError


class HyperliquidClient(ExchangeClient):
    name = "hyperliquid"
    venue_type = "dex"

    async def fetch_snapshots(self, specs: list[SymbolSpec]) -> list[MarketSnapshot]:
        response = await self.http.post(
            self.settings.hyperliquid_info_url,
            json={"type": "metaAndAssetCtxs", "dex": self.settings.hyperliquid_dex},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or len(payload) < 2:
            raise ExchangeClientError("Unexpected Hyperliquid metaAndAssetCtxs payload shape")

        meta, contexts = payload[0], payload[1]
        if not isinstance(meta, dict):
            raise ExchangeClientError("Unexpected Hyperliquid metadata payload shape")
        universe = meta.get("universe")
        if not isinstance(universe, list) or not isinstance(contexts, list):
            raise ExchangeClientError("Unexpected Hyperliquid universe/contexts payload shape")

        by_coin: dict[str, dict[str, Any]] = {}
        for asset, context in zip(universe, contexts):
            if not isinstance(asset, dict) or not isinstance(context, dict):
                continue
            asset_name = str(asset.get("name", "")).upper()
            by_coin[asset_name] = {"asset": asset, "context": context}

        now_ms = int(time.time() * 1000)
        snapshots: list[MarketSnapshot] = []
        for spec in specs:
            record = by_coin.get(spec.hyperliquid_coin.upper())
            if record is None:
                raise ExchangeClientError(f"Hyperliquid symbol not found: {spec.hyperliquid_coin}")

            context = record["context"]
            asset = record["asset"]
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    venue_type=self.venue_type,
                    base_symbol=spec.base_symbol,
                    normalized_symbol=spec.normalized_symbol,
                    instrument_id=str(asset.get("name", spec.hyperliquid_coin)),
                    mark_price=float(context["markPx"]),
                    index_price=self._to_float(context.get("oraclePx")),
                    funding_rate=self._to_float(context.get("funding")),
                    funding_rate_source="current_8h",
                    funding_time_ms=None,
                    next_funding_time_ms=None,
                    funding_period_hours=8,
                    timestamp_ms=now_ms,
                    raw={"asset": asset, "context": context},
                )
            )

        return snapshots
