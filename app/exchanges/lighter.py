from __future__ import annotations

import asyncio
import json
import ssl
import time
from typing import Any

import certifi
import httpx
import websockets

from app.core.config import Settings
from app.core.symbols import SymbolSpec
from app.models.market import MarketSnapshot

from .base import ExchangeClient, ExchangeClientError


class LighterClient(ExchangeClient):
    name = "lighter"
    venue_type = "dex"

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._ca_file = certifi.where()
        self._ssl_context = ssl.create_default_context(cafile=self._ca_file)
        # Keep trust_env=True so existing proxy/env behavior remains intact.
        self._lighter_http = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            headers={"User-Agent": "crypto-arb-scanner/0.1"},
            verify=self._ssl_context,
            trust_env=True,
        )

    async def aclose(self) -> None:
        await asyncio.gather(super().aclose(), self._lighter_http.aclose())

    async def fetch_snapshots(self, specs: list[SymbolSpec]) -> list[MarketSnapshot]:
        market_map = await self._fetch_market_id_map()
        wanted_ids: dict[int, SymbolSpec] = {}
        for spec in specs:
            market_id = market_map.get(spec.lighter_symbol.upper())
            if market_id is None:
                raise ExchangeClientError(f"Lighter market id not found for {spec.lighter_symbol}")
            wanted_ids[market_id] = spec

        stats_by_symbol = await self._fetch_market_stats_ws(wanted_ids)
        snapshots: list[MarketSnapshot] = []

        for spec in specs:
            stats = stats_by_symbol.get(spec.lighter_symbol.upper())
            if stats is None:
                raise ExchangeClientError(f"Missing Lighter market stats for {spec.lighter_symbol}")

            timestamp_ms = self._to_int(stats.get("_message_timestamp_ms")) or int(time.time() * 1000)
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    venue_type=self.venue_type,
                    base_symbol=spec.base_symbol,
                    normalized_symbol=spec.normalized_symbol,
                    instrument_id=str(stats["market_id"]),
                    mark_price=float(stats["mark_price"]),
                    index_price=self._to_float(stats.get("index_price")),
                    last_price=self._to_float(stats.get("last_trade_price")),
                    funding_rate=self._to_float(stats.get("current_funding_rate")),
                    funding_rate_source="estimated_current",
                    funding_time_ms=self._to_int(stats.get("funding_timestamp")),
                    next_funding_time_ms=None,
                    funding_period_hours=4,
                    timestamp_ms=timestamp_ms,
                    raw=stats,
                )
            )

        return snapshots

    async def _fetch_market_id_map(self) -> dict[str, int]:
        response = await self._lighter_http.get(self.settings.lighter_markets_url)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ExchangeClientError("Unexpected Lighter markets payload shape")

        mapping: dict[str, int] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            market_index = item.get("market_index")
            if isinstance(symbol, str) and market_index is not None:
                mapping[symbol.upper()] = int(market_index)
        return mapping

    async def _fetch_market_stats_ws(self, wanted_ids: dict[int, SymbolSpec]) -> dict[str, dict[str, Any]]:
        collected: dict[str, dict[str, Any]] = {}
        pending = set(wanted_ids)

        async with websockets.connect(
            self.settings.lighter_ws_url,
            ping_interval=20,
            close_timeout=1,
            ssl=self._ssl_context,
        ) as ws:
            for market_id in wanted_ids:
                await ws.send(json.dumps({"type": "subscribe", "channel": f"market_stats/{market_id}"}))

            deadline = time.monotonic() + self.settings.lighter_ws_timeout_seconds
            while pending and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                raw_message = await asyncio.wait_for(ws.recv(), timeout=max(remaining, 0.1))
                message = json.loads(raw_message)
                if message.get("type") != "update/market_stats":
                    continue
                stats = message.get("market_stats")
                if not isinstance(stats, dict):
                    continue
                market_id = stats.get("market_id")
                symbol = stats.get("symbol")
                if market_id is None or not isinstance(symbol, str):
                    continue
                stats["_message_timestamp_ms"] = message.get("timestamp")
                collected[symbol.upper()] = stats
                pending.discard(int(market_id))

        return collected
