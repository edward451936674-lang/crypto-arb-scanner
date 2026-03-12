from __future__ import annotations

import asyncio

from app.core.config import Settings
from app.core.symbols import resolve_symbol_specs
from app.exchanges.binance import BinanceClient
from app.exchanges.hyperliquid import HyperliquidClient
from app.exchanges.lighter import LighterClient
from app.exchanges.okx import OkxClient
from app.models.market import ExchangeError, MarketDataResponse, MarketSnapshot


class MarketDataService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.clients = []
        if settings.enable_binance:
            self.clients.append(BinanceClient(settings))
        if settings.enable_okx:
            self.clients.append(OkxClient(settings))
        if settings.enable_hyperliquid:
            self.clients.append(HyperliquidClient(settings))
        if settings.enable_lighter:
            self.clients.append(LighterClient(settings))

    async def fetch_snapshots(self, symbols: list[str]) -> MarketDataResponse:
        specs = resolve_symbol_specs(symbols)
        try:
            results = await asyncio.gather(
                *(client.fetch_snapshots(specs) for client in self.clients),
                return_exceptions=True,
            )

            snapshots: list[MarketSnapshot] = []
            errors: list[ExchangeError] = []

            for client, result in zip(self.clients, results):
                if isinstance(result, Exception):
                    errors.append(ExchangeError(exchange=client.name, message=str(result)))
                else:
                    snapshots.extend(result)

            snapshots.sort(key=lambda item: (item.base_symbol, item.exchange))
            return MarketDataResponse(
                requested_symbols=[spec.base_symbol for spec in specs],
                snapshots=snapshots,
                errors=errors,
            )
        finally:
            await asyncio.gather(*(client.aclose() for client in self.clients), return_exceptions=True)
