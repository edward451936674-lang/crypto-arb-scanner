from __future__ import annotations

from app.venues.models import (
    VenueAuthStyle,
    VenueCapabilities,
    VenueDefinition,
    VenueExecutionStyle,
    VenueId,
    VenueType,
)

VENUE_REGISTRY: tuple[VenueDefinition, ...] = (
    VenueDefinition(
        venue_id=VenueId.BINANCE,
        display_name="Binance",
        venue_type=VenueType.CEX,
        market_adapter_module="app.exchanges.binance.BinanceClient",
        execution_adapter_module="app.execution_adapters.paper.PaperExecutionAdapter",
        execution_style=VenueExecutionStyle.PAPER_ONLY,
        auth_style=VenueAuthStyle.API_KEY_SECRET,
        capabilities=VenueCapabilities(
            supports_snapshots=True,
            supports_place_order=False,
            supports_cancel_order=False,
            supports_order_status=False,
            supports_positions=False,
            supports_balances=False,
            paper_supported=True,
            live_supported_now=False,
        ),
        notes="Market data adapter is active. Execution remains paper-only in this repository.",
    ),
    VenueDefinition(
        venue_id=VenueId.OKX,
        display_name="OKX",
        venue_type=VenueType.CEX,
        market_adapter_module="app.exchanges.okx.OkxClient",
        execution_adapter_module="app.execution_adapters.paper.PaperExecutionAdapter",
        execution_style=VenueExecutionStyle.PAPER_ONLY,
        auth_style=VenueAuthStyle.API_KEY_SECRET,
        capabilities=VenueCapabilities(
            supports_snapshots=True,
            supports_place_order=False,
            supports_cancel_order=False,
            supports_order_status=False,
            supports_positions=False,
            supports_balances=False,
            paper_supported=True,
            live_supported_now=False,
        ),
        notes="Market data adapter is active. Execution remains paper-only in this repository.",
    ),
    VenueDefinition(
        venue_id=VenueId.HYPERLIQUID,
        display_name="Hyperliquid",
        venue_type=VenueType.DEX,
        market_adapter_module="app.exchanges.hyperliquid.HyperliquidClient",
        execution_adapter_module="app.execution_adapters.paper.PaperExecutionAdapter",
        execution_style=VenueExecutionStyle.PAPER_ONLY,
        auth_style=VenueAuthStyle.WALLET_SIGNATURE,
        capabilities=VenueCapabilities(
            supports_snapshots=True,
            supports_place_order=False,
            supports_cancel_order=False,
            supports_order_status=False,
            supports_positions=False,
            supports_balances=False,
            paper_supported=True,
            live_supported_now=False,
        ),
        notes="Market data adapter is active. Execution remains paper-only in this repository.",
    ),
    VenueDefinition(
        venue_id=VenueId.LIGHTER,
        display_name="Lighter",
        venue_type=VenueType.DEX,
        market_adapter_module="app.exchanges.lighter.LighterClient",
        execution_adapter_module="app.execution_adapters.paper.PaperExecutionAdapter",
        execution_style=VenueExecutionStyle.PAPER_ONLY,
        auth_style=VenueAuthStyle.WALLET_SIGNATURE,
        capabilities=VenueCapabilities(
            supports_snapshots=True,
            supports_place_order=False,
            supports_cancel_order=False,
            supports_order_status=False,
            supports_positions=False,
            supports_balances=False,
            paper_supported=True,
            live_supported_now=False,
        ),
        notes="Market data adapter is active. Execution remains paper-only in this repository.",
    ),
)


def list_venue_definitions() -> list[VenueDefinition]:
    return [item.model_copy(deep=True) for item in VENUE_REGISTRY]
