from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class VenueId(str, Enum):
    BINANCE = "binance"
    OKX = "okx"
    HYPERLIQUID = "hyperliquid"
    LIGHTER = "lighter"


class VenueType(str, Enum):
    CEX = "cex"
    DEX = "dex"


class VenueExecutionStyle(str, Enum):
    PAPER_ONLY = "paper_only"
    OFFLINE_MOCK = "offline_mock"


class VenueAuthStyle(str, Enum):
    NONE = "none"
    API_KEY_SECRET = "api_key_secret"
    WALLET_SIGNATURE = "wallet_signature"


class VenueCapabilities(BaseModel):
    supports_snapshots: bool
    supports_place_order: bool
    supports_cancel_order: bool
    supports_order_status: bool
    supports_positions: bool
    supports_balances: bool
    paper_supported: bool
    live_supported_now: bool
    supports_rest_trading_api: bool = False
    supports_private_websocket_trading: bool = False
    supports_signed_actions: bool = False
    supports_sdk_recommended: bool = False
    execution_interface_notes: str = ""


class VenueDefinition(BaseModel):
    venue_id: VenueId
    display_name: str
    venue_type: VenueType
    market_adapter_module: str
    execution_adapter_module: str
    execution_style: VenueExecutionStyle
    auth_style: VenueAuthStyle
    capabilities: VenueCapabilities
    notes: str
