from typing import Any, Literal

from pydantic import BaseModel, Field


class MarketSnapshot(BaseModel):
    """Normalized perpetual market snapshot.

    Funding timestamp semantics:
    - funding_time_ms: settlement timestamp for the funding rate in this snapshot
    - next_funding_time_ms: next known settlement timestamp (if the venue provides it)
    - funding_period_hours: cadence between settlements for the venue
    """

    exchange: str
    venue_type: Literal["cex", "dex"]
    base_symbol: str
    normalized_symbol: str
    instrument_id: str
    mark_price: float
    index_price: float | None = None
    last_price: float | None = None
    funding_rate: float | None = None
    funding_rate_source: str | None = None
    funding_time_ms: int | None = None
    next_funding_time_ms: int | None = None
    funding_period_hours: int | None = None
    timestamp_ms: int
    raw: dict[str, Any] = Field(default_factory=dict)


class ExchangeError(BaseModel):
    exchange: str
    message: str


class MarketDataResponse(BaseModel):
    requested_symbols: list[str]
    snapshots: list[MarketSnapshot]
    errors: list[ExchangeError] = Field(default_factory=list)


class Opportunity(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    long_price: float
    short_price: float
    price_spread_abs: float
    price_spread_bps: float
    long_funding_rate: float | None = None
    short_funding_rate: float | None = None
    funding_rate_diff: float | None = None
    funding_spread_bps: float | None = None
    long_funding_period_hours: int | None = None
    short_funding_period_hours: int | None = None
    long_hourly_funding_rate: float | None = None
    short_hourly_funding_rate: float | None = None
    hourly_funding_rate_diff: float | None = None
    hourly_funding_spread_bps: float | None = None
    estimated_edge_bps: float
    holding_hours: int
    expected_funding_edge_bps: float
    estimated_fee_bps: float
    net_edge_bps: float


class OpportunitiesResponse(BaseModel):
    requested_symbols: list[str]
    opportunities: list[Opportunity]
    snapshot_errors: list[ExchangeError] = Field(default_factory=list)
