from typing import Any, Literal

from pydantic import BaseModel, Field


class MarketSnapshot(BaseModel):
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
    timestamp_ms: int
    raw: dict[str, Any] = Field(default_factory=dict)


class ExchangeError(BaseModel):
    exchange: str
    message: str


class MarketDataResponse(BaseModel):
    requested_symbols: list[str]
    snapshots: list[MarketSnapshot]
    errors: list[ExchangeError] = Field(default_factory=list)
