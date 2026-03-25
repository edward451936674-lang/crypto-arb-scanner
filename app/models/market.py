from typing import Any, Literal

from pydantic import BaseModel, Field

BPS_MULTIPLIER = 10_000


class MarketSnapshot(BaseModel):
    """Normalized perpetual market snapshot.

    Funding timestamp semantics:
    - funding_time_ms: settlement timestamp for the funding rate in this snapshot
    - next_funding_time_ms: next known settlement timestamp (if the venue provides it)
    - funding_period_hours: cadence between settlements for the venue
    - hourly_funding_rate: funding_rate normalized to one hour
    - hourly_funding_rate_bps: hourly_funding_rate expressed in basis points
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
    hourly_funding_rate: float | None = None
    hourly_funding_rate_bps: float | None = None
    open_interest_usd: float | None = None
    quote_volume_24h_usd: float | None = None
    timestamp_ms: int
    raw: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        """Normalize hourly funding fields once at the snapshot layer.

        If either `funding_rate` or `funding_period_hours` is missing/zero, the
        hourly funding fields remain `None`.
        """
        if self.hourly_funding_rate is None:
            self.hourly_funding_rate = self._compute_hourly_funding_rate(
                self.funding_rate,
                self.funding_period_hours,
            )
        if self.hourly_funding_rate_bps is None:
            self.hourly_funding_rate_bps = (
                None if self.hourly_funding_rate is None else self.hourly_funding_rate * BPS_MULTIPLIER
            )

    @staticmethod
    def _compute_hourly_funding_rate(
        funding_rate: float | None,
        funding_period_hours: int | None,
    ) -> float | None:
        if funding_rate is None or funding_period_hours in (None, 0):
            return None
        return funding_rate / funding_period_hours


class ExchangeError(BaseModel):
    exchange: str
    message: str


class MarketDataResponse(BaseModel):
    requested_symbols: list[str]
    snapshots: list[MarketSnapshot]
    errors: list[ExchangeError] = Field(default_factory=list)


class Opportunity(BaseModel):
    symbol: str
    cluster_id: str | None = None
    is_primary_route: bool = False
    route_rank: int | None = None
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
    funding_confidence_score: float
    funding_confidence_label: str
    conviction_score: float = 0.0
    conviction_label: str = "low"
    conviction_drivers: list[str] = Field(default_factory=list)
    size_up_eligible: bool = False
    risk_adjusted_edge_bps: float
    risk_flags: list[str] = Field(default_factory=list)
    opportunity_grade: str
    is_tradable: bool
    reject_reasons: list[str] = Field(default_factory=list)
    position_size_multiplier: float
    suggested_position_pct: float
    final_position_pct: float = 0.0
    max_position_pct: float
    execution_mode: str
    portfolio_clamp_reasons: list[str] = Field(default_factory=list)
    portfolio_reject_reasons: list[str] = Field(default_factory=list)
    portfolio_total_position_after: float = 0.0
    portfolio_symbol_position_after: float = 0.0
    portfolio_long_exchange_position_after: float = 0.0
    portfolio_short_exchange_position_after: float = 0.0
    portfolio_rank: int | None = None
    allocation_priority_label: str | None = None


class OpportunitiesResponse(BaseModel):
    requested_symbols: list[str]
    opportunities: list[Opportunity]
    snapshot_errors: list[ExchangeError] = Field(default_factory=list)
