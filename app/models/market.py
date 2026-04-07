from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

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
    data_quality_status: str | None = None
    data_quality_score: float | None = None
    data_quality_flags: list[str] = Field(default_factory=list)
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
    data_quality_status: str = "healthy"
    data_quality_score: float = 1.0
    data_quality_flags: list[str] = Field(default_factory=list)
    data_quality_drivers: list[str] = Field(default_factory=list)
    data_quality_penalty_multiplier: float = 1.0
    data_quality_adjusted_edge_bps: float = 0.0
    normal_required_edge_bps: float = 10.0
    size_up_required_edge_bps: float = 18.0
    size_up_edge_buffer_bps: float = 0.0
    edge_buffer_bps: float = 0.0
    normal_eligibility_score: float = 0.0
    soft_risk_flag_count: int = 0
    normal_blockers: list[str] = Field(default_factory=list)
    normal_promotion_reasons: list[str] = Field(default_factory=list)
    size_up_blockers: list[str] = Field(default_factory=list)
    size_up_promotion_reasons: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    opportunity_grade: str
    is_tradable: bool
    reject_reasons: list[str] = Field(default_factory=list)
    position_size_multiplier: float
    suggested_position_pct: float
    mode_base_cap_pct: float = 0.0
    remaining_total_cap_pct: float = 0.0
    remaining_symbol_cap_pct: float = 0.0
    remaining_long_exchange_cap_pct: float = 0.0
    remaining_short_exchange_cap_pct: float = 0.0
    absolute_single_opportunity_cap_pct: float = 0.0
    effective_leverage: float | None = None
    leverage_cap_pct: float | None = None
    long_liquidation_distance_pct: float | None = None
    short_liquidation_distance_pct: float | None = None
    worst_leg_liquidation_distance_pct: float | None = None
    liquidation_cap_pct: float | None = None
    final_single_cap_pct: float = 0.0
    extended_size_up_eligible: bool = False
    configured_target_leverage: float | None = None
    configured_max_allowed_leverage: float | None = None
    configured_min_required_liquidation_buffer_pct: float | None = None
    active_execution_policy_profile: str | None = None
    resolved_execution_extended_size_up_enabled: bool = False
    resolved_execution_target_leverage: float | None = None
    resolved_execution_max_allowed_leverage: float | None = None
    resolved_execution_required_liquidation_buffer_pct: float | None = None
    extended_size_up_risk_eligible: bool = False
    extended_size_up_risk_blockers: list[str] = Field(default_factory=list)
    extended_size_up_execution_ready: bool = False
    extended_size_up_execution_blockers: list[str] = Field(default_factory=list)
    execution_max_single_cap_pct: float = 0.0
    execution_cap_reasons: list[str] = Field(default_factory=list)
    final_position_pct: float = 0.0
    is_executable_now: bool = False
    max_position_pct: float
    execution_mode: str
    execution_mode_drivers: list[str] = Field(default_factory=list)
    portfolio_clamp_reasons: list[str] = Field(default_factory=list)
    portfolio_reject_reasons: list[str] = Field(default_factory=list)
    portfolio_total_used_after: float = 0.0
    portfolio_symbol_used_after: float = 0.0
    portfolio_long_exchange_used_after: float = 0.0
    portfolio_short_exchange_used_after: float = 0.0
    portfolio_rank: int | None = None
    allocation_priority_label: str | None = None
    route_key: str | None = None
    replay_net_after_cost_bps: float | None = None
    score: float | None = None
    rank: int | None = None
    opportunity_type: str | None = None


class OpportunitiesResponse(BaseModel):
    requested_symbols: list[str]
    opportunities: list[Opportunity]
    snapshot_errors: list[ExchangeError] = Field(default_factory=list)


class ReplayAssumptions(BaseModel):
    holding_mode: Literal["to_next_funding", "fixed_minutes"] = "to_next_funding"
    holding_minutes: int | None = None
    slippage_bps_per_leg: float
    extra_exit_slippage_bps_per_leg: float
    latency_decay_bps: float
    borrow_or_misc_cost_bps: float = 0.0

    @model_validator(mode="after")
    def _validate_fixed_minutes_inputs(self) -> "ReplayAssumptions":
        if self.holding_mode == "fixed_minutes" and (self.holding_minutes is None or self.holding_minutes <= 0):
            raise ValueError("holding_minutes must be provided and > 0 when holding_mode='fixed_minutes'")
        return self


class ReplayResearchMetrics(BaseModel):
    edge_retention_rate: float | None = None
    funding_capture_rate: float | None = None
    replay_cost_drag_bps: float
    research_confidence_score: float = Field(ge=0.0, le=1.0)


class OpportunityReplayResult(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    entry_price_edge_bps: float
    entry_expected_funding_edge_bps: float
    entry_net_edge_bps: float
    gross_price_edge_bps: float
    realized_funding_bps: float
    fees_bps: float
    slippage_bps: float
    latency_decay_bps: float
    borrow_or_misc_cost_bps: float
    net_realized_edge_bps: float
    holding_minutes: int
    long_funding_capture_fraction: float
    short_funding_capture_fraction: float
    pair_funding_capture_fraction: float
    replay_confidence_label: str
    research_metrics: ReplayResearchMetrics


class ReplayPreviewItem(BaseModel):
    cluster_id: str | None = None
    route_rank: int | None = None
    symbol: str
    long_exchange: str
    short_exchange: str
    execution_mode: str
    opportunity_grade: str
    replay: OpportunityReplayResult


class ReplayPreviewResponse(BaseModel):
    requested_symbols: list[str]
    replay_assumptions: ReplayAssumptions
    preview_count: int
    items: list[ReplayPreviewItem]
    snapshot_errors: list[ExchangeError] = Field(default_factory=list)


class ReplayProfileComparisonResult(BaseModel):
    profile_name: str
    resolved_execution_extended_size_up_enabled: bool
    resolved_execution_target_leverage: float
    resolved_execution_max_allowed_leverage: float
    resolved_execution_required_liquidation_buffer_pct: float
    extended_size_up_execution_ready: bool
    extended_size_up_execution_blockers: list[str] = Field(default_factory=list)
    why_not_explainability: "ProfileWhyNotExplainability"
    execution_max_single_cap_pct: float = 0.0
    execution_cap_reasons: list[str] = Field(default_factory=list)
    replay: OpportunityReplayResult


class ProfileWhyNotExplainability(BaseModel):
    opportunity_blockers: list[str] = Field(default_factory=list)
    profile_policy_blockers: list[str] = Field(default_factory=list)
    execution_capacity_blockers: list[str] = Field(default_factory=list)


class ReplayProfileCompareItem(BaseModel):
    cluster_id: str | None = None
    route_rank: int | None = None
    symbol: str
    long_exchange: str
    short_exchange: str
    execution_mode: str
    opportunity_grade: str
    normal_blockers: list[str] = Field(default_factory=list)
    normal_promotion_reasons: list[str] = Field(default_factory=list)
    size_up_blockers: list[str] = Field(default_factory=list)
    size_up_promotion_reasons: list[str] = Field(default_factory=list)
    extended_size_up_risk_eligible: bool = False
    extended_size_up_risk_blockers: list[str] = Field(default_factory=list)
    profile_results: list[ReplayProfileComparisonResult]


class ReplayProfileCompareResponse(BaseModel):
    requested_symbols: list[str]
    replay_assumptions: ReplayAssumptions
    compared_profiles: list[str]
    compare_count: int
    account_state_applied: bool = False
    items: list[ReplayProfileCompareItem]
    snapshot_errors: list[ExchangeError] = Field(default_factory=list)
