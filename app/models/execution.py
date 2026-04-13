from typing import Any, Literal

from pydantic import BaseModel, Field


class ExecutionPlan(BaseModel):
    target_position_pct: float | None = None
    target_notional_usd: float | None = None
    max_slippage_bps: float | None = None
    max_order_age_ms: int | None = None


class ExecutionCandidate(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    route_key: str
    opportunity_type: str | None = None
    execution_mode: str | None = None
    expected_edge_bps: float | None = None
    replay_net_after_cost_bps: float | None = None
    risk_adjusted_edge_bps: float | None = None
    target_position_pct: float | None = None
    target_notional_usd: float | None = None
    max_slippage_bps: float | None = None
    max_order_age_ms: int | None = None
    is_executable_now: bool = False
    why_not_executable: str | None = None
    replay_confidence_label: str | None = None
    replay_passes_min_trade_gate: bool | None = None
    risk_flags: list[str] = Field(default_factory=list)
    entry_reference_price_long: float | None = None
    entry_reference_price_short: float | None = None
    generated_at_ms: int
    is_test: bool = False


class QuantityResolutionResult(BaseModel):
    resolved_quantity_long: float | None = None
    resolved_quantity_short: float | None = None
    quantity_resolution_status: Literal["resolved", "partial", "unavailable"] = "unavailable"
    quantity_resolution_source: Literal[
        "target_notional_and_reference_price",
        "target_position_pct_only",
        "unavailable",
    ] = "unavailable"
    warnings: list[str] = Field(default_factory=list)
    notes: str | None = None


class PaperExecutionRecord(BaseModel):
    id: int | None = None
    created_at_ms: int
    symbol: str
    long_exchange: str
    short_exchange: str
    route_key: str
    opportunity_type: str | None = None
    execution_mode: str | None = None
    target_position_pct: float | None = None
    target_notional_usd: float | None = None
    expected_edge_bps: float | None = None
    replay_net_after_cost_bps: float | None = None
    risk_adjusted_edge_bps: float | None = None
    is_executable_now: bool
    why_not_executable: str | None = None
    replay_confidence_label: str | None = None
    replay_passes_min_trade_gate: bool | None = None
    risk_flags: list[str] = Field(default_factory=list)
    status: Literal["planned", "expired", "still_valid", "invalidated"] = "planned"
    status_updated_at_ms: int
    expires_at_ms: int
    evaluation_due_at_ms: int
    closed_at_ms: int | None = None
    closure_reason: str | None = None
    latest_observed_edge_bps: float | None = None
    latest_replay_net_after_cost_bps: float | None = None
    latest_risk_adjusted_edge_bps: float | None = None
    entry_reference_price_long: float | None = None
    entry_reference_price_short: float | None = None
    latest_reference_price_long: float | None = None
    latest_reference_price_short: float | None = None
    paper_pnl_bps: float | None = None
    paper_pnl_usd: float | None = None
    outcome_status: Literal["unknown", "flat", "positive", "negative"] = "unknown"
    outcome_updated_at_ms: int
    raw_execution_json: dict[str, Any] = Field(default_factory=dict)


class OrderIntent(BaseModel):
    venue_id: str
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"] | None = None
    quantity: float | None = None
    price: float | None = None
    time_in_force: Literal["gtc", "ioc", "fok", "post_only"] | None = None
    reduce_only: bool | None = None
    client_order_id: str | None = None
    route_key: str | None = None
    target_position_pct: float | None = None
    target_notional_usd: float | None = None
    max_slippage_bps: float | None = None
    max_order_age_ms: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class CancelIntent(BaseModel):
    venue_id: str
    order_id: str | None = None
    client_order_id: str | None = None
    symbol: str | None = None
    route_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class OrderStatusSnapshot(BaseModel):
    venue_id: str
    order_id: str | None = None
    client_order_id: str | None = None
    symbol: str | None = None
    side: Literal["buy", "sell"] | None = None
    order_type: Literal["market", "limit"] | None = None
    status: Literal["accepted", "open", "partially_filled", "filled", "cancelled", "rejected", "unknown"] = "unknown"
    quantity: float | None = None
    filled_qty: float | None = None
    remaining_qty: float | None = None
    average_fill_price: float | None = None
    reduce_only: bool | None = None
    time_in_force: Literal["gtc", "ioc", "fok", "post_only"] | None = None
    route_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class PositionSnapshot(BaseModel):
    venue_id: str
    symbol: str
    size: float | None = None
    entry_price: float | None = None
    mark_price: float | None = None
    unrealized_pnl: float | None = None
    leverage: float | None = None
    route_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class BalanceSnapshot(BaseModel):
    venue_id: str
    asset: str
    free: float | None = None
    locked: float | None = None
    equity: float | None = None
    available: float | None = None
    route_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class VenueRequestPreview(BaseModel):
    venue_id: str
    operation: Literal["place_order", "cancel_order"]
    route_key: str | None = None
    intent_ref: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    validation_errors: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    is_live: bool = False


class VenueTranslationResult(BaseModel):
    venue_id: str
    operation: Literal["place_order", "cancel_order"]
    normalized_intent_id: str | None = None
    route_key: str | None = None
    symbol: str | None = None
    preview: VenueRequestPreview
    accepted: bool
    notes: str | None = None
    is_live: bool = False


class AdapterExecutionResult(BaseModel):
    venue_id: str
    operation: Literal["place_order", "cancel_order", "get_order_status", "get_position", "get_balance"]
    accepted: bool
    message: str | None = None
    order_status: OrderStatusSnapshot | None = None
    position: PositionSnapshot | None = None
    balance: BalanceSnapshot | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    translation: VenueTranslationResult | None = None
    is_live: bool = False


ExecutionPreflightStatus = Literal["ready", "blocked"]
ExecutionPreflightBlocker = Literal[
    "long_quantity_unresolved",
    "short_quantity_unresolved",
    "long_validation_error",
    "short_validation_error",
    "unsupported_venue",
    "missing_route_key",
]


class ExecutionLegPreflight(BaseModel):
    venue_id: str
    side: Literal["buy", "sell"]
    symbol: str
    route_key: str
    quantity: float | None = None
    quantity_resolution_status: Literal["resolved", "partial", "unavailable"] = "unavailable"
    request_preview_available: bool = False
    validation_errors: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    supported_venue: bool = False
    is_ready: bool = False


class ExecutionBundlePreflight(BaseModel):
    route_key: str
    symbol: str
    long_leg: ExecutionLegPreflight
    short_leg: ExecutionLegPreflight
    bundle_status: ExecutionPreflightStatus = "blocked"
    blockers: list[ExecutionPreflightBlocker] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    is_executable_bundle: bool = False
    preview_only: bool = True
    is_live: bool = False
