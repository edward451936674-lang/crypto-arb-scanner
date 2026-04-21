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


class ExecutionAdapterCapability(BaseModel):
    venue_id: str
    supports_live_submit_now: bool = False
    supports_cancel_now: bool = False
    supports_order_status_now: bool = False
    credential_type: str = "none"
    sandbox_or_testnet_supported: bool = False
    stub_only: bool = True


class CredentialReadinessSignal(BaseModel):
    venue_id: str
    credential_type: str
    status: Literal["missing", "present", "malformed"]
    reasons: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


ExecutionPreflightStatus = Literal["ready", "blocked"]
ExecutionPolicyStatus = Literal["allowed", "blocked"]
ExecutionAccountStateStatus = Literal["allowed", "blocked"]
ExecutionCredentialReadinessStatus = Literal["allowed", "blocked"]
ExecutionAccountStateBlockReason = Literal[
    "execution_account_state_disabled",
    "target_notional_missing",
    "global_capacity_missing",
    "symbol_capacity_missing",
    "long_exchange_capacity_missing",
    "short_exchange_capacity_missing",
    "target_notional_exceeds_global_capacity",
    "target_notional_exceeds_symbol_capacity",
    "target_notional_exceeds_long_exchange_capacity",
    "target_notional_exceeds_short_exchange_capacity",
]
ExecutionPolicyBlockReason = Literal[
    "execution_globally_disabled",
    "preflight_not_ready",
    "test_bundle_not_allowed",
    "long_venue_not_allowed",
    "short_venue_not_allowed",
    "symbol_not_allowed",
    "symbol_explicitly_blocked",
    "target_notional_missing",
    "target_notional_exceeds_limit",
]
ExecutionCredentialReadinessBlockReason = Literal[
    "credential_readiness_disabled",
    "long_credentials_missing",
    "short_credentials_missing",
    "long_credentials_status_unknown",
    "short_credentials_status_unknown",
    "unsupported_credential_fixture",
]
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


class ExecutionPolicyConfigSnapshot(BaseModel):
    execution_enabled: bool = False
    allow_test_execution: bool = False
    allowed_venues: list[str] = Field(default_factory=list)
    allowed_symbols: list[str] = Field(default_factory=list)
    blocked_symbols: list[str] = Field(default_factory=list)
    max_target_notional_usd: float | None = None


class ExecutionAccountStateSnapshot(BaseModel):
    execution_account_state_enabled: bool = False
    execution_account_state_fixture_total_notional_usd: float | None = None
    execution_account_state_fixture_remaining_total_notional_usd: float | None = None
    execution_account_state_fixture_remaining_symbol_notional_usd: dict[str, float] = Field(default_factory=dict)
    execution_account_state_fixture_remaining_long_exchange_notional_usd: dict[str, float] = Field(default_factory=dict)
    execution_account_state_fixture_remaining_short_exchange_notional_usd: dict[str, float] = Field(default_factory=dict)


class ExecutionAccountStateDecision(BaseModel):
    route_key: str
    symbol: str
    long_exchange: str
    short_exchange: str
    target_notional_usd: float | None = None
    account_state_status: ExecutionAccountStateStatus = "blocked"
    allowed: bool = False
    block_reasons: list[ExecutionAccountStateBlockReason] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    remaining_global_notional_usd: float | None = None
    remaining_symbol_notional_usd: float | None = None
    remaining_long_exchange_notional_usd: float | None = None
    remaining_short_exchange_notional_usd: float | None = None
    preview_only: bool = True
    is_live: bool = False


class ExecutionCredentialReadinessConfigSnapshot(BaseModel):
    execution_credential_readiness_enabled: bool = False
    execution_credential_fixture_configured_venues: dict[str, bool] = Field(default_factory=dict)


class ExecutionCredentialReadinessDecision(BaseModel):
    route_key: str
    symbol: str
    long_exchange: str
    short_exchange: str
    credential_readiness_status: ExecutionCredentialReadinessStatus = "blocked"
    allowed: bool = False
    block_reasons: list[ExecutionCredentialReadinessBlockReason] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    long_credentials_configured: bool | None = None
    short_credentials_configured: bool | None = None
    preview_only: bool = True
    is_live: bool = False



LiveExecutionEntryStatus = Literal["allowed", "blocked"]
LiveExecutionEntryBlockReason = Literal[
    "live_execution_not_enabled",
    "policy_blocked",
    "venue_not_live_enabled",
    "adapter_is_stub_only",
    "credential_readiness_blocked",
    "unsupported_live_execution_path",
]


class LiveExecutionEntryResult(BaseModel):
    route_key: str
    symbol: str
    long_exchange: str
    short_exchange: str
    policy_status: ExecutionPolicyStatus = "blocked"
    entry_status: LiveExecutionEntryStatus = "blocked"
    allowed_to_enter_live_path: bool = False
    block_reasons: list[LiveExecutionEntryBlockReason] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    preview_only: bool = True
    is_live: bool = False


class LiveExecutionEntryConfigSnapshot(BaseModel):
    live_execution_enabled: bool = False
    live_execution_allowed_venues: list[str] = Field(default_factory=list)


class ExecutionPolicyDecision(BaseModel):
    route_key: str
    symbol: str
    long_exchange: str
    short_exchange: str
    bundle_status_from_preflight: ExecutionPreflightStatus
    policy_status: ExecutionPolicyStatus = "blocked"
    allowed: bool = False
    block_reasons: list[ExecutionPolicyBlockReason] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    preview_only: bool = True
    is_live: bool = False


DryRunExecutionStatus = Literal["accepted", "blocked", "failed"]
DryRunExecutionFailureReason = Literal[
    "preflight_blocked",
    "long_leg_submit_rejected",
    "short_leg_submit_rejected",
    "unsupported_venue",
    "missing_route_key",
    "quantity_unresolved",
    "validation_error",
]


class DryRunExecutionLegAttempt(BaseModel):
    venue_id: str
    side: Literal["buy", "sell"]
    symbol: str
    route_key: str
    leg_index: Literal[0, 1]
    submit_sequence: Literal[1, 2]
    submit_order: Literal["first", "second"]
    quantity: float | None = None
    request_preview: VenueRequestPreview | None = None
    submit_status: Literal["accepted", "rejected", "skipped"] = "skipped"
    submit_message: str | None = None
    accepted: bool = False
    supported_venue: bool = False
    validation_errors: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)


class DryRunExecutionAttempt(BaseModel):
    attempt_id: str
    route_key: str
    symbol: str
    long_leg: DryRunExecutionLegAttempt
    short_leg: DryRunExecutionLegAttempt
    bundle_status: DryRunExecutionStatus = "blocked"
    failure_reasons: list[DryRunExecutionFailureReason] = Field(default_factory=list)
    submitted_leg_count: int = 0
    accepted_leg_count: int = 0
    preview_only: bool = True
    is_live: bool = False
    created_at_ms: int


LiveSubmitStatus = Literal["blocked", "armed", "submitted", "failed"]
LiveSubmitBlockReason = Literal[
    "preflight_blocked",
    "policy_blocked",
    "account_state_blocked",
    "credential_readiness_blocked",
    "live_entry_blocked",
    "guarded_live_submit_disabled",
    "arm_token_required",
    "arm_token_mismatch",
    "unsupported_live_submit_path",
    "no_live_adapter_implemented",
    "mixed_live_venue_path_not_supported_yet",
    "second_leg_live_adapter_not_implemented",
    "cancel_not_supported_for_route",
    "order_status_not_supported_for_route",
    "live_adapter_submit_failed",
]


class LiveSubmitConfigSnapshot(BaseModel):
    guarded_live_submit_enabled: bool = False
    guarded_live_submit_require_arm_token: bool = True
    guarded_live_submit_arm_token: str = ""
    guarded_live_submit_persist_attempts: bool = True


class LiveSubmitLegAttempt(BaseModel):
    venue_id: str
    side: Literal["buy", "sell"]
    symbol: str
    route_key: str
    quantity: float | None = None
    leg_index: Literal[0, 1]
    submit_sequence: Literal[1, 2]
    submit_order: Literal["first", "second"]
    supported_venue: bool = False
    attempted_live_submit: bool = False
    submit_status: LiveSubmitStatus = "blocked"
    submit_message: str | None = None
    accepted: bool = False
    block_reasons: list[str] = Field(default_factory=list)
    validation_errors: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    final_quantity: str | None = None
    final_price: str | None = None
    final_client_order_id: str | None = None
    normalization_applied: bool = False


class LiveSubmitAttempt(BaseModel):
    attempt_id: str
    route_key: str
    symbol: str
    long_leg: LiveSubmitLegAttempt
    short_leg: LiveSubmitLegAttempt
    live_submit_status: LiveSubmitStatus = "blocked"
    block_reasons: list[LiveSubmitBlockReason] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    submitted_leg_count: int = 0
    accepted_leg_count: int = 0
    real_adapter_path_attempted: bool = False
    preview_only: bool = True
    is_live: bool = False
    created_at_ms: int
