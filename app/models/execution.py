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
    generated_at_ms: int
    is_test: bool = False


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
    raw_execution_json: dict[str, Any] = Field(default_factory=dict)
