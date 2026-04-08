from typing import Any

from pydantic import BaseModel, Field


class ObservationRecord(BaseModel):
    id: int | None = None
    observed_at_ms: int
    symbol: str
    cluster_id: str
    long_exchange: str
    short_exchange: str
    price_spread_bps: float | None = None
    funding_spread_bps: float | None = None
    risk_adjusted_edge_bps: float | None = None
    estimated_net_edge_bps: float | None = None
    opportunity_grade: str | None = None
    execution_mode: str | None = None
    final_position_pct: float | None = None
    why_not_tradable: str | None = None
    replay_net_after_cost_bps: float | None = None
    replay_confidence_label: str | None = None
    replay_passes_min_trade_gate: bool | None = None
    risk_flags: list[str] = Field(default_factory=list)
    replay_summary: str | None = None
    raw_opportunity_json: dict[str, Any] = Field(default_factory=dict)


class ObserveRunSummary(BaseModel):
    evaluated_count: int
    stored_count: int
    stored_routes: list[str] = Field(default_factory=list)
    stored_symbols: list[str] = Field(default_factory=list)
