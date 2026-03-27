from pydantic import BaseModel, Field

from app.models.market import MarketSnapshot


class SnapshotQualityReport(BaseModel):
    exchange: str | None = None
    symbol: str | None = None
    normalized_symbol: str | None = None
    timestamp_ms: int | None = None
    quality_score: float
    quality_status: str
    quality_flags: list[str] = Field(default_factory=list)
    quality_blockers: list[str] = Field(default_factory=list)
    quality_warnings: list[str] = Field(default_factory=list)
    can_enter_scanner: bool
    watchlist_only: bool
    freshness_ok: bool
    derived_checks_ok: bool
    exchange_rule_checks_ok: bool
    cross_exchange_checks_ok: bool


class DataQualityGateResult(BaseModel):
    accepted_snapshots: list[MarketSnapshot] = Field(default_factory=list)
    rejected_snapshots: list[MarketSnapshot] = Field(default_factory=list)
    snapshot_reports: list[SnapshotQualityReport] = Field(default_factory=list)
    collection_status: str
    collection_flags: list[str] = Field(default_factory=list)
    total_snapshots: int
    accepted_count: int
    rejected_count: int
    healthy_count: int
    degraded_count: int
    suspicious_count: int
    invalid_count: int
