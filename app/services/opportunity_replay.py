from __future__ import annotations

from dataclasses import dataclass

from app.models.market import MarketSnapshot, Opportunity, OpportunityReplayResult, ReplayAssumptions

MINUTES_PER_HOUR = 60
BPS_MULTIPLIER = 10_000


@dataclass(frozen=True)
class _FundingWindowEstimate:
    holding_minutes: int
    confidence_label: str


class OpportunityReplayService:
    """Deterministic, conservative post-detection replay for research workflows."""

    def replay(
        self,
        opportunity: Opportunity,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
        assumptions: ReplayAssumptions,
    ) -> OpportunityReplayResult:
        funding_window = self._resolve_funding_window_minutes(assumptions, long_snapshot, short_snapshot)

        long_fraction = self._funding_capture_fraction(long_snapshot, funding_window.holding_minutes)
        short_fraction = self._funding_capture_fraction(short_snapshot, funding_window.holding_minutes)

        long_funding_bps = self._funding_bps(long_snapshot) * long_fraction
        short_funding_bps = self._funding_bps(short_snapshot) * short_fraction
        realized_funding_bps = short_funding_bps - long_funding_bps

        slippage_bps = 2.0 * (assumptions.slippage_bps_per_leg + assumptions.extra_exit_slippage_bps_per_leg)
        fees_bps = opportunity.estimated_fee_bps
        gross_price_edge_bps = opportunity.price_spread_bps
        net_realized_edge_bps = (
            gross_price_edge_bps
            + realized_funding_bps
            - fees_bps
            - slippage_bps
            - assumptions.latency_decay_bps
            - assumptions.borrow_or_misc_cost_bps
        )

        return OpportunityReplayResult(
            symbol=opportunity.symbol,
            long_exchange=opportunity.long_exchange,
            short_exchange=opportunity.short_exchange,
            entry_edge_bps=opportunity.price_spread_bps,
            gross_price_edge_bps=gross_price_edge_bps,
            realized_funding_bps=realized_funding_bps,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            latency_decay_bps=assumptions.latency_decay_bps,
            borrow_or_misc_cost_bps=assumptions.borrow_or_misc_cost_bps,
            net_realized_edge_bps=net_realized_edge_bps,
            holding_minutes=funding_window.holding_minutes,
            funding_capture_fraction=min(long_fraction, short_fraction),
            replay_confidence_label=funding_window.confidence_label,
        )

    def _resolve_funding_window_minutes(
        self,
        assumptions: ReplayAssumptions,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
    ) -> _FundingWindowEstimate:
        if assumptions.holding_mode == "fixed_minutes":
            holding_minutes = assumptions.holding_minutes or 0
            return _FundingWindowEstimate(holding_minutes=max(holding_minutes, 0), confidence_label="high")

        candidates = [
            self._minutes_to_next_funding(long_snapshot),
            self._minutes_to_next_funding(short_snapshot),
        ]
        known_candidates = [value for value in candidates if value is not None and value >= 0]
        if known_candidates:
            return _FundingWindowEstimate(holding_minutes=min(known_candidates), confidence_label="high")

        fallback_candidates = [
            self._period_minutes(long_snapshot),
            self._period_minutes(short_snapshot),
        ]
        known_periods = [value for value in fallback_candidates if value is not None]
        if known_periods:
            return _FundingWindowEstimate(holding_minutes=min(known_periods), confidence_label="medium")

        return _FundingWindowEstimate(holding_minutes=0, confidence_label="low")

    @staticmethod
    def _period_minutes(snapshot: MarketSnapshot) -> int | None:
        if snapshot.funding_period_hours in (None, 0):
            return None
        return snapshot.funding_period_hours * MINUTES_PER_HOUR

    def _minutes_to_next_funding(self, snapshot: MarketSnapshot) -> int | None:
        if snapshot.next_funding_time_ms is not None:
            delta_ms = snapshot.next_funding_time_ms - snapshot.timestamp_ms
            if delta_ms <= 0:
                return 0
            return int(delta_ms // 60_000)

        period_minutes = self._period_minutes(snapshot)
        if period_minutes is None or snapshot.funding_time_ms is None:
            return None

        delta_ms = (snapshot.funding_time_ms + period_minutes * 60_000) - snapshot.timestamp_ms
        if delta_ms <= 0:
            return 0
        return int(delta_ms // 60_000)

    def _funding_capture_fraction(self, snapshot: MarketSnapshot, holding_minutes: int) -> float:
        period_minutes = self._period_minutes(snapshot)
        if period_minutes in (None, 0) or holding_minutes <= 0:
            return 0.0
        return min(max(holding_minutes / period_minutes, 0.0), 1.0)

    @staticmethod
    def _funding_bps(snapshot: MarketSnapshot) -> float:
        if snapshot.funding_rate is None:
            return 0.0
        return snapshot.funding_rate * BPS_MULTIPLIER
