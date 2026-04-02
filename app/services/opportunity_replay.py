from __future__ import annotations

from dataclasses import dataclass

from app.models.market import (
    MarketSnapshot,
    Opportunity,
    OpportunityReplayResult,
    ReplayAssumptions,
    ReplayResearchMetrics,
)

MINUTES_PER_HOUR = 60
BPS_MULTIPLIER = 10_000
MIN_SAFE_EDGE_DENOMINATOR_BPS = 0.1


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
        replay_cost_drag_bps = (
            fees_bps + slippage_bps + assumptions.latency_decay_bps + assumptions.borrow_or_misc_cost_bps
        )
        gross_price_edge_bps = opportunity.price_spread_bps
        net_realized_edge_bps = (
            gross_price_edge_bps
            + realized_funding_bps
            - replay_cost_drag_bps
        )
        edge_retention_rate = self._safe_ratio(net_realized_edge_bps, opportunity.net_edge_bps)
        funding_capture_rate = self._safe_ratio(realized_funding_bps, opportunity.expected_funding_edge_bps)

        return OpportunityReplayResult(
            symbol=opportunity.symbol,
            long_exchange=opportunity.long_exchange,
            short_exchange=opportunity.short_exchange,
            entry_price_edge_bps=opportunity.price_spread_bps,
            entry_expected_funding_edge_bps=opportunity.expected_funding_edge_bps,
            entry_net_edge_bps=opportunity.net_edge_bps,
            gross_price_edge_bps=gross_price_edge_bps,
            realized_funding_bps=realized_funding_bps,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            latency_decay_bps=assumptions.latency_decay_bps,
            borrow_or_misc_cost_bps=assumptions.borrow_or_misc_cost_bps,
            net_realized_edge_bps=net_realized_edge_bps,
            holding_minutes=funding_window.holding_minutes,
            long_funding_capture_fraction=long_fraction,
            short_funding_capture_fraction=short_fraction,
            pair_funding_capture_fraction=min(long_fraction, short_fraction),
            replay_confidence_label=funding_window.confidence_label,
            research_metrics=ReplayResearchMetrics(
                edge_retention_rate=edge_retention_rate,
                funding_capture_rate=funding_capture_rate,
                replay_cost_drag_bps=replay_cost_drag_bps,
                research_confidence_score=self._research_confidence_score(
                    replay_confidence_label=funding_window.confidence_label,
                    edge_retention_rate=edge_retention_rate,
                    pair_funding_capture_fraction=min(long_fraction, short_fraction),
                    replay_cost_drag_bps=replay_cost_drag_bps,
                    entry_net_edge_bps=opportunity.net_edge_bps,
                ),
            ),
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

    @staticmethod
    def _safe_ratio(numerator: float, denominator: float) -> float | None:
        if denominator <= MIN_SAFE_EDGE_DENOMINATOR_BPS:
            return None
        return numerator / denominator

    def _research_confidence_score(
        self,
        *,
        replay_confidence_label: str,
        edge_retention_rate: float | None,
        pair_funding_capture_fraction: float,
        replay_cost_drag_bps: float,
        entry_net_edge_bps: float,
    ) -> float:
        label_component = {"high": 0.90, "medium": 0.65, "low": 0.35}.get(replay_confidence_label, 0.35)
        edge_component = 0.50 if edge_retention_rate is None else self._clamp(edge_retention_rate, 0.0, 1.0)
        funding_component = self._clamp(pair_funding_capture_fraction, 0.0, 1.0)

        if entry_net_edge_bps <= MIN_SAFE_EDGE_DENOMINATOR_BPS:
            cost_component = 0.50
        else:
            cost_pressure = replay_cost_drag_bps / entry_net_edge_bps
            cost_component = 1.0 - self._clamp(cost_pressure / 2.0, 0.0, 1.0)

        score = (
            0.35 * label_component
            + 0.30 * edge_component
            + 0.20 * funding_component
            + 0.15 * cost_component
        )
        return round(self._clamp(score, 0.0, 1.0), 4)

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return min(max(value, lower), upper)
