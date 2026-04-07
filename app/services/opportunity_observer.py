from dataclasses import dataclass

from app.models.market import MarketSnapshot, Opportunity, ReplayAssumptions
from app.models.observation import ObservationRecord, ObserveRunSummary
from app.services.opportunity_replay import OpportunityReplayResult, OpportunityReplayService


@dataclass(frozen=True)
class OpportunityObservationContext:
    opportunity: Opportunity
    why_not_tradable: str
    replay_net_after_cost_bps: float | None
    replay_confidence_label: str | None
    replay_passes_min_trade_gate: bool | None
    replay_summary: str | None


class OpportunityObserverService:
    def __init__(self) -> None:
        self._replay_service = OpportunityReplayService()

    def build_observation_contexts(
        self,
        opportunities: list[Opportunity],
        snapshots: list[MarketSnapshot],
    ) -> list[OpportunityObservationContext]:
        snapshot_lookup = {(item.base_symbol.upper(), item.exchange.lower()): item for item in snapshots}
        assumptions = ReplayAssumptions(
            holding_mode="to_next_funding",
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
        contexts: list[OpportunityObservationContext] = []
        for opportunity in opportunities:
            long_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.long_exchange.lower()))
            short_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.short_exchange.lower()))
            replay: OpportunityReplayResult | None = None
            if long_snapshot is not None and short_snapshot is not None:
                replay = self._replay_service.replay(opportunity, long_snapshot, short_snapshot, assumptions)
            replay_net = replay.net_realized_edge_bps if replay else None
            replay_gate = self._replay_passes_min_trade_gate(opportunity, replay_net) if replay else None
            contexts.append(
                OpportunityObservationContext(
                    opportunity=opportunity,
                    why_not_tradable=self._why_not_tradable_label(opportunity, replay_net, replay_gate),
                    replay_net_after_cost_bps=replay_net,
                    replay_confidence_label=(replay.replay_confidence_label if replay else None),
                    replay_passes_min_trade_gate=replay_gate,
                    replay_summary=self._replay_summary(replay),
                )
            )
        return contexts

    def select_top_opportunities(
        self,
        contexts: list[OpportunityObservationContext],
        *,
        max_global: int = 10,
        max_per_symbol: int = 3,
    ) -> list[OpportunityObservationContext]:
        kept = [item for item in contexts if not self._is_low_value_noise(item)]
        ranked = sorted(
            kept,
            key=self.score_observation_candidate,
            reverse=True,
        )
        selected: list[OpportunityObservationContext] = []
        per_symbol_count: dict[str, int] = {}
        for item in ranked:
            symbol = item.opportunity.symbol
            if per_symbol_count.get(symbol, 0) >= max_per_symbol:
                continue
            selected.append(item)
            per_symbol_count[symbol] = per_symbol_count.get(symbol, 0) + 1
            if len(selected) >= max_global:
                break
        return selected

    def to_observation_records(
        self,
        contexts: list[OpportunityObservationContext],
        observed_at_ms: int,
    ) -> list[ObservationRecord]:
        records: list[ObservationRecord] = []
        for item in contexts:
            opportunity = item.opportunity
            route_id = opportunity.cluster_id or f"{opportunity.symbol}:{opportunity.long_exchange}->{opportunity.short_exchange}"
            records.append(
                ObservationRecord(
                    observed_at_ms=observed_at_ms,
                    symbol=opportunity.symbol,
                    cluster_id=route_id,
                    long_exchange=opportunity.long_exchange,
                    short_exchange=opportunity.short_exchange,
                    estimated_net_edge_bps=opportunity.net_edge_bps,
                    opportunity_grade=opportunity.opportunity_grade,
                    execution_mode=opportunity.execution_mode,
                    final_position_pct=opportunity.final_position_pct,
                    why_not_tradable=item.why_not_tradable,
                    replay_net_after_cost_bps=item.replay_net_after_cost_bps,
                    replay_confidence_label=item.replay_confidence_label,
                    replay_passes_min_trade_gate=item.replay_passes_min_trade_gate,
                    risk_flags=opportunity.risk_flags,
                    replay_summary=item.replay_summary,
                    raw_opportunity_json=opportunity.model_dump(),
                )
            )
        return records

    def build_summary(
        self,
        *,
        evaluated_count: int,
        records: list[ObservationRecord],
    ) -> ObserveRunSummary:
        routes = [f"{item.symbol}:{item.long_exchange}->{item.short_exchange}" for item in records]
        return ObserveRunSummary(
            evaluated_count=evaluated_count,
            stored_count=len(records),
            stored_routes=routes,
            stored_symbols=sorted({item.symbol for item in records}),
        )

    @staticmethod
    def score_observation_candidate(item: OpportunityObservationContext) -> tuple:
        opportunity = item.opportunity
        execution_priority = {
            "extended_size_up": 4,
            "size_up": 4,
            "normal": 3,
            "small_probe": 2,
            "paper": 1,
        }
        replay_net = item.replay_net_after_cost_bps if item.replay_net_after_cost_bps is not None else -999.0
        research_value = OpportunityObserverService._has_research_value(item)
        return (
            execution_priority.get(opportunity.execution_mode, 0),
            bool(item.replay_passes_min_trade_gate),
            opportunity.net_edge_bps,
            replay_net,
            research_value,
            opportunity.final_position_pct,
            opportunity.symbol,
            opportunity.long_exchange,
            opportunity.short_exchange,
        )

    @staticmethod
    def _is_low_value_noise(item: OpportunityObservationContext) -> bool:
        opportunity = item.opportunity
        replay_net = item.replay_net_after_cost_bps if item.replay_net_after_cost_bps is not None else -999.0
        weak_replay = replay_net < 2.0
        weak_edge = opportunity.net_edge_bps < 4.0
        is_paper_zero = opportunity.execution_mode == "paper" and opportunity.final_position_pct == 0.0
        return is_paper_zero and weak_replay and weak_edge and not OpportunityObserverService._has_research_value(item)

    @staticmethod
    def _has_research_value(item: OpportunityObservationContext) -> bool:
        opportunity = item.opportunity
        risk_flags = set(opportunity.risk_flags)
        if {"mixed_funding_sources", "different_funding_periods"} & risk_flags:
            return True
        if opportunity.data_quality_status != "healthy":
            return True
        if opportunity.funding_confidence_label in {"low", "very_low"} and opportunity.net_edge_bps >= 8.0:
            return True
        if opportunity.opportunity_grade == "watchlist" and opportunity.net_edge_bps >= 8.0:
            return True
        if item.replay_net_after_cost_bps is not None and item.replay_net_after_cost_bps >= 4.0:
            return True
        return False

    @staticmethod
    def _replay_passes_min_trade_gate(opportunity: Opportunity, replay_net_after_cost_bps: float | None) -> bool:
        if replay_net_after_cost_bps is None:
            return False
        min_gate_bps = 6.0 if opportunity.execution_mode in {"small_probe", "paper"} else opportunity.normal_required_edge_bps
        return replay_net_after_cost_bps >= min_gate_bps

    @staticmethod
    def _why_not_tradable_label(
        opportunity: Opportunity,
        replay_net_after_cost_bps: float | None,
        replay_passes_min_trade_gate: bool | None,
    ) -> str:
        risk_flags = set(opportunity.risk_flags)
        if "mixed_funding_sources" in risk_flags:
            return "mixed funding semantics"
        if "different_funding_periods" in risk_flags:
            return "funding period mismatch"
        if opportunity.data_quality_status != "healthy":
            return "quality gate downgraded opportunity"
        if replay_passes_min_trade_gate is False:
            return "replay edge too weak after costs"
        if opportunity.execution_mode == "paper":
            return "paper-only due to risk flags" if risk_flags else "insufficient confidence for live sizing"
        if opportunity.execution_mode == "small_probe":
            return "small probe only"
        if replay_net_after_cost_bps is not None and replay_net_after_cost_bps > 0:
            return "live candidate"
        return ""

    @staticmethod
    def _replay_summary(replay: OpportunityReplayResult | None) -> str | None:
        if replay is None:
            return None
        return (
            f"net={replay.net_realized_edge_bps:.2f}bps "
            f"fees={replay.fees_bps:.2f}bps "
            f"slippage={replay.slippage_bps:.2f}bps "
            f"confidence={replay.replay_confidence_label}"
        )
