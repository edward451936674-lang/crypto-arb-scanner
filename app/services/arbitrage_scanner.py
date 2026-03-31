from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations

from app.core.symbols import supported_symbols
from app.models.market import MarketSnapshot, Opportunity

MAJOR_SYMBOL_ALLOWLIST = [
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "MATIC",
    "LTC",
    "DOT",
    "TRX",
    "ATOM",
    "APT",
    "ARB",
    "OP",
    "NEAR",
    "FIL",
    "SUI",
]

MIN_PRICE_SPREAD_BPS = 5.0
MIN_HOURLY_FUNDING_SPREAD_BPS = 2.0
MAX_ABS_HOURLY_FUNDING_BPS = 5.0
ABNORMAL_ABS_HOURLY_FUNDING_BPS = 3.0
BASE_POSITION_PCT = 0.10
DEFAULT_HOLDING_HOURS = 8
MAX_OPPORTUNITIES_PER_SYMBOL = 3
BPS_MULTIPLIER = 10_000
MAX_TOTAL_POSITION_PCT = 0.20
MAX_SYMBOL_POSITION_PCT = 0.08
MAX_EXCHANGE_POSITION_PCT = 0.10
MAX_SINGLE_OPPORTUNITY_PCT = 0.05
EXECUTION_MODE_PRIORITY = {
    "paper": 0,
    "small_probe": 1,
    "normal": 2,
    "size_up": 3,
}
DATA_QUALITY_SEVERITY = {
    "healthy": 0,
    "degraded": 1,
    "suspicious": 2,
    "invalid": 3,
}
DATA_QUALITY_PENALTY_MULTIPLIER = {
    "healthy": 1.0,
    "degraded": 0.85,
}
BLOCKING_RISK_FLAGS = {
    "mixed_funding_sources",
    "low_confidence_funding",
    "different_funding_periods",
    "abnormal_hourly_funding",
    "low_open_interest",
    "low_quote_volume",
    "missing_liquidity_data",
}
LIQUIDITY_RISK_FLAGS = {
    "missing_liquidity_data",
    "low_open_interest",
    "low_quote_volume",
}
NORMAL_SOFT_RISK_FLAGS = {
    "mixed_funding_sources",
    "different_funding_periods",
    "abnormal_hourly_funding",
    "low_confidence_funding",
}
EXCHANGE_FEE_BPS = {
    "binance": 5.0,
    "okx": 5.0,
    "hyperliquid": 4.0,
    "lighter": 6.0,
}
FUNDING_SOURCE_CONFIDENCE = {
    "current": 0.9,
    "current_8h": 0.9,
    "latest_reported": 0.9,
    "estimated_current": 0.6,
    "last_settled_fallback": 0.5,
}


@dataclass
class OpportunityCandidate:
    opportunity: Opportunity
    long_snapshot: MarketSnapshot
    short_snapshot: MarketSnapshot


@dataclass(frozen=True)
class ExecutionRiskConfig:
    target_leverage: float
    max_allowed_leverage: float
    min_required_liquidation_buffer_pct: float


EXECUTION_RISK_CONFIGS = {
    "small_probe": ExecutionRiskConfig(1.5, 2.0, 10.0),
    "normal": ExecutionRiskConfig(2.0, 3.0, 15.0),
    "size_up": ExecutionRiskConfig(2.0, 2.5, 22.0),
    "extended_size_up": ExecutionRiskConfig(1.5, 2.0, 28.0),
}


class ArbitrageScannerService:
    """Build deterministic, pairwise arbitrage opportunities from market snapshots."""

    def __init__(self) -> None:
        self.allowed_symbols = set(MAJOR_SYMBOL_ALLOWLIST) & set(supported_symbols())

    def build_opportunities(self, snapshots: list[MarketSnapshot]) -> list[Opportunity]:
        grouped: dict[str, list[MarketSnapshot]] = {}
        for snapshot in snapshots:
            symbol = snapshot.base_symbol.upper()
            if symbol not in self.allowed_symbols:
                continue
            grouped.setdefault(snapshot.normalized_symbol, []).append(snapshot)

        candidates: list[OpportunityCandidate] = []
        for symbol_snapshots in grouped.values():
            candidates.extend(self._build_symbol_opportunities(symbol_snapshots))

        annotated_candidates = self._annotate_clusters(candidates)
        opportunities = [candidate.opportunity for candidate in annotated_candidates]
        opportunities.sort(key=self._allocation_sort_key, reverse=True)
        opportunities = self._allocate_portfolio(opportunities)
        return self._limit_opportunities_per_symbol(opportunities)

    def _build_symbol_opportunities(self, snapshots: list[MarketSnapshot]) -> list[OpportunityCandidate]:
        opportunities: list[OpportunityCandidate] = []
        for left, right in combinations(snapshots, 2):
            try:
                opportunity = self._build_pair_opportunity(left, right)
            except Exception:
                continue
            if opportunity is None:
                continue
            opportunities.append(opportunity)
        return opportunities

    def _build_pair_opportunity(
        self,
        left: MarketSnapshot,
        right: MarketSnapshot,
    ) -> OpportunityCandidate | None:
        if left.mark_price is None or right.mark_price is None:
            return None

        long_snapshot, short_snapshot = (left, right) if left.mark_price <= right.mark_price else (right, left)
        long_price = long_snapshot.mark_price
        short_price = short_snapshot.mark_price

        midpoint = (long_price + short_price) / 2
        if midpoint <= 0:
            return None

        price_spread_abs = short_price - long_price
        price_spread_bps = (price_spread_abs / midpoint) * BPS_MULTIPLIER

        long_hourly_rate = long_snapshot.hourly_funding_rate
        short_hourly_rate = short_snapshot.hourly_funding_rate

        funding_rate_diff = self._optional_diff(short_snapshot.funding_rate, long_snapshot.funding_rate)
        funding_spread_bps = self._to_bps(funding_rate_diff)

        hourly_funding_rate_diff = self._optional_diff(short_hourly_rate, long_hourly_rate)
        hourly_funding_spread_bps = self._optional_diff(
            short_snapshot.hourly_funding_rate_bps,
            long_snapshot.hourly_funding_rate_bps,
        )

        estimated_edge_bps = price_spread_bps + (hourly_funding_spread_bps or 0.0)
        if (
            price_spread_bps < MIN_PRICE_SPREAD_BPS
            and abs(hourly_funding_spread_bps or 0.0) < MIN_HOURLY_FUNDING_SPREAD_BPS
        ):
            return None

        holding_hours = DEFAULT_HOLDING_HOURS
        expected_funding_edge_bps = (hourly_funding_spread_bps or 0.0) * holding_hours
        long_fee_bps = EXCHANGE_FEE_BPS.get(long_snapshot.exchange.lower(), 0.0)
        short_fee_bps = EXCHANGE_FEE_BPS.get(short_snapshot.exchange.lower(), 0.0)
        estimated_fee_bps = long_fee_bps + short_fee_bps
        net_edge_bps = price_spread_bps + expected_funding_edge_bps - estimated_fee_bps

        funding_confidence_score = self._funding_confidence_score(long_snapshot, short_snapshot)
        funding_confidence_label = self._funding_confidence_label(funding_confidence_score)
        (
            data_quality_status,
            data_quality_score,
            data_quality_flags,
            data_quality_drivers,
        ) = self._opportunity_data_quality(long_snapshot, short_snapshot)
        data_quality_penalty_multiplier = DATA_QUALITY_PENALTY_MULTIPLIER.get(data_quality_status, 0.85)
        risk_flags = self._risk_flags(
            long_snapshot,
            short_snapshot,
            funding_confidence_score,
            data_quality_status,
            data_quality_flags,
        )
        risk_adjusted_edge_bps = net_edge_bps * funding_confidence_score
        data_quality_adjusted_edge_bps = risk_adjusted_edge_bps * data_quality_penalty_multiplier
        normal_required_edge_bps = self._normal_required_edge_bps()
        size_up_required_edge_bps = self._size_up_required_edge_bps()
        size_up_edge_buffer_bps = data_quality_adjusted_edge_bps - size_up_required_edge_bps
        edge_buffer_bps = data_quality_adjusted_edge_bps - normal_required_edge_bps
        normal_eligibility_score = self._normal_eligibility_score(
            edge_buffer_bps,
            funding_confidence_score,
        )
        is_tradable = risk_adjusted_edge_bps >= 8
        opportunity_grade = self._opportunity_grade(risk_adjusted_edge_bps, is_tradable)
        if opportunity_grade == "discard":
            return None

        reject_reasons = [] if is_tradable else self._reject_reasons(risk_adjusted_edge_bps, risk_flags)
        max_position_pct = self._max_position_pct(opportunity_grade)

        opportunity = Opportunity(
            symbol=long_snapshot.base_symbol,
            long_exchange=long_snapshot.exchange,
            short_exchange=short_snapshot.exchange,
            long_price=long_price,
            short_price=short_price,
            price_spread_abs=price_spread_abs,
            price_spread_bps=price_spread_bps,
            long_funding_rate=long_snapshot.funding_rate,
            short_funding_rate=short_snapshot.funding_rate,
            funding_rate_diff=funding_rate_diff,
            funding_spread_bps=funding_spread_bps,
            long_funding_period_hours=long_snapshot.funding_period_hours,
            short_funding_period_hours=short_snapshot.funding_period_hours,
            long_hourly_funding_rate=long_hourly_rate,
            short_hourly_funding_rate=short_hourly_rate,
            hourly_funding_rate_diff=hourly_funding_rate_diff,
            hourly_funding_spread_bps=hourly_funding_spread_bps,
            estimated_edge_bps=estimated_edge_bps,
            holding_hours=holding_hours,
            expected_funding_edge_bps=expected_funding_edge_bps,
            estimated_fee_bps=estimated_fee_bps,
            net_edge_bps=net_edge_bps,
            funding_confidence_score=funding_confidence_score,
            funding_confidence_label=funding_confidence_label,
            risk_adjusted_edge_bps=risk_adjusted_edge_bps,
            data_quality_status=data_quality_status,
            data_quality_score=data_quality_score,
            data_quality_flags=data_quality_flags,
            data_quality_drivers=data_quality_drivers,
            data_quality_penalty_multiplier=data_quality_penalty_multiplier,
            data_quality_adjusted_edge_bps=data_quality_adjusted_edge_bps,
            normal_required_edge_bps=normal_required_edge_bps,
            size_up_required_edge_bps=size_up_required_edge_bps,
            size_up_edge_buffer_bps=size_up_edge_buffer_bps,
            edge_buffer_bps=edge_buffer_bps,
            normal_eligibility_score=normal_eligibility_score,
            risk_flags=risk_flags,
            opportunity_grade=opportunity_grade,
            is_tradable=is_tradable,
            reject_reasons=reject_reasons,
            position_size_multiplier=funding_confidence_score,
            suggested_position_pct=0.0,
            max_position_pct=max_position_pct,
            execution_mode="paper",
        )
        return OpportunityCandidate(
            opportunity=opportunity,
            long_snapshot=long_snapshot,
            short_snapshot=short_snapshot,
        )

    def _annotate_clusters(self, candidates: list[OpportunityCandidate]) -> list[OpportunityCandidate]:
        clustered: dict[str, list[OpportunityCandidate]] = {}
        for candidate in candidates:
            cluster_id = self._cluster_id(candidate.opportunity)
            candidate.opportunity.cluster_id = cluster_id
            clustered.setdefault(cluster_id, []).append(candidate)

        for cluster_candidates in clustered.values():
            cluster_candidates.sort(key=self._cluster_rank_key)
            for index, candidate in enumerate(cluster_candidates, start=1):
                opportunity = candidate.opportunity
                is_primary_route = index == 1
                opportunity.route_rank = index
                opportunity.is_primary_route = is_primary_route
                conviction_score, conviction_drivers = self._conviction_score(
                    opportunity,
                    candidate.long_snapshot,
                    candidate.short_snapshot,
                    is_primary_route,
                )
                conviction_label = self._conviction_label(conviction_score)
                baseline_suggested_position_pct = self._baseline_suggested_position_pct(
                    opportunity.position_size_multiplier,
                    conviction_label,
                    opportunity.risk_flags,
                    opportunity.max_position_pct,
                )
                (
                    execution_mode,
                    execution_mode_drivers,
                    soft_risk_flag_count,
                    normal_blockers,
                    normal_promotion_reasons,
                    size_up_blockers,
                    size_up_promotion_reasons,
                ) = self._determine_execution_mode(
                    opportunity,
                    conviction_score,
                    baseline_suggested_position_pct,
                )
                size_up_eligible = execution_mode == "size_up"
                execution_risk_config = self._execution_risk_config(execution_mode)
                (
                    extended_size_up_risk_eligible,
                    extended_size_up_risk_blockers,
                ) = self._extended_size_up_risk_assessment(
                    opportunity,
                    execution_mode,
                    soft_risk_flag_count,
                    size_up_blockers,
                    execution_risk_config,
                )
                suggested_position_pct = self._apply_execution_mode_to_suggested_position_pct(
                    baseline_suggested_position_pct,
                    execution_mode,
                    opportunity.max_position_pct,
                )
                suggested_position_pct *= opportunity.data_quality_penalty_multiplier
                suggested_position_pct = self._apply_position_cap(
                    suggested_position_pct,
                    opportunity.max_position_pct,
                )
                candidate.opportunity = opportunity.model_copy(
                    update={
                        "conviction_score": conviction_score,
                        "conviction_label": conviction_label,
                        "conviction_drivers": conviction_drivers,
                        "size_up_eligible": size_up_eligible,
                        "execution_mode": execution_mode,
                        "execution_mode_drivers": execution_mode_drivers,
                        "soft_risk_flag_count": soft_risk_flag_count,
                        "normal_blockers": normal_blockers,
                        "normal_promotion_reasons": normal_promotion_reasons,
                        "size_up_blockers": size_up_blockers,
                        "size_up_promotion_reasons": size_up_promotion_reasons,
                        "suggested_position_pct": suggested_position_pct,
                        "extended_size_up_eligible": False,
                        "configured_target_leverage": execution_risk_config.target_leverage,
                        "configured_max_allowed_leverage": execution_risk_config.max_allowed_leverage,
                        "configured_min_required_liquidation_buffer_pct": (
                            execution_risk_config.min_required_liquidation_buffer_pct
                        ),
                        "extended_size_up_risk_eligible": extended_size_up_risk_eligible,
                        "extended_size_up_risk_blockers": extended_size_up_risk_blockers,
                    }
                )
        return candidates

    @staticmethod
    def _cluster_id(opportunity: Opportunity) -> str:
        return f"{opportunity.symbol}|{opportunity.long_exchange}|funding_capture"

    @staticmethod
    def _cluster_rank_key(candidate: OpportunityCandidate) -> tuple[float, float, int, int, float]:
        opportunity = candidate.opportunity
        return (
            -opportunity.data_quality_adjusted_edge_bps,
            -opportunity.net_edge_bps,
            -opportunity.funding_confidence_score,
            ArbitrageScannerService._missing_liquidity_count(opportunity.risk_flags),
            opportunity.estimated_fee_bps,
            ArbitrageScannerService._blocking_risk_count(opportunity.risk_flags),
        )

    @staticmethod
    def _allocation_sort_key(opportunity: Opportunity) -> tuple[int, bool, float, float, float]:
        return (
            EXECUTION_MODE_PRIORITY.get(opportunity.execution_mode, -1),
            opportunity.is_primary_route,
            opportunity.conviction_score,
            opportunity.data_quality_adjusted_edge_bps,
            opportunity.funding_confidence_score,
        )

    def _allocate_portfolio(self, opportunities: list[Opportunity]) -> list[Opportunity]:
        total_used = 0.0
        symbol_used: dict[str, float] = {}
        exchange_used: dict[str, float] = {}
        allocated: list[Opportunity] = []

        for rank, opportunity in enumerate(opportunities, start=1):
            suggested_size = max(0.0, opportunity.suggested_position_pct)
            size = suggested_size
            clamp_reasons: list[str] = []
            reject_reasons: list[str] = []

            if opportunity.execution_mode == "paper":
                size = 0.0
                reject_reasons.append("paper_mode")
            else:
                mode_base_cap_pct = self._mode_base_cap_pct(opportunity.execution_mode)
                remaining_total = max(0.0, MAX_TOTAL_POSITION_PCT - total_used)
                current_symbol_used = symbol_used.get(opportunity.symbol, 0.0)
                remaining_symbol = max(0.0, MAX_SYMBOL_POSITION_PCT - current_symbol_used)
                long_exchange_key = opportunity.long_exchange.lower()
                short_exchange_key = opportunity.short_exchange.lower()
                remaining_long = max(0.0, MAX_EXCHANGE_POSITION_PCT - exchange_used.get(long_exchange_key, 0.0))
                remaining_short = max(0.0, MAX_EXCHANGE_POSITION_PCT - exchange_used.get(short_exchange_key, 0.0))
                absolute_single_opportunity_cap_pct = MAX_SINGLE_OPPORTUNITY_PCT

                active_cap_candidates = [
                    mode_base_cap_pct,
                    remaining_total,
                    remaining_symbol,
                    remaining_long,
                    remaining_short,
                    absolute_single_opportunity_cap_pct,
                ]
                final_single_cap_pct = max(0.0, min(active_cap_candidates))

                # Reserved placeholders for a future account-risk-input phase.
                effective_leverage = None
                leverage_cap_pct = None
                long_liquidation_distance_pct = None
                short_liquidation_distance_pct = None
                worst_leg_liquidation_distance_pct = None
                liquidation_cap_pct = None

                size, was_clamped = self._clamp_size(size, absolute_single_opportunity_cap_pct)
                if was_clamped:
                    clamp_reasons.append("capped_by_single_opportunity_limit")

                if remaining_total <= 0:
                    reject_reasons.append("no_total_capacity_remaining")
                size, was_clamped = self._clamp_size(size, remaining_total)
                if was_clamped:
                    clamp_reasons.append("capped_by_total_portfolio_limit")

                if remaining_symbol <= 0:
                    reject_reasons.append("no_symbol_capacity_remaining")
                size, was_clamped = self._clamp_size(size, remaining_symbol)
                if was_clamped:
                    clamp_reasons.append("capped_by_symbol_limit")

                if remaining_long <= 0:
                    reject_reasons.append("no_long_exchange_capacity_remaining")
                if remaining_short <= 0:
                    reject_reasons.append("no_short_exchange_capacity_remaining")
                size, was_clamped = self._clamp_size(size, remaining_long)
                if was_clamped:
                    clamp_reasons.append("capped_by_long_exchange_limit")
                size, was_clamped = self._clamp_size(size, remaining_short)
                if was_clamped:
                    clamp_reasons.append("capped_by_short_exchange_limit")

            final_position_pct = max(0.0, min(size, suggested_size))
            is_executable_now = opportunity.execution_mode in {"small_probe", "normal", "size_up"}
            if final_position_pct <= 0.0 and not reject_reasons:
                if max(0.0, MAX_TOTAL_POSITION_PCT - total_used) <= 0:
                    reject_reasons.append("no_total_capacity_remaining")
                if max(0.0, MAX_SYMBOL_POSITION_PCT - symbol_used.get(opportunity.symbol, 0.0)) <= 0:
                    reject_reasons.append("no_symbol_capacity_remaining")
                if max(0.0, MAX_EXCHANGE_POSITION_PCT - exchange_used.get(opportunity.long_exchange.lower(), 0.0)) <= 0:
                    reject_reasons.append("no_long_exchange_capacity_remaining")
                if max(0.0, MAX_EXCHANGE_POSITION_PCT - exchange_used.get(opportunity.short_exchange.lower(), 0.0)) <= 0:
                    reject_reasons.append("no_short_exchange_capacity_remaining")

            if final_position_pct > 0.0:
                total_used += final_position_pct
                symbol_used[opportunity.symbol] = symbol_used.get(opportunity.symbol, 0.0) + final_position_pct
                long_exchange_key = opportunity.long_exchange.lower()
                short_exchange_key = opportunity.short_exchange.lower()
                exchange_used[long_exchange_key] = exchange_used.get(long_exchange_key, 0.0) + final_position_pct
                exchange_used[short_exchange_key] = exchange_used.get(short_exchange_key, 0.0) + final_position_pct

            allocation_priority_label = (
                f"{opportunity.execution_mode}_{'primary' if opportunity.is_primary_route else 'secondary'}"
            )
            update_payload = {
                "mode_base_cap_pct": 0.0,
                "remaining_total_cap_pct": 0.0,
                "remaining_symbol_cap_pct": 0.0,
                "remaining_long_exchange_cap_pct": 0.0,
                "remaining_short_exchange_cap_pct": 0.0,
                "absolute_single_opportunity_cap_pct": MAX_SINGLE_OPPORTUNITY_PCT,
                "effective_leverage": None,
                "leverage_cap_pct": None,
                "long_liquidation_distance_pct": None,
                "short_liquidation_distance_pct": None,
                "worst_leg_liquidation_distance_pct": None,
                "liquidation_cap_pct": None,
                "final_single_cap_pct": 0.0,
                "final_position_pct": final_position_pct,
                "is_executable_now": is_executable_now,
                "portfolio_clamp_reasons": list(dict.fromkeys(clamp_reasons)),
                "portfolio_reject_reasons": list(dict.fromkeys(reject_reasons)),
                "portfolio_total_used_after": total_used,
                "portfolio_symbol_used_after": symbol_used.get(opportunity.symbol, 0.0),
                "portfolio_long_exchange_used_after": exchange_used.get(
                    opportunity.long_exchange.lower(),
                    0.0,
                ),
                "portfolio_short_exchange_used_after": exchange_used.get(
                    opportunity.short_exchange.lower(),
                    0.0,
                ),
                "portfolio_rank": rank,
                "allocation_priority_label": allocation_priority_label,
            }
            if opportunity.execution_mode != "paper":
                update_payload.update(
                    {
                        "mode_base_cap_pct": mode_base_cap_pct,
                        "remaining_total_cap_pct": remaining_total,
                        "remaining_symbol_cap_pct": remaining_symbol,
                        "remaining_long_exchange_cap_pct": remaining_long,
                        "remaining_short_exchange_cap_pct": remaining_short,
                        "absolute_single_opportunity_cap_pct": absolute_single_opportunity_cap_pct,
                        "effective_leverage": effective_leverage,
                        "leverage_cap_pct": leverage_cap_pct,
                        "long_liquidation_distance_pct": long_liquidation_distance_pct,
                        "short_liquidation_distance_pct": short_liquidation_distance_pct,
                        "worst_leg_liquidation_distance_pct": worst_leg_liquidation_distance_pct,
                        "liquidation_cap_pct": liquidation_cap_pct,
                        "final_single_cap_pct": final_single_cap_pct,
                    }
                )
            allocated.append(
                opportunity.model_copy(
                    update=update_payload
                )
            )
        return allocated

    @staticmethod
    def _clamp_size(size: float, cap: float) -> tuple[float, bool]:
        capped = min(size, max(0.0, cap))
        return capped, capped < size

    @staticmethod
    def _mode_base_cap_pct(execution_mode: str) -> float:
        if execution_mode == "small_probe":
            return 0.005
        if execution_mode == "normal":
            return 0.02
        if execution_mode == "size_up":
            return 0.05
        return 0.0

    @staticmethod
    def _execution_risk_config(execution_mode: str) -> ExecutionRiskConfig:
        return EXECUTION_RISK_CONFIGS.get(execution_mode, EXECUTION_RISK_CONFIGS["small_probe"])

    def _extended_size_up_risk_assessment(
        self,
        opportunity: Opportunity,
        execution_mode: str,
        soft_risk_flag_count: int,
        size_up_blockers: list[str],
        execution_risk_config: ExecutionRiskConfig,
    ) -> tuple[bool, list[str]]:
        blockers: list[str] = []
        extended_policy = EXECUTION_RISK_CONFIGS["extended_size_up"]

        if execution_mode != "size_up":
            blockers.append("size_up_not_achieved_blocks_extended_size_up")
        if not opportunity.is_primary_route:
            blockers.append("non_primary_route_blocks_extended_size_up")
        if not opportunity.is_tradable:
            blockers.append("non_tradable_grade_blocks_extended_size_up")
        if opportunity.data_quality_status != "healthy":
            blockers.append("degraded_data_quality_blocks_extended_size_up")
        if soft_risk_flag_count > 1:
            blockers.append("too_many_soft_risk_flags_for_extended_size_up")
        if opportunity.size_up_edge_buffer_bps < 5.0:
            blockers.append("insufficient_extended_size_up_edge_buffer")
        if "missing_liquidity_data_blocks_size_up" in size_up_blockers:
            blockers.append("missing_liquidity_blocks_extended_size_up")
        if "degraded_data_quality_blocks_size_up" in size_up_blockers:
            blockers.append("degraded_data_quality_blocks_extended_size_up")
        if execution_risk_config.target_leverage > extended_policy.max_allowed_leverage:
            blockers.append("configured_target_leverage_too_high_for_extended_size_up")
        if (
            execution_risk_config.min_required_liquidation_buffer_pct
            < extended_policy.min_required_liquidation_buffer_pct
        ):
            blockers.append("configured_liquidation_buffer_requirement_not_strict_enough")

        blockers = list(dict.fromkeys(blockers))
        return len(blockers) == 0, blockers

    @staticmethod
    def _blocking_risk_count(risk_flags: list[str]) -> int:
        return sum(1 for risk_flag in risk_flags if risk_flag in BLOCKING_RISK_FLAGS)

    @staticmethod
    def _missing_liquidity_count(risk_flags: list[str]) -> int:
        return 1 if "missing_liquidity_data" in risk_flags else 0

    def _conviction_score(
        self,
        opportunity: Opportunity,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
        is_primary_route: bool,
    ) -> tuple[float, list[str]]:
        score = 0.10
        drivers: list[str] = []

        if opportunity.risk_adjusted_edge_bps >= 18:
            score += 0.22
            drivers.append("strong_risk_adjusted_edge")
        elif opportunity.risk_adjusted_edge_bps >= 12:
            score += 0.16
        elif opportunity.risk_adjusted_edge_bps >= 8:
            score += 0.10

        if opportunity.net_edge_bps >= 20:
            score += 0.18
            drivers.append("strong_net_edge")
        elif opportunity.net_edge_bps >= 12:
            score += 0.12

        if opportunity.funding_confidence_score >= 0.85:
            score += 0.12
            drivers.append("high_funding_confidence")
        elif opportunity.funding_confidence_score >= 0.65:
            score += 0.08
            drivers.append("reliable_funding_confidence")

        if long_snapshot.funding_period_hours == short_snapshot.funding_period_hours:
            score += 0.06
            drivers.append("matched_funding_periods")

        if "missing_liquidity_data" not in opportunity.risk_flags:
            score += 0.08
            drivers.append("complete_liquidity_data")
        if not any(flag in LIQUIDITY_RISK_FLAGS for flag in opportunity.risk_flags):
            score += 0.16
            drivers.append("adequate_liquidity")
        if is_primary_route:
            score += 0.10
            drivers.append("primary_route")

        if "different_funding_periods" in opportunity.risk_flags:
            score -= 0.06
        if "low_confidence_funding" in opportunity.risk_flags:
            score -= 0.08
        if "missing_liquidity_data" in opportunity.risk_flags:
            score -= 0.12
        if "low_open_interest" in opportunity.risk_flags:
            score -= 0.06
        if "low_quote_volume" in opportunity.risk_flags:
            score -= 0.06
        if "abnormal_hourly_funding" in opportunity.risk_flags:
            score -= 0.10

        return max(0.0, min(1.0, score)), drivers

    @staticmethod
    def _conviction_label(conviction_score: float) -> str:
        if conviction_score >= 0.75:
            return "high"
        if conviction_score >= 0.50:
            return "medium"
        return "low"

    @staticmethod
    def _funding_confidence_score(
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
    ) -> float:
        base_score = min(
            ArbitrageScannerService._funding_source_score(long_snapshot.funding_rate_source),
            ArbitrageScannerService._funding_source_score(short_snapshot.funding_rate_source),
        )
        if long_snapshot.funding_period_hours != short_snapshot.funding_period_hours:
            base_score -= 0.1
        return max(0.0, min(1.0, base_score))

    @staticmethod
    def _funding_source_score(funding_rate_source: str | None) -> float:
        if funding_rate_source is None:
            return 0.2
        return FUNDING_SOURCE_CONFIDENCE.get(funding_rate_source, 0.2)

    @staticmethod
    def _funding_confidence_label(funding_confidence_score: float) -> str:
        if funding_confidence_score >= 0.8:
            return "high"
        if funding_confidence_score >= 0.55:
            return "medium"
        return "low"

    def _risk_flags(
        self,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
        funding_confidence_score: float,
        data_quality_status: str,
        data_quality_flags: list[str],
    ) -> list[str]:
        flags: list[str] = []
        if long_snapshot.funding_rate_source != short_snapshot.funding_rate_source:
            flags.append("mixed_funding_sources")
        if long_snapshot.funding_period_hours != short_snapshot.funding_period_hours:
            flags.append("different_funding_periods")
        if abs(long_snapshot.hourly_funding_rate_bps or 0.0) > MAX_ABS_HOURLY_FUNDING_BPS:
            flags.append("high_long_hourly_funding")
        if abs(short_snapshot.hourly_funding_rate_bps or 0.0) > MAX_ABS_HOURLY_FUNDING_BPS:
            flags.append("high_short_hourly_funding")
        if (
            abs(long_snapshot.hourly_funding_rate_bps or 0.0) > ABNORMAL_ABS_HOURLY_FUNDING_BPS
            or abs(short_snapshot.hourly_funding_rate_bps or 0.0) > ABNORMAL_ABS_HOURLY_FUNDING_BPS
        ):
            flags.append("abnormal_hourly_funding")
        if self._is_missing_liquidity_data(long_snapshot, short_snapshot):
            flags.append("missing_liquidity_data")
        if self._has_low_open_interest(long_snapshot, short_snapshot):
            flags.append("low_open_interest")
        if self._has_low_quote_volume(long_snapshot, short_snapshot):
            flags.append("low_quote_volume")
        if funding_confidence_score < 0.55:
            flags.append("low_confidence_funding")
        if data_quality_status != "healthy":
            flags.append("degraded_data_quality")
        if self._has_cross_exchange_quality_flag(data_quality_flags):
            flags.append("cross_exchange_price_quality_risk")
        return list(dict.fromkeys(flags))

    @staticmethod
    def _snapshot_quality_status(snapshot: MarketSnapshot) -> str:
        return snapshot.data_quality_status or "healthy"

    @staticmethod
    def _snapshot_quality_score(snapshot: MarketSnapshot) -> float:
        return snapshot.data_quality_score if snapshot.data_quality_score is not None else 1.0

    @staticmethod
    def _has_cross_exchange_quality_flag(flags: list[str]) -> bool:
        return any("cross_exchange_price_outlier" in flag for flag in flags)

    def _opportunity_data_quality(
        self,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
    ) -> tuple[str, float, list[str], list[str]]:
        long_status = self._snapshot_quality_status(long_snapshot)
        short_status = self._snapshot_quality_status(short_snapshot)
        status = max((long_status, short_status), key=lambda value: DATA_QUALITY_SEVERITY.get(value, 99))
        score = min(self._snapshot_quality_score(long_snapshot), self._snapshot_quality_score(short_snapshot))
        flags = list(
            dict.fromkeys(
                list(long_snapshot.data_quality_flags or [])
                + list(short_snapshot.data_quality_flags or [])
            )
        )
        drivers: list[str] = []
        degraded_count = sum(1 for value in (long_status, short_status) if value == "degraded")
        if degraded_count == 0 and status == "healthy":
            drivers.append("both_legs_healthy")
        elif degraded_count == 1:
            drivers.append("one_leg_degraded")
        elif degraded_count >= 2:
            drivers.append("both_legs_degraded")
        if self._has_cross_exchange_quality_flag(flags):
            drivers.append("degraded_cross_exchange_price_signal")
        return status, score, flags, drivers

    @staticmethod
    def _opportunity_grade(risk_adjusted_edge_bps: float, is_tradable: bool) -> str:
        if is_tradable:
            return "tradable"
        if risk_adjusted_edge_bps >= 3:
            return "watchlist"
        return "discard"

    @staticmethod
    def _reject_reasons(
        risk_adjusted_edge_bps: float,
        risk_flags: list[str],
    ) -> list[str]:
        reject_reasons: list[str] = []
        if risk_adjusted_edge_bps < 8:
            reject_reasons.append("insufficient_risk_adjusted_edge")
        for risk_flag in risk_flags:
            if risk_flag in BLOCKING_RISK_FLAGS and risk_flag not in reject_reasons:
                reject_reasons.append(risk_flag)
        return reject_reasons

    @staticmethod
    def _baseline_suggested_position_pct(
        position_size_multiplier: float,
        conviction_label: str,
        risk_flags: list[str],
        max_position_pct: float,
    ) -> float:
        liquidity_factor = ArbitrageScannerService._liquidity_factor(risk_flags)
        conviction_factor = {
            "high": 1.0,
            "medium": 0.85,
            "low": 0.5,
        }.get(conviction_label, 0.5)
        base_position_pct = BASE_POSITION_PCT * position_size_multiplier * liquidity_factor * conviction_factor
        return ArbitrageScannerService._apply_position_cap(base_position_pct, max_position_pct)

    @staticmethod
    def _apply_execution_mode_to_suggested_position_pct(
        baseline_suggested_position_pct: float,
        execution_mode: str,
        max_position_pct: float,
    ) -> float:
        suggested_position_pct = ArbitrageScannerService._adjust_position_pct_for_execution_mode(
            baseline_suggested_position_pct,
            execution_mode,
        )
        return ArbitrageScannerService._apply_position_cap(suggested_position_pct, max_position_pct)

    @staticmethod
    def _liquidity_factor(risk_flags: list[str]) -> float:
        if "low_open_interest" in risk_flags:
            return 0.3
        if "missing_liquidity_data" in risk_flags:
            return 0.5
        return 1.0

    @staticmethod
    def _max_position_pct(opportunity_grade: str) -> float:
        if opportunity_grade == "tradable":
            return 0.10
        return 0.03

    @staticmethod
    def _normal_required_edge_bps() -> float:
        return 10.0

    @staticmethod
    def _size_up_required_edge_bps() -> float:
        return 18.0

    @staticmethod
    def _normal_eligibility_score(edge_buffer_bps: float, funding_confidence_score: float) -> float:
        edge_component = max(0.0, min(1.0, (edge_buffer_bps + 6.0) / 12.0))
        return max(0.0, min(1.0, (edge_component * 0.6) + (funding_confidence_score * 0.4)))

    @staticmethod
    def _determine_execution_mode(
        opportunity: Opportunity,
        conviction_score: float,
        baseline_suggested_position_pct: float,
    ) -> tuple[str, list[str], int, list[str], list[str], list[str], list[str]]:
        risk_flags = set(opportunity.risk_flags)
        missing_liquidity = "missing_liquidity_data" in risk_flags
        soft_risk_count = len(risk_flags & NORMAL_SOFT_RISK_FLAGS)
        positive_drivers: list[str] = []
        if opportunity.is_primary_route:
            positive_drivers.append("primary_route")
        if opportunity.opportunity_grade == "tradable":
            positive_drivers.append("tradable")
        if conviction_score >= 0.20:
            positive_drivers.append("meets_small_probe_conviction")
        if opportunity.funding_confidence_score >= 0.45:
            positive_drivers.append("meets_small_probe_funding_confidence")
        if not missing_liquidity:
            positive_drivers.append("complete_liquidity_data")

        gap_drivers: list[str] = []
        if opportunity.edge_buffer_bps < 0:
            gap_drivers.append("below_normal_edge_threshold")
        if conviction_score < 0.50:
            gap_drivers.append("below_normal_conviction")
        if opportunity.funding_confidence_score < 0.55:
            gap_drivers.append("below_normal_funding_confidence")
        if soft_risk_count > 2:
            gap_drivers.append("too_many_soft_risk_flags")
        normal_blockers: list[str] = list(gap_drivers)
        if not opportunity.is_primary_route:
            normal_blockers.append("non_primary_route")
        if opportunity.opportunity_grade != "tradable":
            normal_blockers.append("non_tradable_opportunity_grade")
        if missing_liquidity:
            normal_blockers.append("missing_liquidity_data_blocks_normal")
        normal_blockers = list(dict.fromkeys(normal_blockers))
        normal_promotion_reasons: list[str] = []
        size_up_blockers: list[str] = []
        if opportunity.size_up_edge_buffer_bps < 0:
            size_up_blockers.append("insufficient_size_up_edge_buffer")
        if conviction_score < 0.75:
            size_up_blockers.append("insufficient_size_up_conviction")
        if opportunity.funding_confidence_score < 0.80:
            size_up_blockers.append("insufficient_size_up_funding_confidence")
        if opportunity.data_quality_status != "healthy":
            size_up_blockers.append("degraded_data_quality_blocks_size_up")
        if missing_liquidity:
            size_up_blockers.append("missing_liquidity_data_blocks_size_up")
        if not opportunity.is_primary_route:
            size_up_blockers.append("non_primary_route_blocks_size_up")
        if opportunity.opportunity_grade != "tradable":
            size_up_blockers.append("non_tradable_grade_blocks_size_up")
        if soft_risk_count > 1:
            size_up_blockers.append("too_many_soft_risk_flags_for_size_up")
        size_up_blockers = list(dict.fromkeys(size_up_blockers))
        size_up_promotion_reasons: list[str] = []
        if baseline_suggested_position_pct <= 0:
            return (
                "paper",
                ["paper_due_to_zero_suggested_size"],
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )
        if opportunity.risk_adjusted_edge_bps < 6:
            return (
                "paper",
                ["paper_due_to_low_risk_adjusted_edge"],
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )
        if opportunity.funding_confidence_score < 0.45:
            return (
                "paper",
                ["paper_due_to_low_funding_confidence"],
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )
        if conviction_score < 0.20:
            return (
                "paper",
                ["paper_due_to_low_conviction"],
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )
        if (not opportunity.is_primary_route) and conviction_score < 0.45:
            return (
                "paper",
                ["paper_due_to_secondary_low_conviction"],
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )

        if (
            opportunity.is_primary_route
            and opportunity.opportunity_grade == "tradable"
            and opportunity.size_up_edge_buffer_bps >= 0
            and conviction_score >= 0.75
            and opportunity.funding_confidence_score >= 0.80
            and opportunity.data_quality_status == "healthy"
            and not missing_liquidity
            and soft_risk_count <= 1
        ):
            return (
                "size_up",
                list(dict.fromkeys(positive_drivers + ["strong_risk_adjusted_edge", "meets_size_up_thresholds"])),
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                ["meets_size_up_thresholds"],
            )

        if (
            opportunity.is_primary_route
            and opportunity.opportunity_grade == "tradable"
            and opportunity.edge_buffer_bps >= 0
            and conviction_score >= 0.50
            and opportunity.funding_confidence_score >= 0.55
            and not missing_liquidity
            and soft_risk_count <= 2
        ):
            return (
                "normal",
                list(dict.fromkeys(positive_drivers + ["meets_normal_thresholds"])),
                soft_risk_count,
                normal_blockers,
                ["meets_normal_thresholds"],
                size_up_blockers,
                size_up_promotion_reasons,
            )

        if (
            missing_liquidity
            and opportunity.is_primary_route
            and opportunity.risk_adjusted_edge_bps >= 10
            and opportunity.funding_confidence_score >= 0.45
        ):
            return (
                "small_probe",
                list(
                    dict.fromkeys(
                        positive_drivers
                        + [
                            "small_probe_despite_missing_liquidity_data",
                            "blocked_from_normal_due_to_missing_liquidity_data",
                        ]
                    )
                ),
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )

        if (
            opportunity.risk_adjusted_edge_bps >= 6
            and conviction_score >= 0.20
            and opportunity.funding_confidence_score >= 0.45
            and not missing_liquidity
        ):
            drivers = list(dict.fromkeys(positive_drivers + gap_drivers))
            if not gap_drivers:
                drivers.append("below_normal_thresholds")
            return (
                "small_probe",
                drivers,
                soft_risk_count,
                normal_blockers,
                normal_promotion_reasons,
                size_up_blockers,
                size_up_promotion_reasons,
            )

        return (
            "paper",
            ["paper_due_to_execution_rules"],
            soft_risk_count,
            normal_blockers,
            normal_promotion_reasons,
            size_up_blockers,
            size_up_promotion_reasons,
        )

    @staticmethod
    def _adjust_position_pct_for_execution_mode(
        suggested_position_pct: float,
        execution_mode: str,
    ) -> float:
        if execution_mode == "paper":
            return 0.0
        if execution_mode == "small_probe":
            return suggested_position_pct * 0.3
        if execution_mode == "size_up":
            return suggested_position_pct * 1.25
        return suggested_position_pct

    @staticmethod
    def _apply_position_cap(
        suggested_position_pct: float,
        max_position_pct: float,
    ) -> float:
        return max(0.0, min(suggested_position_pct, max_position_pct))

    @staticmethod
    def _is_missing_liquidity_data(long_snapshot: MarketSnapshot, short_snapshot: MarketSnapshot) -> bool:
        return (
            ArbitrageScannerService._snapshot_missing_liquidity_data(long_snapshot)
            or ArbitrageScannerService._snapshot_missing_liquidity_data(short_snapshot)
        )

    @staticmethod
    def _snapshot_missing_liquidity_data(snapshot: MarketSnapshot) -> bool:
        return snapshot.open_interest_usd is None and snapshot.quote_volume_24h_usd is None

    @staticmethod
    def _has_low_open_interest(long_snapshot: MarketSnapshot, short_snapshot: MarketSnapshot) -> bool:
        return (
            ArbitrageScannerService._snapshot_low_open_interest(long_snapshot)
            or ArbitrageScannerService._snapshot_low_open_interest(short_snapshot)
        )

    @staticmethod
    def _snapshot_low_open_interest(snapshot: MarketSnapshot) -> bool:
        return snapshot.open_interest_usd is not None and snapshot.open_interest_usd < 10_000_000

    @staticmethod
    def _has_low_quote_volume(long_snapshot: MarketSnapshot, short_snapshot: MarketSnapshot) -> bool:
        return (
            ArbitrageScannerService._snapshot_low_quote_volume(long_snapshot)
            or ArbitrageScannerService._snapshot_low_quote_volume(short_snapshot)
        )

    @staticmethod
    def _snapshot_low_quote_volume(snapshot: MarketSnapshot) -> bool:
        return snapshot.quote_volume_24h_usd is not None and snapshot.quote_volume_24h_usd < 20_000_000

    @staticmethod
    def _limit_opportunities_per_symbol(opportunities: list[Opportunity]) -> list[Opportunity]:
        kept_counts: dict[str, int] = {}
        limited: list[Opportunity] = []
        for opportunity in opportunities:
            symbol_count = kept_counts.get(opportunity.symbol, 0)
            if symbol_count >= MAX_OPPORTUNITIES_PER_SYMBOL:
                continue
            kept_counts[opportunity.symbol] = symbol_count + 1
            limited.append(opportunity)
        return limited

    @staticmethod
    def _optional_diff(left: float | None, right: float | None) -> float | None:
        if left is None or right is None:
            return None
        return left - right

    @staticmethod
    def _to_bps(value: float | None) -> float | None:
        if value is None:
            return None
        return value * BPS_MULTIPLIER
