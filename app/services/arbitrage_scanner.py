from __future__ import annotations

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
MIN_WATCHLIST_NET_EDGE_BPS = 5.0
MIN_TRADABLE_NET_EDGE_BPS = 8.0
DEFAULT_HOLDING_HOURS = 8
MAX_OPPORTUNITIES_PER_SYMBOL = 3
BPS_MULTIPLIER = 10_000
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


class ArbitrageScannerService:
    """Build deterministic, pairwise arbitrage opportunities from market snapshots."""

    def __init__(self) -> None:
        # Restrict scanner universe to major assets and symbols supported by this project.
        self.allowed_symbols = set(MAJOR_SYMBOL_ALLOWLIST) & set(supported_symbols())

    def build_opportunities(self, snapshots: list[MarketSnapshot]) -> list[Opportunity]:
        grouped: dict[str, list[MarketSnapshot]] = {}
        for snapshot in snapshots:
            symbol = snapshot.base_symbol.upper()
            if symbol not in self.allowed_symbols:
                continue
            grouped.setdefault(snapshot.normalized_symbol, []).append(snapshot)

        opportunities: list[Opportunity] = []
        for symbol_snapshots in grouped.values():
            opportunities.extend(self._build_symbol_opportunities(symbol_snapshots))

        opportunities.sort(
            key=lambda item: (item.is_tradable, item.risk_adjusted_edge_bps, item.net_edge_bps),
            reverse=True,
        )
        return self._limit_opportunities_per_symbol(opportunities)

    def _build_symbol_opportunities(self, snapshots: list[MarketSnapshot]) -> list[Opportunity]:
        opportunities: list[Opportunity] = []
        for left, right in combinations(snapshots, 2):
            try:
                opportunity = self._build_pair_opportunity(left, right)
            except Exception:
                continue
            if opportunity is None:
                continue
            opportunities.append(opportunity)
        return opportunities

    def _build_pair_opportunity(self, left: MarketSnapshot, right: MarketSnapshot) -> Opportunity | None:
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
        risk_flags = self._risk_flags(long_snapshot, short_snapshot, funding_confidence_score)
        risk_adjusted_edge_bps = net_edge_bps * funding_confidence_score
        is_tradable = risk_adjusted_edge_bps >= 8
        opportunity_grade = self._opportunity_grade(risk_adjusted_edge_bps, is_tradable)
        reject_reasons = [] if is_tradable else self._reject_reasons(
            risk_adjusted_edge_bps,
            risk_flags,
        )
        suggested_position_pct = self._suggested_position_pct(
            funding_confidence_score,
            funding_confidence_label,
            risk_flags,
        )
        max_position_pct = self._max_position_pct(opportunity_grade)
        execution_mode = self._execution_mode(opportunity_grade, risk_adjusted_edge_bps)

        if opportunity_grade == "discard":
            return None

        return Opportunity(
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
            risk_flags=risk_flags,
            opportunity_grade=opportunity_grade,
            is_tradable=is_tradable,
            reject_reasons=reject_reasons,
            position_size_multiplier=funding_confidence_score,
            suggested_position_pct=suggested_position_pct,
            max_position_pct=max_position_pct,
            execution_mode=execution_mode,
        )

    def _funding_confidence_score(
        self,
        long_snapshot: MarketSnapshot,
        short_snapshot: MarketSnapshot,
    ) -> float:
        base_score = min(
            self._funding_source_score(long_snapshot.funding_rate_source),
            self._funding_source_score(short_snapshot.funding_rate_source),
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
        return flags

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
            if risk_flag in {
                "mixed_funding_sources",
                "low_confidence_funding",
                "different_funding_periods",
                "abnormal_hourly_funding",
                "low_open_interest",
                "low_quote_volume",
                "missing_liquidity_data",
            } and risk_flag not in reject_reasons:
                reject_reasons.append(risk_flag)
        return reject_reasons

    @staticmethod
    def _suggested_position_pct(
        position_size_multiplier: float,
        funding_confidence_label: str,
        risk_flags: list[str],
    ) -> float:
        liquidity_factor = ArbitrageScannerService._liquidity_factor(risk_flags)
        risk_factor = 0.5 if funding_confidence_label == "low" else 1.0
        suggested_position_pct = BASE_POSITION_PCT * position_size_multiplier * liquidity_factor * risk_factor
        return max(0.0, min(BASE_POSITION_PCT, suggested_position_pct))

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
    def _execution_mode(opportunity_grade: str, risk_adjusted_edge_bps: float) -> str:
        if opportunity_grade == "tradable":
            return "normal"
        if opportunity_grade == "watchlist" and risk_adjusted_edge_bps >= 5:
            return "small_probe"
        return "paper"

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
