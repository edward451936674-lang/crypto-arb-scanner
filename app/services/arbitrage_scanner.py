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
DEFAULT_HOLDING_HOURS = 8
BPS_MULTIPLIER = 10_000
EXCHANGE_FEE_BPS = {
    "binance": 5.0,
    "okx": 5.0,
    "hyperliquid": 4.0,
    "lighter": 6.0,
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

        opportunities.sort(key=lambda item: item.net_edge_bps, reverse=True)
        return opportunities

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

        if net_edge_bps <= 0:
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
        )

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
