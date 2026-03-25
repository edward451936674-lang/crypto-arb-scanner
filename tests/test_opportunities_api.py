import asyncio

from app.main import get_opportunities
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.market_data import MarketDataService


def _snapshot(
    exchange: str,
    mark_price: float,
    *,
    base_symbol: str = "BTC",
    funding_rate: float = 0.0,
    funding_rate_source: str = "current",
    funding_period_hours: int = 8,
    open_interest_usd: float | None = 15_000_000.0,
    quote_volume_24h_usd: float | None = 25_000_000.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=exchange,
        venue_type="dex" if exchange in {"hyperliquid", "lighter"} else "cex",
        base_symbol=base_symbol,
        normalized_symbol=f"{base_symbol}-USDT-PERP",
        instrument_id=f"{exchange}-{base_symbol}",
        mark_price=mark_price,
        funding_rate=funding_rate,
        funding_rate_source=funding_rate_source,
        funding_period_hours=funding_period_hours,
        open_interest_usd=open_interest_usd,
        quote_volume_24h_usd=quote_volume_24h_usd,
        timestamp_ms=1710000100000,
    )


def test_get_opportunities_returns_ranked_items(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["symbol"] == "BTC"
    assert item["cluster_id"] == "BTC|binance|funding_capture"
    assert item["is_primary_route"] is True
    assert item["route_rank"] == 1
    assert item["conviction_label"] == "high"
    assert item["size_up_eligible"] is True
    assert item["execution_mode"] == "size_up"
    assert item["suggested_position_pct"] == item["max_position_pct"]
    assert item["final_position_pct"] <= item["suggested_position_pct"]
    assert item["portfolio_rank"] == 1


def test_get_opportunities_filters_non_positive_net_edge(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.06,
                    funding_rate_source="current",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []


def test_strong_primary_route_can_be_medium_conviction_and_normal(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "lighter",
                    100.0,
                    funding_rate=-0.0004,
                    funding_rate_source="estimated_current",
                    funding_period_hours=4,
                ),
                _snapshot(
                    "okx",
                    100.32,
                    funding_rate=0.0004,
                    funding_rate_source="last_settled_fallback",
                    funding_period_hours=8,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "tradable"
    assert item["is_primary_route"] is True
    assert item["conviction_label"] == "medium"
    assert 0.50 <= item["conviction_score"] < 0.75
    assert item["execution_mode"] == "normal"
    assert item["size_up_eligible"] is False
    assert "strong_net_edge" in item["conviction_drivers"]
    assert "adequate_liquidity" in item["conviction_drivers"]
    assert "primary_route" in item["conviction_drivers"]


def test_missing_liquidity_routes_score_lower_than_clean_routes() -> None:
    scanner = ArbitrageScannerService()

    clean_route = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.0004, funding_rate_source="estimated_current", funding_period_hours=4),
            _snapshot("okx", 100.32, funding_rate=0.0004, funding_rate_source="last_settled_fallback", funding_period_hours=8),
        ]
    )[0]
    missing_liquidity_route = scanner.build_opportunities(
        [
            _snapshot(
                "lighter",
                100.0,
                funding_rate=-0.0004,
                funding_rate_source="estimated_current",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.32,
                funding_rate=0.0004,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=8,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )[0]

    assert clean_route.conviction_score > missing_liquidity_route.conviction_score
    assert clean_route.suggested_position_pct > missing_liquidity_route.suggested_position_pct
    assert missing_liquidity_route.execution_mode == "small_probe"


def test_get_opportunities_keeps_watchlist_items_as_small_probe(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate_source="latest_reported"),
                _snapshot("okx", 100.17, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "watchlist"
    assert item["execution_mode"] == "small_probe"
    assert 0.0 < item["suggested_position_pct"] < item["max_position_pct"]


def test_get_opportunities_sets_paper_mode_to_zero_position(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.24,
                    funding_rate_source="last_settled_fallback",
                    funding_period_hours=4,
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["execution_mode"] == "paper"
    assert item["suggested_position_pct"] == 0.0
    assert item["final_position_pct"] == 0.0
    assert "paper_mode" in item["portfolio_reject_reasons"]


def test_routes_with_same_long_exchange_share_cluster_and_single_primary() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
        ]
    )

    lighter_cluster = [item for item in opportunities if item.cluster_id == "BTC|lighter|funding_capture"]
    assert len(lighter_cluster) == 2
    assert sum(1 for item in lighter_cluster if item.is_primary_route) == 1
    assert [item.route_rank for item in lighter_cluster] == [1, 2]
    assert all(item.long_exchange == "lighter" for item in lighter_cluster)


def test_only_primary_routes_with_medium_plus_conviction_can_be_normal() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
            _snapshot(
                "hyperliquid",
                103.0,
                funding_rate=0.002,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    normal_routes = [item for item in opportunities if item.execution_mode == "normal"]
    assert all(item.is_primary_route for item in normal_routes)
    assert all(item.conviction_score >= 0.50 for item in normal_routes)


def test_secondary_routes_usually_fall_to_small_probe() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
        ]
    )

    secondary_routes = [item for item in opportunities if item.route_rank and item.route_rank > 1]
    assert secondary_routes
    assert all(item.execution_mode == "small_probe" for item in secondary_routes)


def test_size_up_remains_strict_for_noisy_mismatched_routes() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot(
                "lighter",
                100.0,
                funding_rate=-0.0004,
                funding_rate_source="estimated_current",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.32,
                funding_rate=0.0004,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=8,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.opportunity_grade == "tradable"
    assert item.size_up_eligible is False
    assert item.execution_mode != "size_up"


def test_portfolio_total_allocated_never_exceeds_cap() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    total_final = sum(item.final_position_pct for item in opportunities)
    assert total_final <= 0.20 + 1e-12
    assert all(item.final_position_pct <= item.suggested_position_pct + 1e-12 for item in opportunities)


def test_portfolio_symbol_cap_is_enforced() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    symbol_total = sum(item.final_position_pct for item in opportunities if item.symbol == "BTC")
    assert symbol_total <= 0.08 + 1e-12
    capped = [item for item in opportunities if "capped_by_symbol_limit" in item.portfolio_clamp_reasons]
    assert capped


def test_portfolio_exchange_cap_is_enforced() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("binance", 200.0, base_symbol="ETH", funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 202.0, base_symbol="ETH", funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 204.0, base_symbol="ETH", funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    assert all(item.portfolio_long_exchange_position_after <= 0.10 + 1e-12 for item in opportunities)
    assert all(item.portfolio_short_exchange_position_after <= 0.10 + 1e-12 for item in opportunities)
    has_exchange_clamp = any(
        "capped_by_long_exchange_limit" in item.portfolio_clamp_reasons
        or "capped_by_short_exchange_limit" in item.portfolio_clamp_reasons
        for item in opportunities
    )
    assert has_exchange_clamp


def test_opportunities_still_visible_when_final_allocation_zero() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    zero_alloc = [item for item in opportunities if item.final_position_pct == 0.0]
    assert zero_alloc
    assert all(item.portfolio_rank is not None for item in zero_alloc)
