import asyncio

from app.main import get_opportunities
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.market_data import MarketDataService


def _snapshot(
    exchange: str,
    mark_price: float,
    *,
    funding_rate: float = 0.0,
    funding_rate_source: str = "current",
    funding_period_hours: int = 8,
    open_interest_usd: float | None = 15_000_000.0,
    quote_volume_24h_usd: float | None = 25_000_000.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=exchange,
        venue_type="dex" if exchange in {"hyperliquid", "lighter"} else "cex",
        base_symbol="BTC",
        normalized_symbol="BTC-USDT-PERP",
        instrument_id=f"{exchange}-BTC",
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

    assert "opportunities" in response
    assert len(response["opportunities"]) == 1

    item = response["opportunities"][0]
    assert item["symbol"] == "BTC"
    assert item["cluster_id"] == "BTC|okx|funding_capture"
    assert item["is_primary_route"] is True
    assert item["route_rank"] == 1
    assert item["long_exchange"] == "binance"
    assert item["short_exchange"] == "okx"
    assert item["price_spread_bps"] > 0
    assert item["estimated_fee_bps"] == 10.0
    assert item["funding_confidence_label"] == "high"
    assert item["conviction_score"] >= 0.75
    assert item["conviction_label"] == "high"
    assert "high_funding_confidence" in item["conviction_drivers"]
    assert "primary_route" in item["conviction_drivers"]
    assert item["size_up_eligible"] is True
    assert item["opportunity_grade"] == "tradable"
    assert item["execution_mode"] == "size_up"
    assert item["suggested_position_pct"] > 0.09
    assert item["max_position_pct"] == 0.10


def test_get_opportunities_filters_non_positive_net_edge(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate_source="latest_reported", open_interest_usd=None, quote_volume_24h_usd=None),
                _snapshot("okx", 100.06, funding_rate_source="current", open_interest_usd=None, quote_volume_24h_usd=None),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []


def test_get_opportunities_keeps_low_confidence_funding_when_edge_is_strong(monkeypatch) -> None:
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
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.32,
                    funding_rate=0.0004,
                    funding_rate_source="last_settled_fallback",
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "tradable"
    assert item["is_tradable"] is True
    assert "low_confidence_funding" in item["risk_flags"]
    assert 0.0 <= item["conviction_score"] <= 1.0
    assert item["conviction_label"] == "low"
    assert item["size_up_eligible"] is False
    assert item["execution_mode"] == "normal"


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
    assert item["is_tradable"] is False
    assert item["execution_mode"] == "small_probe"
    assert item["suggested_position_pct"] > 0.0
    assert item["suggested_position_pct"] < item["max_position_pct"]


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
    assert item["opportunity_grade"] == "watchlist"
    assert item["conviction_label"] == "low"
    assert item["execution_mode"] == "paper"
    assert item["suggested_position_pct"] == 0.0


def test_get_opportunities_same_cluster_routes_rank_primary_first() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=-0.0005, funding_rate_source="current"),
            _snapshot("lighter", 102.0, funding_rate=0.0012, funding_rate_source="current"),
        ]
    )

    lighter_cluster = [item for item in opportunities if item.cluster_id == "BTC|lighter|funding_capture"]
    assert len(lighter_cluster) == 2
    assert {item.cluster_id for item in lighter_cluster} == {"BTC|lighter|funding_capture"}
    assert sum(1 for item in lighter_cluster if item.is_primary_route) == 1
    assert [item.route_rank for item in lighter_cluster] == [1, 2]
    assert lighter_cluster[0].is_primary_route is True
    assert lighter_cluster[0].risk_adjusted_edge_bps >= lighter_cluster[1].risk_adjusted_edge_bps


def test_get_opportunities_conviction_score_is_clamped_and_labels_match() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.002, funding_rate_source="latest_reported"),
            _snapshot("lighter", 103.0, funding_rate=0.002, funding_rate_source="current"),
            _snapshot(
                "okx",
                100.5,
                funding_rate=0.0001,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    assert opportunities
    for item in opportunities:
        assert 0.0 <= item.conviction_score <= 1.0
        if item.conviction_score >= 0.75:
            assert item.conviction_label == "high"
        elif item.conviction_score >= 0.50:
            assert item.conviction_label == "medium"
        else:
            assert item.conviction_label == "low"


def test_size_up_requires_primary_route_and_beats_normal_sizing() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 100.5, funding_rate=-0.0006, funding_rate_source="current"),
            _snapshot("lighter", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )

    size_up_item = next(item for item in opportunities if item.execution_mode == "size_up")
    normal_item = next(
        item for item in opportunities if item.cluster_id == "BTC|lighter|funding_capture" and item.execution_mode == "normal"
    )

    assert size_up_item.opportunity_grade == "tradable"
    assert size_up_item.is_primary_route is True
    assert size_up_item.conviction_score >= 0.75
    assert size_up_item.funding_confidence_score >= 0.8
    assert "missing_liquidity_data" not in size_up_item.risk_flags
    assert "different_funding_periods" not in size_up_item.risk_flags
    assert size_up_item.size_up_eligible is True
    assert normal_item.is_primary_route is False
    assert normal_item.size_up_eligible is False
    assert size_up_item.suggested_position_pct > normal_item.suggested_position_pct
