import asyncio

from app.main import get_opportunities
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.market_data import MarketDataService


def test_get_opportunities_returns_ranked_items(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                MarketSnapshot(
                    exchange="binance",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTCUSDT",
                    mark_price=100.0,
                    funding_rate=-0.001,
                    funding_rate_source="latest_reported",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
                MarketSnapshot(
                    exchange="okx",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTC-USDT-SWAP",
                    mark_price=101.0,
                    funding_rate=0.001,
                    funding_rate_source="current",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert "opportunities" in response
    assert len(response["opportunities"]) >= 1

    item = response["opportunities"][0]
    assert item["symbol"] == "BTC"
    assert item["long_exchange"] == "binance"
    assert item["short_exchange"] == "okx"
    assert item["price_spread_bps"] > 0
    assert "estimated_edge_bps" in item
    assert "net_edge_bps" in item
    assert "long_hourly_funding_rate" in item
    assert "short_hourly_funding_rate" in item
    assert "hourly_funding_spread_bps" in item
    assert item["holding_hours"] == 8
    assert item["estimated_fee_bps"] == 10.0
    assert item["net_edge_bps"] > 0
    assert "funding_confidence_score" in item
    assert item["funding_confidence_label"] == "high"
    assert "risk_adjusted_edge_bps" in item
    assert "risk_flags" in item
    assert "mixed_funding_sources" in item["risk_flags"]
    assert item["opportunity_grade"] == "tradable"
    assert item["is_tradable"] is True
    assert item["reject_reasons"] == []


def test_get_opportunities_filters_non_positive_net_edge(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                MarketSnapshot(
                    exchange="binance",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTCUSDT",
                    mark_price=100.0,
                    funding_rate=0.0,
                    funding_rate_source="latest_reported",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
                MarketSnapshot(
                    exchange="okx",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTC-USDT-SWAP",
                    mark_price=100.06,
                    funding_rate=0.0,
                    funding_rate_source="current",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []


def test_get_opportunities_filters_low_confidence_funding(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                MarketSnapshot(
                    exchange="lighter",
                    venue_type="dex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="1",
                    mark_price=100.0,
                    funding_rate=-0.0004,
                    funding_rate_source="estimated_current",
                    funding_period_hours=4,
                    timestamp_ms=1710000100000,
                ),
                MarketSnapshot(
                    exchange="okx",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTC-USDT-SWAP",
                    mark_price=101.0,
                    funding_rate=0.0004,
                    funding_rate_source="last_settled_fallback",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []


def test_get_opportunities_keeps_watchlist_items(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                MarketSnapshot(
                    exchange="binance",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTCUSDT",
                    mark_price=100.0,
                    funding_rate=-0.0008,
                    funding_rate_source="estimated_current",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
                MarketSnapshot(
                    exchange="okx",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC-USDT-PERP",
                    instrument_id="BTC-USDT-SWAP",
                    mark_price=100.2,
                    funding_rate=0.0008,
                    funding_rate_source="current",
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "watchlist"
    assert item["is_tradable"] is False
    assert item["reject_reasons"] == ["mixed_funding_sources"]
