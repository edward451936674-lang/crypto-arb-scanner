import asyncio

from app.main import get_snapshots
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.market_data import MarketDataService


def test_get_snapshots_includes_funding_fields(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                MarketSnapshot(
                    exchange="binance",
                    venue_type="cex",
                    base_symbol="BTC",
                    normalized_symbol="BTC",
                    instrument_id="BTCUSDT",
                    mark_price=100000.0,
                    funding_rate=0.0001,
                    funding_rate_source="latest_reported",
                    funding_time_ms=1710000000000,
                    next_funding_time_ms=1710028800000,
                    funding_period_hours=8,
                    timestamp_ms=1710000100000,
                )
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_snapshots(symbols="BTC"))

    snapshot = response["snapshots"][0]
    assert snapshot["funding_time_ms"] == 1710000000000
    assert snapshot["next_funding_time_ms"] == 1710028800000
    assert snapshot["funding_period_hours"] == 8
    assert snapshot["hourly_funding_rate"] == 0.0000125
    assert snapshot["hourly_funding_rate_bps"] == 0.125
