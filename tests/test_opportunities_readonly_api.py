import time

from fastapi.testclient import TestClient

from app.main import app
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.market_data import MarketDataService


def _snapshot(exchange: str, mark_price: float, funding_rate: float) -> MarketSnapshot:
    ts_ms = int(time.time() * 1000)
    return MarketSnapshot(
        exchange=exchange,
        venue_type="cex",
        base_symbol="BTC",
        normalized_symbol="BTC-USDT-PERP",
        instrument_id=f"{exchange}-BTC",
        mark_price=mark_price,
        index_price=mark_price,
        funding_rate=funding_rate,
        funding_period_hours=8,
        timestamp_ms=ts_ms,
    )


def test_opportunities_endpoint_returns_200_and_btc_opportunity(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", mark_price=100.0, funding_rate=-0.0001),
                _snapshot("okx", mark_price=101.0, funding_rate=0.0002),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"symbols": "BTC", "top_n": 10})

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert any(item["symbol"] == "BTC" for item in payload)
