import asyncio

from app.core.config import Settings
from app.core.symbols import resolve_symbol_specs
from app.exchanges.binance import BINANCE_FUNDING_PERIOD_HOURS, HOUR_TO_MS, BinanceClient


class _StubResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


async def _run_fetch(payload: dict[str, object]):
    client = BinanceClient(Settings())

    async def _fake_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(payload)

    client.http.get = _fake_get  # type: ignore[method-assign]
    spec = resolve_symbol_specs(["BTC"])[0]
    snapshot = await client._fetch_one(spec)
    await client.aclose()
    return snapshot


def test_binance_derives_funding_time_from_next_funding_time() -> None:
    next_funding_time_ms = 1710028800000
    payload = {
        "symbol": "BTCUSDT",
        "markPrice": "100000.0",
        "indexPrice": "99950.0",
        "lastFundingRate": "0.0001",
        "nextFundingTime": str(next_funding_time_ms),
        "time": 1710000100000,
    }

    snapshot = asyncio.run(_run_fetch(payload))

    assert snapshot.next_funding_time_ms == next_funding_time_ms
    assert snapshot.funding_period_hours == BINANCE_FUNDING_PERIOD_HOURS
    assert snapshot.funding_time_ms == next_funding_time_ms - (BINANCE_FUNDING_PERIOD_HOURS * HOUR_TO_MS)


def test_binance_keeps_funding_time_none_without_next_funding_time() -> None:
    payload = {
        "symbol": "BTCUSDT",
        "markPrice": "100000.0",
        "indexPrice": "99950.0",
        "lastFundingRate": "0.0001",
        "nextFundingTime": None,
        "time": 1710000100000,
    }

    snapshot = asyncio.run(_run_fetch(payload))

    assert snapshot.next_funding_time_ms is None
    assert snapshot.funding_time_ms is None
    assert snapshot.funding_period_hours == BINANCE_FUNDING_PERIOD_HOURS
