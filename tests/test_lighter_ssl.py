import asyncio
import ssl

import certifi

from app.core.config import Settings
from app.exchanges.lighter import LighterClient


class _StubResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_lighter_uses_certifi_ca_file() -> None:
    client = LighterClient(Settings())
    try:
        assert client._ca_file == certifi.where()
        assert isinstance(client._ssl_context, ssl.SSLContext)
    finally:
        asyncio.run(client.aclose())


def test_lighter_ws_connect_uses_ssl_context(monkeypatch) -> None:
    captured = {}

    class _DummyWs:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send(self, message):
            return None

    def _fake_connect(*args, **kwargs):
        captured["kwargs"] = kwargs
        return _DummyWs()

    client = LighterClient(Settings())
    monkeypatch.setattr("app.exchanges.lighter.websockets.connect", _fake_connect)

    try:
        result = asyncio.run(client._fetch_market_stats_ws({}))
        assert result == {}
        assert captured["kwargs"]["ssl"] is client._ssl_context
    finally:
        asyncio.run(client.aclose())


def test_lighter_market_map_uses_certifi_http_client(monkeypatch) -> None:
    client = LighterClient(Settings())

    async def _fail_if_called(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("base http client should not be used for Lighter market map")

    async def _fake_get(url):  # type: ignore[no-untyped-def]
        return _StubResponse([{"symbol": "BTC", "market_index": 1}])

    client.http.get = _fail_if_called  # type: ignore[method-assign]
    client._lighter_http.get = _fake_get  # type: ignore[method-assign]

    try:
        mapping = asyncio.run(client._fetch_market_id_map())
        assert mapping == {"BTC": 1}
    finally:
        asyncio.run(client.aclose())
