import asyncio

from app.core.config import Settings
from app.execution_adapters.binance_live import (
    BinanceExecutionAdapterLive,
    _build_signed_params,
    load_binance_credentials,
)
from app.execution_adapters.registry import get_execution_adapter_capability
from app.models.execution import CancelIntent, OrderIntent


class MockTransport:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    async def request(self, *, method: str, url: str, headers: dict[str, str], params: dict[str, object]) -> dict[str, object]:
        self.calls.append({"method": method, "url": url, "headers": headers, "params": params})
        return dict(self.response)


def test_adapter_capability_metadata_marks_binance_as_real_pilot() -> None:
    cap = get_execution_adapter_capability("binance")
    assert cap.supports_live_submit_now is True
    assert cap.supports_cancel_now is True
    assert cap.supports_order_status_now is True
    assert cap.credential_type == "binance_api_key_secret"
    assert cap.stub_only is False


def test_binance_credentials_missing_present_and_malformed_states(monkeypatch) -> None:
    monkeypatch.delenv("ARB_BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("ARB_BINANCE_API_SECRET", raising=False)
    creds, readiness = load_binance_credentials(Settings())
    assert creds is None
    assert readiness.status == "missing"

    monkeypatch.setenv("ARB_BINANCE_API_KEY", "abc")
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "short")
    creds, readiness = load_binance_credentials(Settings())
    assert creds is None
    assert readiness.status == "malformed"

    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    creds, readiness = load_binance_credentials(Settings())
    assert creds is not None
    assert readiness.status == "present"


def test_binance_signing_adds_signature_field() -> None:
    signed = _build_signed_params(params={"symbol": "BTCUSDT", "timestamp": 123, "recvWindow": 5000}, api_secret="secret")
    assert "signature" in signed
    assert len(str(signed["signature"])) == 64


def test_binance_place_order_uses_mocked_transport(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"})
    adapter = BinanceExecutionAdapterLive(
        settings=Settings(),
        transport=transport,
        clock_ms=lambda: 123456,
    )

    result = asyncio.run(
        adapter.place_order(
            OrderIntent(
                venue_id="binance",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                quantity=1.0,
                route_key="BTC:binance->binance",
                is_live=True,
            )
        )
    )

    assert result.accepted is True
    assert transport.calls[0]["method"] == "POST"
    assert transport.calls[0]["url"].endswith("/fapi/v1/order")
    assert transport.calls[0]["headers"]["X-MBX-APIKEY"] == "a" * 16
    assert "signature" in transport.calls[0]["params"]


def test_binance_cancel_order_uses_mocked_transport(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"status": "CANCELED", "symbol": "BTCUSDT", "orderId": 7})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)

    result = asyncio.run(
        adapter.cancel_order(
            CancelIntent(
                venue_id="binance",
                symbol="BTCUSDT",
                order_id="7",
                route_key="BTC:binance->binance",
                is_live=True,
            )
        )
    )

    assert result.accepted is True
    assert transport.calls[0]["method"] == "DELETE"


def test_binance_order_status_uses_mocked_transport(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"status": "FILLED", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "1"})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)

    snapshot = asyncio.run(adapter.get_order_status(order_id="7", symbol="BTCUSDT"))

    assert snapshot.status == "filled"
    assert transport.calls[0]["method"] == "GET"


def test_binance_adapter_missing_credentials_returns_explicit_failure(monkeypatch) -> None:
    monkeypatch.delenv("ARB_BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("ARB_BINANCE_API_SECRET", raising=False)
    adapter = BinanceExecutionAdapterLive(settings=Settings())

    result = asyncio.run(
        adapter.place_order(
            OrderIntent(
                venue_id="binance",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                quantity=1.0,
            )
        )
    )

    assert result.accepted is False
    assert result.message == "credentials_not_ready:missing"


def test_binance_live_adapter_tests_never_need_real_network_by_default(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)

    async def fail_request(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("real network should not be used in adapter unit tests")

    monkeypatch.setattr("httpx.AsyncClient.request", fail_request)

    transport = MockTransport({"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)
    result = asyncio.run(
        adapter.place_order(
            OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0)
        )
    )
    assert result.accepted is True
    assert len(transport.calls) == 1
