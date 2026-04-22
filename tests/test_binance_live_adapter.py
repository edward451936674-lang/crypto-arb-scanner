import asyncio

from app.core.config import Settings
from app.execution_adapters.binance_live import (
    BinanceExecutionAdapterLive,
    _build_signed_params,
    load_binance_credentials,
)
from app.execution_adapters.binance_rules import parse_binance_exchange_info_symbol_rules
from app.execution_adapters.registry import get_execution_adapter_capability
from app.models.execution import CancelIntent, OrderIntent


class MockTransport:
    def __init__(self, responses: dict[str, dict[str, object]] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[dict[str, object]] = []

    async def request(self, *, method: str, url: str, headers: dict[str, str], params: dict[str, object]) -> dict[str, object]:
        self.calls.append({"method": method, "url": url, "headers": headers, "params": params})
        if url.endswith("/fapi/v1/exchangeInfo"):
            return dict(self.responses.get("exchangeInfo", {}))
        if url.endswith("/fapi/v1/order") and method == "POST":
            return dict(self.responses.get("place_order", {}))
        if url.endswith("/fapi/v1/order") and method == "DELETE":
            return dict(self.responses.get("cancel_order", {}))
        if url.endswith("/fapi/v1/order") and method == "GET":
            return dict(self.responses.get("order_status", {}))
        return {}


def _exchange_info_payload(*, symbol: str = "BTCUSDT", include_min_notional: bool = False) -> dict[str, object]:
    filters: list[dict[str, str]] = [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
        {"filterType": "LOT_SIZE", "minQty": "0.010", "stepSize": "0.010"},
        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.100", "stepSize": "0.100"},
    ]
    if include_min_notional:
        filters.append({"filterType": "MIN_NOTIONAL", "minNotional": "100"})
    return {"symbols": [{"symbol": symbol, "orderTypes": ["LIMIT", "MARKET"], "filters": filters}]}


def test_binance_exchange_info_parsing_symbol_rules() -> None:
    rules = parse_binance_exchange_info_symbol_rules(_exchange_info_payload(include_min_notional=True), "BTCUSDT")
    assert rules is not None
    assert str(rules.price_tick_size) == "0.10"
    assert str(rules.lot_size_min_qty) == "0.010"
    assert str(rules.market_lot_size_step_size) == "0.100"
    assert "MARKET" in rules.allowed_order_types
    assert str(rules.min_notional) == "100"


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
    transport = MockTransport(
        {
            "exchangeInfo": _exchange_info_payload(),
            "place_order": {"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"},
        }
    )
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
    assert transport.calls[0]["url"].endswith("/fapi/v1/exchangeInfo")
    assert transport.calls[1]["method"] == "POST"
    assert transport.calls[1]["url"].endswith("/fapi/v1/order")
    assert transport.calls[1]["headers"]["X-MBX-APIKEY"] == "a" * 16
    assert "signature" in transport.calls[1]["params"]


def test_binance_testnet_environment_resolves_to_testnet_endpoint(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    settings = Settings(
        binance_execution_environment="testnet",
        binance_testnet_base_url="https://testnet.binancefuture.com",
        binance_live_base_url="https://fapi.binance.com",
    )
    transport = MockTransport(
        {
            "exchangeInfo": _exchange_info_payload(),
            "place_order": {"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"},
        }
    )
    adapter = BinanceExecutionAdapterLive(settings=settings, transport=transport, clock_ms=lambda: 123456)
    asyncio.run(
        adapter.place_order(
            OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0)
        )
    )
    assert transport.calls[0]["url"].startswith("https://testnet.binancefuture.com/")


def test_binance_cancel_order_uses_mocked_transport(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"cancel_order": {"status": "CANCELED", "symbol": "BTCUSDT", "orderId": 7}})
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
    transport = MockTransport({"order_status": {"status": "FILLED", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "1"}})
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

    transport = MockTransport(
        {
            "exchangeInfo": _exchange_info_payload(),
            "place_order": {"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"},
        }
    )
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)
    result = asyncio.run(
        adapter.place_order(
            OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0)
        )
    )
    assert result.accepted is True
    assert len(transport.calls) == 2


def test_binance_place_order_symbol_not_found_failure(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"exchangeInfo": _exchange_info_payload(symbol="ETHUSDT")})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)

    result = asyncio.run(
        adapter.place_order(OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0))
    )
    assert result.accepted is False
    assert "binance_symbol_not_found" in result.translation.preview.validation_errors


def test_binance_place_order_unsupported_order_type_failure(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    payload = _exchange_info_payload()
    payload["symbols"][0]["orderTypes"] = ["LIMIT"]  # type: ignore[index]
    transport = MockTransport({"exchangeInfo": payload})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)

    result = asyncio.run(
        adapter.place_order(OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0))
    )
    assert result.accepted is False
    assert "unsupported_order_type" in result.translation.preview.validation_errors


def test_binance_quantity_and_price_normalization(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport(
        {
            "exchangeInfo": _exchange_info_payload(),
            "place_order": {"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "0.2", "executedQty": "0"},
        }
    )
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)
    result = asyncio.run(
        adapter.place_order(
            OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="limit", quantity=0.123, price=100.27)
        )
    )
    assert result.accepted is True
    assert "quantity_step_misaligned" in result.translation.preview.validation_warnings
    assert "price_tick_misaligned" in result.translation.preview.validation_warnings
    assert result.translation.preview.metadata["final_quantity"] == "0.12"
    assert result.translation.preview.metadata["final_price"] == "100.2"


def test_binance_quantity_below_min_qty_failure(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"exchangeInfo": _exchange_info_payload()})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)
    result = asyncio.run(
        adapter.place_order(OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=0.05))
    )
    assert result.accepted is False
    assert "quantity_below_min_qty" in result.translation.preview.validation_errors


def test_binance_min_notional_only_when_rule_present(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport({"exchangeInfo": _exchange_info_payload(include_min_notional=True)})
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)
    result = asyncio.run(
        adapter.place_order(
            OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="limit", quantity=0.2, price=100.2)
        )
    )
    assert result.accepted is False
    assert "min_notional_not_met" in result.translation.preview.validation_errors


def test_binance_client_order_id_generated_and_invalid_cases(monkeypatch) -> None:
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)
    transport = MockTransport(
        {
            "exchangeInfo": _exchange_info_payload(),
            "place_order": {"status": "NEW", "symbol": "BTCUSDT", "orderId": 7, "origQty": "1", "executedQty": "0"},
        }
    )
    adapter = BinanceExecutionAdapterLive(settings=Settings(), transport=transport, clock_ms=lambda: 123456)

    generated = asyncio.run(
        adapter.place_order(OrderIntent(venue_id="binance", symbol="BTCUSDT", side="buy", order_type="market", quantity=1.0))
    )
    assert generated.accepted is True
    assert "client_order_id_generated" in generated.translation.preview.validation_warnings
    assert generated.translation.preview.metadata["final_client_order_id"].startswith("arbp-")

    invalid = asyncio.run(
        adapter.place_order(
            OrderIntent(
                venue_id="binance",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                quantity=1.0,
                client_order_id="bad id!",
            )
        )
    )
    assert invalid.accepted is False
    assert "client_order_id_invalid" in invalid.translation.preview.validation_errors

    too_long = asyncio.run(
        adapter.place_order(
            OrderIntent(
                venue_id="binance",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                quantity=1.0,
                client_order_id="x" * 37,
            )
        )
    )
    assert too_long.accepted is False
    assert "client_order_id_too_long" in too_long.translation.preview.validation_errors
