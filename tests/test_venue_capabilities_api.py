from fastapi.testclient import TestClient

from app.main import app


def test_venue_capabilities_endpoint_returns_registry_without_network(monkeypatch) -> None:
    def _fail_network(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("network should not be used by venue capabilities endpoint")

    monkeypatch.setattr("httpx.AsyncClient.request", _fail_network)
    client = TestClient(app)

    response = client.get("/api/v1/execution/venue-capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert payload["live_execution_enabled"] is False
    assert payload["live_execution_status"] == "not_enabled_in_this_repo"
    assert len(payload["venues"]) == 4
    assert [item["venue_id"] for item in payload["venues"]] == ["binance", "okx", "hyperliquid", "lighter"]
    assert all(item["capabilities"]["live_supported_now"] is False for item in payload["venues"])

    assert payload["execution_styles"] == {
        "classic_api_style_venues": ["binance", "okx"],
        "signed_action_style_venues": ["hyperliquid", "lighter"],
        "sdk_recommended_venues": ["hyperliquid", "lighter"],
    }

    by_venue = {item["venue_id"]: item for item in payload["venues"]}
    assert by_venue["binance"]["capabilities"]["supports_rest_trading_api"] is True
    assert by_venue["okx"]["capabilities"]["supports_private_websocket_trading"] is True
    assert by_venue["hyperliquid"]["capabilities"]["supports_signed_actions"] is True
    assert by_venue["lighter"]["capabilities"]["supports_sdk_recommended"] is True
