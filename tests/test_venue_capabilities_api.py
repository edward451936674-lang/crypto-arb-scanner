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
