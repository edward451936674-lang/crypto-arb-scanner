from app.venues.registry import list_venue_definitions


def test_venue_registry_contains_all_current_supported_venues() -> None:
    venues = list_venue_definitions()

    assert [item.venue_id.value for item in venues] == ["binance", "okx", "hyperliquid", "lighter"]
    assert all(item.capabilities.supports_snapshots for item in venues)
    assert all(item.capabilities.paper_supported for item in venues)
    assert all(item.capabilities.live_supported_now is False for item in venues)
