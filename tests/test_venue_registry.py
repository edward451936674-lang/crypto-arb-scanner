from app.venues.registry import list_venue_definitions


def test_venue_registry_contains_all_current_supported_venues() -> None:
    venues = list_venue_definitions()

    assert [item.venue_id.value for item in venues] == ["binance", "okx", "hyperliquid", "lighter"]
    assert all(item.capabilities.supports_snapshots for item in venues)
    assert all(item.capabilities.paper_supported for item in venues)
    assert all(item.capabilities.live_supported_now is False for item in venues)


def test_venue_registry_contains_execution_reality_metadata() -> None:
    venues = {item.venue_id.value: item for item in list_venue_definitions()}

    assert venues["binance"].capabilities.supports_rest_trading_api is True
    assert venues["okx"].capabilities.supports_rest_trading_api is True

    assert venues["hyperliquid"].capabilities.supports_signed_actions is True
    assert venues["lighter"].capabilities.supports_signed_actions is True

    assert venues["hyperliquid"].capabilities.supports_sdk_recommended is True
    assert venues["lighter"].capabilities.supports_sdk_recommended is True

    assert venues["binance"].capabilities.execution_interface_notes
    assert venues["okx"].capabilities.execution_interface_notes
    assert venues["hyperliquid"].capabilities.execution_interface_notes
    assert venues["lighter"].capabilities.execution_interface_notes
