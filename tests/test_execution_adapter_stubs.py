import asyncio

from fastapi.testclient import TestClient

from app.execution_adapters.registry import get_execution_adapter
from app.execution_adapters.stubs import (
    BinanceExecutionAdapterStub,
    HyperliquidExecutionAdapterStub,
    LighterExecutionAdapterStub,
    OkxExecutionAdapterStub,
)
from app.main import app
from app.models.execution import OrderIntent
from app.models.observation import ObservationRecord
from app.storage.observations import ObservationStore


async def _translate_with(adapter, *, venue: str, order_type: str = "limit", quantity: float | None = 1.2):
    intent = OrderIntent(
        venue_id=venue,
        symbol="BTC",
        side="buy",
        order_type=order_type,
        quantity=quantity,
        price=30000.0 if order_type == "limit" else None,
        time_in_force="gtc" if order_type == "limit" else None,
        reduce_only=False,
        client_order_id=f"cid-{venue}",
        route_key="BTC:binance->okx",
        metadata={"debug": True},
    )
    return await adapter.place_order(intent)


def _record(*, symbol: str, long_exchange: str, short_exchange: str) -> ObservationRecord:
    route = f"{symbol}:{long_exchange}->{short_exchange}"
    return ObservationRecord(
        observed_at_ms=1_700_000_000_000,
        symbol=symbol,
        cluster_id=f"{symbol}|{long_exchange}|{short_exchange}",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        estimated_net_edge_bps=14.0,
        opportunity_grade="tradable",
        execution_mode="normal",
        final_position_pct=0.03,
        why_not_tradable=None,
        replay_net_after_cost_bps=16.0,
        replay_confidence_label="high",
        replay_passes_min_trade_gate=True,
        risk_flags=[],
        raw_opportunity_json={
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "route_key": route,
            "opportunity_type": "tradable",
            "execution_mode": "normal",
            "final_position_pct": 0.03,
            "risk_adjusted_edge_bps": 22.0,
            "replay_net_after_cost_bps": 16.0,
            "estimated_net_edge_bps": 14.0,
            "long_price": 100.0,
            "short_price": 101.0,
            "replay_passes_min_trade_gate": True,
            "replay_confidence_label": "high",
            "risk_flags": [],
            "test": False,
        },
    )


def test_classic_venue_stubs_produce_rest_style_previews() -> None:
    binance_result = asyncio.run(_translate_with(BinanceExecutionAdapterStub(), venue="binance"))
    okx_result = asyncio.run(_translate_with(OkxExecutionAdapterStub(), venue="okx"))

    assert binance_result.accepted is True
    assert binance_result.translation.preview.payload["symbol"] == "BTC"
    assert binance_result.translation.preview.payload["type"] == "LIMIT"
    assert binance_result.translation.preview.payload["quantity"] == 1.2

    assert okx_result.accepted is True
    assert okx_result.translation.preview.payload["instId"] == "BTC"
    assert okx_result.translation.preview.payload["ordType"] == "limit"
    assert okx_result.translation.preview.payload["sz"] == 1.2


def test_signed_action_stubs_produce_action_or_transaction_style_previews() -> None:
    hyperliquid_result = asyncio.run(_translate_with(HyperliquidExecutionAdapterStub(), venue="hyperliquid"))
    lighter_result = asyncio.run(_translate_with(LighterExecutionAdapterStub(), venue="lighter"))

    assert hyperliquid_result.accepted is True
    assert hyperliquid_result.translation.preview.payload["action"]["type"] == "order"
    assert hyperliquid_result.translation.preview.payload["signature"] == "not_implemented"

    assert lighter_result.accepted is True
    assert lighter_result.translation.preview.payload["tx"]["kind"] == "place_order"
    assert lighter_result.translation.preview.payload["auth"]["signature"] == "not_implemented"


def test_missing_required_fields_return_validation_errors() -> None:
    result = asyncio.run(_translate_with(BinanceExecutionAdapterStub(), venue="binance", quantity=None))

    assert result.accepted is False
    assert "quantity_required" in result.translation.preview.validation_errors


def test_adapter_registry_returns_expected_adapter_types() -> None:
    assert isinstance(get_execution_adapter("binance"), BinanceExecutionAdapterStub)
    assert isinstance(get_execution_adapter("okx"), OkxExecutionAdapterStub)
    assert isinstance(get_execution_adapter("hyperliquid"), HyperliquidExecutionAdapterStub)
    assert isinstance(get_execution_adapter("lighter"), LighterExecutionAdapterStub)


def test_venue_request_preview_endpoint_translates_intents_by_venue(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(symbol="ETH", long_exchange="hyperliquid", short_exchange="lighter"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get(
        "/api/v1/execution/venue-request-preview",
        params=[("top_n", "10"), ("include_test", "false")],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 2
    assert payload["intent_count"] == 4
    assert payload["translation_count"] == 4
    assert payload["is_live"] is False
    translated_venues = {item["translation"]["venue_id"] for item in payload["items"]}
    assert translated_venues == {"binance", "okx", "hyperliquid", "lighter"}


def test_venue_request_preview_endpoint_is_network_free(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in venue request preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)
    client = TestClient(app)

    response = client.get("/api/v1/execution/venue-request-preview")
    assert response.status_code == 200
    assert response.json()["translation_count"] == 2
