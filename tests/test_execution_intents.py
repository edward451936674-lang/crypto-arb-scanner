from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import ExecutionCandidate, OrderIntent
from app.models.observation import ObservationRecord
from app.services.execution_intents import candidate_to_order_intents
from app.storage.observations import ObservationStore


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


def test_normalized_execution_models_serialize_cleanly() -> None:
    intent = OrderIntent(
        venue_id="binance",
        symbol="BTC",
        side="buy",
        order_type="market",
        quantity=0.03,
        route_key="BTC:binance->okx",
        metadata={"k": "v"},
    )

    dumped = intent.model_dump()
    roundtrip = OrderIntent.model_validate(dumped)

    assert dumped["venue_id"] == "binance"
    assert dumped["symbol"] == "BTC"
    assert dumped["is_live"] is False
    assert roundtrip == intent


def test_candidate_to_order_intents_translation_preserves_route_and_venues() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        generated_at_ms=1,
        target_position_pct=0.05,
        target_notional_usd=5000.0,
        max_slippage_bps=12.0,
        max_order_age_ms=1000,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
    )

    intents = candidate_to_order_intents(candidate)

    assert len(intents) == 2
    long_intent, short_intent = intents
    assert long_intent.route_key == candidate.route_key
    assert short_intent.route_key == candidate.route_key
    assert long_intent.venue_id == "binance"
    assert short_intent.venue_id == "okx"
    assert long_intent.side == "buy"
    assert short_intent.side == "sell"
    assert long_intent.quantity is None
    assert short_intent.quantity is None
    assert long_intent.target_position_pct == 0.05
    assert short_intent.target_notional_usd == 5000.0


def test_candidate_to_order_intents_does_not_use_target_position_pct_as_quantity() -> None:
    candidate = ExecutionCandidate(
        symbol="ETH",
        long_exchange="binance",
        short_exchange="okx",
        route_key="ETH:binance->okx",
        generated_at_ms=1,
        target_position_pct=0.25,
        target_notional_usd=1000.0,
    )
    intents = candidate_to_order_intents(candidate)
    assert all(item.quantity is None for item in intents)
    assert all(item.target_position_pct == 0.25 for item in intents)
    assert all(item.target_notional_usd == 1000.0 for item in intents)


def test_order_intent_preview_endpoint_returns_translated_intents(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.post(
        "/api/v1/execution/order-intent-preview",
        json={"route_keys": ["BTC:binance->okx"]},
        params={"top_n": 5, "include_test": False},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 1
    assert payload["selected_candidate_count"] == 1
    assert payload["intent_count"] == 2
    assert {item["side"] for item in payload["items"]} == {"buy", "sell"}
    assert {item["venue_id"] for item in payload["items"]} == {"binance", "okx"}
    assert all(item["quantity"] is None for item in payload["items"])


def test_order_intent_preview_endpoint_does_not_require_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in order intent preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)
    client = TestClient(app)

    response = client.post("/api/v1/execution/order-intent-preview")

    assert response.status_code == 200
    assert response.json()["intent_count"] == 2
