import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import ExecutionCandidate, OrderIntent
from app.models.observation import ObservationRecord
from app.services.execution_intents import candidate_to_order_intents
from app.services.execution_quantity_resolver import quantity_resolver
from app.storage.observations import ObservationStore


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    target_notional_usd: float | None = 1000.0,
    long_price: float | None = 100.0,
    short_price: float | None = 101.0,
) -> ObservationRecord:
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
            "target_notional_usd": target_notional_usd,
            "long_price": long_price,
            "short_price": short_price,
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
    assert long_intent.quantity == 50.0
    assert short_intent.quantity == 49.504950495049506
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


def test_quantity_resolver_resolves_both_legs_when_target_notional_and_prices_present() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        generated_at_ms=1,
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=125.0,
    )
    result = quantity_resolver.resolve(candidate)

    assert result.quantity_resolution_status == "resolved"
    assert result.quantity_resolution_source == "target_notional_and_reference_price"
    assert result.resolved_quantity_long == 10.0
    assert result.resolved_quantity_short == 8.0
    assert result.warnings == []


def test_quantity_resolver_resolves_only_available_leg_when_other_price_missing() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        generated_at_ms=1,
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=None,
    )
    result = quantity_resolver.resolve(candidate)

    assert result.quantity_resolution_status == "partial"
    assert result.resolved_quantity_long == 10.0
    assert result.resolved_quantity_short is None
    assert "short_reference_price_missing" in result.warnings


def test_quantity_resolver_non_positive_prices_do_not_resolve_quantity() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        generated_at_ms=1,
        target_notional_usd=1000.0,
        entry_reference_price_long=0.0,
        entry_reference_price_short=-1.0,
    )
    result = quantity_resolver.resolve(candidate)

    assert result.quantity_resolution_status == "unavailable"
    assert result.resolved_quantity_long is None
    assert result.resolved_quantity_short is None
    assert "long_reference_price_non_positive" in result.warnings
    assert "short_reference_price_non_positive" in result.warnings


def test_quantity_resolver_target_position_pct_only_does_not_fabricate_quantity() -> None:
    candidate = ExecutionCandidate(
        symbol="ETH",
        long_exchange="binance",
        short_exchange="okx",
        route_key="ETH:binance->okx",
        generated_at_ms=1,
        target_position_pct=0.2,
        target_notional_usd=None,
    )
    result = quantity_resolver.resolve(candidate)

    assert result.quantity_resolution_status == "unavailable"
    assert result.quantity_resolution_source == "target_position_pct_only"
    assert result.resolved_quantity_long is None
    assert result.resolved_quantity_short is None


@pytest.mark.parametrize("target_notional_usd", [0.0, -100.0])
def test_quantity_resolver_non_positive_target_notional_is_unavailable(target_notional_usd: float) -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        generated_at_ms=1,
        target_notional_usd=target_notional_usd,
        entry_reference_price_long=100.0,
        entry_reference_price_short=125.0,
    )
    result = quantity_resolver.resolve(candidate)

    assert result.quantity_resolution_status == "unavailable"
    assert result.quantity_resolution_source == "unavailable"
    assert result.resolved_quantity_long is None
    assert result.resolved_quantity_short is None
    assert "target_notional_usd_non_positive" in result.warnings


def test_order_intent_preview_endpoint_returns_translated_intents(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get(
        "/api/v1/execution/order-intent-preview",
        params={"route_keys": ["BTC:binance->okx"], "top_n": 5, "include_test": False},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 1
    assert payload["selected_candidate_count"] == 1
    assert payload["intent_count"] == 2
    assert payload["resolved_intent_count"] == 2
    assert payload["unresolved_intent_count"] == 0
    assert payload["quantity_resolution_statuses"] == ["resolved"]
    assert {item["side"] for item in payload["items"]} == {"buy", "sell"}
    assert {item["venue_id"] for item in payload["items"]} == {"binance", "okx"}
    assert all(item["quantity"] is not None for item in payload["items"])


def test_order_intent_preview_endpoint_does_not_require_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in order intent preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)
    client = TestClient(app)

    response = client.get("/api/v1/execution/order-intent-preview")

    assert response.status_code == 200
    assert response.json()["intent_count"] == 2


def test_order_intent_preview_endpoint_filters_by_query_route_keys(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get(
        "/api/v1/execution/order-intent-preview",
        params=[("route_keys", "ETH:binance->okx"), ("top_n", "5"), ("include_test", "false")],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 2
    assert payload["selected_candidate_count"] == 1
    assert payload["intent_count"] == 2
    assert {item["route_key"] for item in payload["items"]} == {"ETH:binance->okx"}


def test_order_intent_preview_endpoint_reports_unresolved_leg_when_reference_price_missing(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                target_notional_usd=1000.0,
                short_price=None,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get("/api/v1/execution/order-intent-preview", params={"top_n": 1, "include_test": False})
    assert response.status_code == 200
    payload = response.json()
    assert payload["resolved_intent_count"] == 1
    assert payload["unresolved_intent_count"] == 1
    assert payload["quantity_resolution_statuses"] == ["partial"]
    assert payload["unresolved_legs"][0]["leg"] == "short"


@pytest.mark.parametrize("target_notional_usd", [0.0, -100.0])
def test_order_intent_preview_endpoint_non_positive_target_notional_keeps_legs_unresolved(
    tmp_path, monkeypatch, target_notional_usd: float
) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                target_notional_usd=target_notional_usd,
                long_price=100.0,
                short_price=101.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get("/api/v1/execution/order-intent-preview", params={"top_n": 1, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolved_intent_count"] == 0
    assert payload["unresolved_intent_count"] == 2
    assert payload["quantity_resolution_statuses"] == ["unavailable"]
    assert payload["quantity_resolution_warnings"] == ["target_notional_usd_non_positive"]
    assert all(item["quantity"] is None for item in payload["items"])


def test_observations_schema_is_unchanged_for_execution_intent_workflow(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    with store._connect() as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]

    assert columns == [
        "id",
        "observed_at_ms",
        "symbol",
        "cluster_id",
        "long_exchange",
        "short_exchange",
        "estimated_net_edge_bps",
        "opportunity_grade",
        "execution_mode",
        "final_position_pct",
        "why_not_tradable",
        "replay_net_after_cost_bps",
        "replay_confidence_label",
        "replay_passes_min_trade_gate",
        "risk_flags",
        "replay_summary",
        "raw_opportunity_json",
    ]
