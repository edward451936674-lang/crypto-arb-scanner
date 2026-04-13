import asyncio

from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import ExecutionCandidate
from app.models.observation import ObservationRecord
from app.services.execution_preflight import evaluate_execution_bundle_preflight
from app.storage.observations import ObservationStore


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    route_key: str | None = None,
    target_notional_usd: float | None = 1200.0,
    long_price: float | None = 100.0,
    short_price: float | None = 101.0,
) -> ObservationRecord:
    route = route_key if route_key is not None else f"{symbol}:{long_exchange}->{short_exchange}"
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


def test_preflight_ready_when_both_legs_resolved_and_clean() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=125.0,
        generated_at_ms=1,
    )

    bundle = asyncio.run(evaluate_execution_bundle_preflight(candidate))

    assert bundle.bundle_status == "ready"
    assert bundle.is_executable_bundle is True
    assert bundle.blockers == []


def test_preflight_blocked_when_one_leg_quantity_unresolved() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=None,
        generated_at_ms=1,
    )

    bundle = asyncio.run(evaluate_execution_bundle_preflight(candidate))

    assert bundle.bundle_status == "blocked"
    assert "short_quantity_unresolved" in bundle.blockers


def test_preflight_blocked_on_venue_validation_error() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        target_notional_usd=0.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )

    bundle = asyncio.run(evaluate_execution_bundle_preflight(candidate))

    assert bundle.bundle_status == "blocked"
    assert "long_validation_error" in bundle.blockers
    assert "short_validation_error" in bundle.blockers


def test_preflight_blocked_for_unsupported_venue() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="kraken",
        short_exchange="okx",
        route_key="BTC:kraken->okx",
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )

    bundle = asyncio.run(evaluate_execution_bundle_preflight(candidate))

    assert bundle.bundle_status == "blocked"
    assert "unsupported_venue" in bundle.blockers


def test_preflight_preview_endpoint_summary_counts_work(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", short_price=None),
            _record(symbol="SOL", long_exchange="kraken", short_exchange="okx"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.get("/api/v1/execution/preflight-preview", params={"top_n": 10, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 3
    assert payload["bundle_count"] == 3
    assert payload["ready_bundle_count"] == 1
    assert payload["blocked_bundle_count"] == 2
    assert payload["partial_bundle_count"] == 0
    assert payload["preview_only"] is True
    assert payload["is_live"] is False


def test_preflight_preview_endpoint_has_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in preflight preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.get("/api/v1/execution/preflight-preview")

    assert response.status_code == 200
    assert response.json()["bundle_count"] == 1


def test_observations_schema_is_unchanged_for_preflight_workflow(tmp_path) -> None:
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
