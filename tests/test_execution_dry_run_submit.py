import asyncio

from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import (
    AdapterExecutionResult,
    ExecutionCandidate,
    VenueRequestPreview,
    VenueTranslationResult,
)
from app.models.observation import ObservationRecord
from app.services.execution_dry_run_submit import simulate_dry_run_execution_attempt
from app.storage.observations import ObservationStore


class _RejectingOkxAdapter:
    async def place_order(self, intent):
        preview = VenueRequestPreview(
            venue_id="okx",
            operation="place_order",
            route_key=intent.route_key,
            intent_ref=intent.client_order_id,
            payload={"instId": intent.symbol},
            validation_errors=["simulated_submit_reject"],
            validation_warnings=[],
            metadata={"stub": True},
            is_live=False,
        )
        translation = VenueTranslationResult(
            venue_id="okx",
            operation="place_order",
            normalized_intent_id=intent.client_order_id,
            route_key=intent.route_key,
            symbol=intent.symbol,
            preview=preview,
            accepted=False,
            is_live=False,
        )
        return AdapterExecutionResult(
            venue_id="okx",
            operation="place_order",
            accepted=False,
            message="simulated_submit_reject",
            translation=translation,
            is_live=False,
        )


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


def test_dry_run_submit_ready_bundle_becomes_accepted() -> None:
    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )

    attempt = asyncio.run(simulate_dry_run_execution_attempt(candidate))

    assert attempt.bundle_status == "accepted"
    assert attempt.submitted_leg_count == 2
    assert attempt.accepted_leg_count == 2
    assert attempt.failure_reasons == []


def test_dry_run_submit_preflight_blocked_bundle_is_blocked() -> None:
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

    attempt = asyncio.run(simulate_dry_run_execution_attempt(candidate))

    assert attempt.bundle_status == "blocked"
    assert "preflight_blocked" in attempt.failure_reasons
    assert "quantity_unresolved" in attempt.failure_reasons
    assert attempt.submitted_leg_count == 0


def test_dry_run_submit_one_leg_rejected_becomes_failed(monkeypatch) -> None:
    from app.execution_adapters.registry import get_execution_adapter as registry_get_execution_adapter

    def _patched_get_execution_adapter(venue_id: str):
        if venue_id == "okx":
            return _RejectingOkxAdapter()
        return registry_get_execution_adapter(venue_id)

    monkeypatch.setattr("app.services.execution_dry_run_submit.get_execution_adapter", _patched_get_execution_adapter)

    candidate = ExecutionCandidate(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        route_key="BTC:binance->okx",
        target_notional_usd=1000.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )

    attempt = asyncio.run(simulate_dry_run_execution_attempt(candidate))

    assert attempt.bundle_status == "failed"
    assert "short_leg_submit_rejected" in attempt.failure_reasons
    assert "validation_error" in attempt.failure_reasons
    assert attempt.accepted_leg_count == 1


def test_dry_run_submit_endpoint_summary_counts_work(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="hyperliquid"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx"),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", short_price=None),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)

    from app.execution_adapters.registry import get_execution_adapter as registry_get_execution_adapter

    def _patched_get_execution_adapter(venue_id: str):
        if venue_id == "okx":
            return _RejectingOkxAdapter()
        return registry_get_execution_adapter(venue_id)

    monkeypatch.setattr("app.services.execution_dry_run_submit.get_execution_adapter", _patched_get_execution_adapter)

    client = TestClient(app)
    response = client.post("/api/v1/execution/dry-run-submit", json={"top_n": 10, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 3
    assert payload["attempt_count"] == 3
    assert payload["accepted_bundle_count"] == 1
    assert payload["blocked_bundle_count"] == 1
    assert payload["failed_bundle_count"] == 1
    assert payload["preview_only"] is True
    assert payload["is_live"] is False


def test_dry_run_submit_endpoint_has_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in dry-run submit API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.post("/api/v1/execution/dry-run-submit", json={"top_n": 5})

    assert response.status_code == 200
    assert response.json()["attempt_count"] == 1


def test_observations_schema_is_unchanged_for_dry_run_submit_workflow(tmp_path) -> None:
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
