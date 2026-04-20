from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import ExecutionCandidate, ExecutionCredentialReadinessConfigSnapshot
from app.models.observation import ObservationRecord
from app.services.execution_credential_readiness import evaluate_execution_credential_readiness_decision
from app.storage.observations import ObservationStore

EXPECTED_OBSERVATIONS_COLUMNS = [
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


def _candidate(
    *,
    symbol: str = "BTC",
    long_exchange: str = "binance",
    short_exchange: str = "okx",
    route_key: str = "BTC:binance->okx",
) -> ExecutionCandidate:
    return ExecutionCandidate(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        route_key=route_key,
        target_notional_usd=1200.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )


def _config(**overrides: object) -> ExecutionCredentialReadinessConfigSnapshot:
    base = ExecutionCredentialReadinessConfigSnapshot(
        execution_credential_readiness_enabled=True,
        execution_credential_fixture_configured_venues={"binance": True, "okx": True},
    )
    return base.model_copy(update=overrides)


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
            "target_notional_usd": 1200.0,
            "long_price": 100.0,
            "short_price": 101.0,
            "replay_passes_min_trade_gate": True,
            "replay_confidence_label": "high",
            "risk_flags": [],
            "test": False,
        },
    )


def test_credential_readiness_gate_disabled_blocks_conservatively() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_readiness_enabled=False),
    )

    assert decision.allowed is False
    assert "credential_readiness_disabled" in decision.block_reasons


def test_missing_long_venue_fixture_blocks_with_unknown() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_fixture_configured_venues={"okx": True}),
    )

    assert decision.allowed is False
    assert "long_credentials_status_unknown" in decision.block_reasons


def test_missing_short_venue_fixture_blocks_with_unknown() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_fixture_configured_venues={"binance": True}),
    )

    assert decision.allowed is False
    assert "short_credentials_status_unknown" in decision.block_reasons


def test_long_fixture_false_blocks_with_missing() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_fixture_configured_venues={"binance": False, "okx": True}),
    )

    assert decision.allowed is False
    assert "long_credentials_missing" in decision.block_reasons


def test_short_fixture_false_blocks_with_missing() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_fixture_configured_venues={"binance": True, "okx": False}),
    )

    assert decision.allowed is False
    assert "short_credentials_missing" in decision.block_reasons


def test_both_fixture_values_true_allows_decision() -> None:
    decision = evaluate_execution_credential_readiness_decision(
        candidate=_candidate(),
        config=_config(execution_credential_fixture_configured_venues={"binance": True, "okx": True}),
    )

    assert decision.allowed is True
    assert decision.credential_readiness_status == "allowed"
    assert decision.block_reasons == []


def test_credential_readiness_preview_endpoint_summary_counts_work(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", True, raising=False)
    monkeypatch.setattr(
        "app.main.settings.execution_credential_fixture_configured_venues",
        {"binance": True, "okx": True},
        raising=False,
    )

    client = TestClient(app)
    response = client.get("/api/v1/execution/credential-readiness-preview", params={"top_n": 10, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 2
    assert payload["decision_count"] == 2
    assert payload["allowed_count"] == 2
    assert payload["blocked_count"] == 0
    assert payload["preview_only"] is True
    assert payload["is_live"] is False


def test_credential_readiness_preview_endpoint_has_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in credential readiness preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.get("/api/v1/execution/credential-readiness-preview")

    assert response.status_code == 200
    assert response.json()["decision_count"] == 1


def test_observations_schema_is_unchanged_for_credential_readiness_workflow(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    with store._connect() as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS
