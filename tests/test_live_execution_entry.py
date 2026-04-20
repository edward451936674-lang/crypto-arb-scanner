from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import (
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionCredentialReadinessDecision,
    ExecutionLegPreflight,
    ExecutionPolicyDecision,
    LiveExecutionEntryConfigSnapshot,
)
from app.models.observation import ObservationRecord
from app.services.live_execution_entry import evaluate_live_execution_entry_decision
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


def _candidate(*, long_exchange: str = "binance", short_exchange: str = "okx") -> ExecutionCandidate:
    return ExecutionCandidate(
        symbol="BTC",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        route_key=f"BTC:{long_exchange}->{short_exchange}",
        target_notional_usd=1200.0,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
    )


def _preflight(bundle_status: str = "ready") -> ExecutionBundlePreflight:
    long_leg = ExecutionLegPreflight(
        venue_id="binance",
        side="buy",
        symbol="BTC",
        route_key="BTC:binance->okx",
        quantity=1.0,
        quantity_resolution_status="resolved",
        request_preview_available=True,
        supported_venue=True,
        is_ready=bundle_status == "ready",
    )
    short_leg = ExecutionLegPreflight(
        venue_id="okx",
        side="sell",
        symbol="BTC",
        route_key="BTC:binance->okx",
        quantity=1.0,
        quantity_resolution_status="resolved",
        request_preview_available=True,
        supported_venue=True,
        is_ready=bundle_status == "ready",
    )
    return ExecutionBundlePreflight(
        route_key="BTC:binance->okx",
        symbol="BTC",
        long_leg=long_leg,
        short_leg=short_leg,
        bundle_status=bundle_status,
        blockers=[] if bundle_status == "ready" else ["long_validation_error"],
        warnings=[],
        is_executable_bundle=bundle_status == "ready",
        preview_only=True,
        is_live=False,
    )


def _policy_decision(*, policy_status: str = "allowed") -> ExecutionPolicyDecision:
    return ExecutionPolicyDecision(
        route_key="BTC:binance->okx",
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        bundle_status_from_preflight="ready",
        policy_status=policy_status,
        allowed=policy_status == "allowed",
        block_reasons=[] if policy_status == "allowed" else ["preflight_not_ready"],
        warnings=[],
        preview_only=True,
        is_live=False,
    )


def _credential_readiness_decision(*, status: str = "blocked", block_reasons: list[str] | None = None) -> ExecutionCredentialReadinessDecision:
    reasons = block_reasons if block_reasons is not None else (["credential_readiness_disabled"] if status == "blocked" else [])
    return ExecutionCredentialReadinessDecision(
        route_key="BTC:binance->okx",
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        credential_readiness_status=status,
        allowed=status == "allowed",
        block_reasons=reasons,
        warnings=[],
        long_credentials_configured=True if status == "allowed" else None,
        short_credentials_configured=True if status == "allowed" else None,
        preview_only=True,
        is_live=False,
    )


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    route_key: str | None = None,
    target_notional_usd: float | None = 1200.0,
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
            "long_price": 100.0,
            "short_price": 101.0,
            "replay_passes_min_trade_gate": True,
            "replay_confidence_label": "high",
            "risk_flags": [],
            "test": False,
        },
    )


def test_live_execution_enabled_false_blocks_everything() -> None:
    decision = evaluate_live_execution_entry_decision(
        candidate=_candidate(),
        preflight=_preflight("ready"),
        policy_decision=_policy_decision(policy_status="allowed"),
        credential_readiness_decision=_credential_readiness_decision(),
        config=LiveExecutionEntryConfigSnapshot(
            live_execution_enabled=False,
            live_execution_allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.entry_status == "blocked"
    assert "live_execution_not_enabled" in decision.block_reasons


def test_policy_blocked_bundle_is_blocked_at_live_entry() -> None:
    decision = evaluate_live_execution_entry_decision(
        candidate=_candidate(),
        preflight=_preflight("ready"),
        policy_decision=_policy_decision(policy_status="blocked"),
        credential_readiness_decision=_credential_readiness_decision(status="allowed"),
        config=LiveExecutionEntryConfigSnapshot(live_execution_enabled=True),
    )

    assert decision.allowed_to_enter_live_path is False
    assert "policy_blocked" in decision.block_reasons


def test_venues_not_live_enabled_are_blocked() -> None:
    decision = evaluate_live_execution_entry_decision(
        candidate=_candidate(long_exchange="binance", short_exchange="okx"),
        preflight=_preflight("ready"),
        policy_decision=_policy_decision(policy_status="allowed"),
        credential_readiness_decision=_credential_readiness_decision(status="allowed"),
        config=LiveExecutionEntryConfigSnapshot(live_execution_enabled=True),
    )

    assert "venue_not_live_enabled" in decision.block_reasons


def test_stub_only_adapters_are_blocked() -> None:
    decision = evaluate_live_execution_entry_decision(
        candidate=_candidate(),
        preflight=_preflight("ready"),
        policy_decision=_policy_decision(policy_status="allowed"),
        credential_readiness_decision=_credential_readiness_decision(status="allowed"),
        config=LiveExecutionEntryConfigSnapshot(live_execution_enabled=True),
    )

    assert "adapter_is_stub_only" in decision.block_reasons


def test_credential_readiness_blocks_live_entry() -> None:
    decision = evaluate_live_execution_entry_decision(
        candidate=_candidate(),
        preflight=_preflight("ready"),
        policy_decision=_policy_decision(policy_status="allowed"),
        credential_readiness_decision=_credential_readiness_decision(
            status="blocked",
            block_reasons=["long_credentials_missing"],
        ),
        config=LiveExecutionEntryConfigSnapshot(live_execution_enabled=True),
    )

    assert "credential_readiness_blocked" in decision.block_reasons
    assert "credential_readiness:long_credentials_missing" in decision.warnings


def test_live_entry_preview_endpoint_summary_counts_work(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", target_notional_usd=1000.0),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", target_notional_usd=None),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", False, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_symbols", [], raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_blocked_symbols", [], raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_enabled", False, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", False, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_fixture_configured_venues", {}, raising=False)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-entry-preview", params={"top_n": 10, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 2
    assert payload["decision_count"] == 2
    assert payload["allowed_count"] == 0
    assert payload["blocked_count"] == 2
    assert payload["preview_only"] is True
    assert payload["is_live"] is False


def test_live_entry_preview_endpoint_has_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", False, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_fixture_configured_venues", {}, raising=False)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in live entry preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-entry-preview")

    assert response.status_code == 200
    assert response.json()["decision_count"] == 1


def test_live_entry_preview_uses_credential_readiness_decision(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx", target_notional_usd=1000.0)])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", True, raising=False)
    monkeypatch.setattr(
        "app.main.settings.execution_credential_fixture_configured_venues",
        {"binance": True, "okx": True},
        raising=False,
    )

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-entry-preview")

    assert response.status_code == 200
    payload = response.json()
    assert payload["decision_count"] == 1
    assert "credential_readiness_blocked" not in payload["items"][0]["block_reasons"]


def test_observations_schema_is_unchanged_for_live_entry_workflow(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    with store._connect() as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS
