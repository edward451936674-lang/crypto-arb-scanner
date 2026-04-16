from fastapi.testclient import TestClient

from app.main import app
from app.models.execution import (
    ExecutionBundlePreflight,
    ExecutionCandidate,
    ExecutionLegPreflight,
    ExecutionPolicyConfigSnapshot,
)
from app.models.observation import ObservationRecord
from app.services.execution_policy import evaluate_execution_policy_decision
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
    target_notional_usd: float | None = 1200.0,
    is_test: bool = False,
) -> ExecutionCandidate:
    return ExecutionCandidate(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        route_key=route_key,
        target_notional_usd=target_notional_usd,
        entry_reference_price_long=100.0,
        entry_reference_price_short=101.0,
        generated_at_ms=1,
        is_test=is_test,
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
        validation_errors=[],
        validation_warnings=[],
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
        validation_errors=[],
        validation_warnings=[],
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


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    route_key: str | None = None,
    target_notional_usd: float | None = 1200.0,
    long_price: float | None = 100.0,
    short_price: float | None = 101.0,
    is_test: bool = False,
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
            "test": is_test,
        },
    )


def test_execution_enabled_false_blocks_everything() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(),
        preflight=_preflight("ready"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=False,
            allow_test_execution=False,
            allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.allowed is False
    assert "execution_globally_disabled" in decision.block_reasons


def test_preflight_blocked_bundle_is_policy_blocked() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(),
        preflight=_preflight("blocked"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=True,
            allow_test_execution=True,
            allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.allowed is False
    assert "preflight_not_ready" in decision.block_reasons


def test_test_bundle_is_blocked_by_default() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(is_test=True),
        preflight=_preflight("ready"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=True,
            allow_test_execution=False,
            allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.allowed is False
    assert "test_bundle_not_allowed" in decision.block_reasons


def test_venue_allowlist_works() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(long_exchange="kraken"),
        preflight=_preflight("ready"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=True,
            allow_test_execution=True,
            allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.allowed is False
    assert "long_venue_not_allowed" in decision.block_reasons


def test_symbol_allowlist_and_blocked_symbols_work() -> None:
    config = ExecutionPolicyConfigSnapshot(
        execution_enabled=True,
        allow_test_execution=True,
        allowed_venues=["binance", "okx"],
        allowed_symbols=["BTC"],
        blocked_symbols=["ETH"],
    )

    denied_not_allowed = evaluate_execution_policy_decision(
        candidate=_candidate(symbol="SOL", route_key="SOL:binance->okx"),
        preflight=_preflight("ready"),
        config=config,
    )
    denied_blocked = evaluate_execution_policy_decision(
        candidate=_candidate(symbol="ETH", route_key="ETH:binance->okx"),
        preflight=_preflight("ready"),
        config=config,
    )

    assert "symbol_not_allowed" in denied_not_allowed.block_reasons
    assert "symbol_explicitly_blocked" in denied_blocked.block_reasons


def test_target_notional_missing_is_blocked() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(target_notional_usd=None),
        preflight=_preflight("ready"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=True,
            allow_test_execution=True,
            allowed_venues=["binance", "okx"],
        ),
    )

    assert decision.allowed is False
    assert "target_notional_missing" in decision.block_reasons


def test_max_target_notional_limit_works() -> None:
    decision = evaluate_execution_policy_decision(
        candidate=_candidate(target_notional_usd=1500.0),
        preflight=_preflight("ready"),
        config=ExecutionPolicyConfigSnapshot(
            execution_enabled=True,
            allow_test_execution=True,
            allowed_venues=["binance", "okx"],
            max_target_notional_usd=1000.0,
        ),
    )

    assert decision.allowed is False
    assert "target_notional_exceeds_limit" in decision.block_reasons


def test_policy_preview_endpoint_summary_counts_work(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", target_notional_usd=1000.0),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", target_notional_usd=None),
            _record(symbol="SOL", long_exchange="kraken", short_exchange="okx", target_notional_usd=700.0),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", False, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_symbols", [], raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_blocked_symbols", [], raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_max_target_notional_usd", 1_200.0, raising=False)

    client = TestClient(app)
    response = client.get("/api/v1/execution/policy-preview", params={"top_n": 10, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_count"] == 3
    assert payload["decision_count"] == 3
    assert payload["allowed_count"] == 1
    assert payload["blocked_count"] == 2
    assert payload["preview_only"] is True
    assert payload["is_live"] is False


def test_policy_preview_endpoint_has_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in policy preview API")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.get("/api/v1/execution/policy-preview")

    assert response.status_code == 200
    assert response.json()["decision_count"] == 1


def test_observations_schema_is_unchanged_for_policy_workflow(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    with store._connect() as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS
