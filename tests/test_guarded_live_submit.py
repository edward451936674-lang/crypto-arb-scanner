from fastapi.testclient import TestClient

from app.main import app
from app.models.observation import ObservationRecord
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


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    route_key: str | None = None,
    target_notional_usd: float | None = 1200.0,
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
            "long_price": 100.0,
            "short_price": short_price,
            "replay_passes_min_trade_gate": True,
            "replay_confidence_label": "high",
            "risk_flags": [],
            "test": False,
        },
    )


def test_live_submit_blocks_when_guard_disabled(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_total_notional_usd", 5000.0, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_symbol_notional_usd", {"BTC": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_long_exchange_notional_usd", {"binance": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_short_exchange_notional_usd", {"okx": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_fixture_configured_venues", {"binance": True, "okx": True}, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", False, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_arm_token", "abc", raising=False)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-submit", json={"top_n": 10, "arm_token": "abc"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["attempt_count"] == 1
    assert payload["blocked_count"] == 1
    assert payload["submitted_count"] == 0
    assert "guarded_live_submit_disabled" in payload["items"][0]["block_reasons"]


def test_live_submit_blocks_on_missing_or_wrong_arm_token(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_arm_token", "expected-token", raising=False)

    client = TestClient(app)
    missing = client.post("/api/v1/execution/live-submit", json={"top_n": 10})
    wrong = client.post("/api/v1/execution/live-submit", json={"top_n": 10, "arm_token": "wrong"})

    assert missing.status_code == 200
    assert wrong.status_code == 200
    assert "arm_token_required" in missing.json()["items"][0]["block_reasons"]
    assert "arm_token_mismatch" in wrong.json()["items"][0]["block_reasons"]


def test_live_submit_propagates_upstream_blocked_layers(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx", short_price=None)])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", False, raising=False)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-submit", json={"top_n": 10})

    assert response.status_code == 200
    block_reasons = response.json()["items"][0]["block_reasons"]
    assert "preflight_blocked" in block_reasons
    assert "policy_blocked" in block_reasons
    assert "account_state_blocked" in block_reasons
    assert "credential_readiness_blocked" in block_reasons
    assert "live_entry_blocked" in block_reasons


def test_live_submit_attempts_are_persisted_and_queryable(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", route_key="BTC:binance->okx"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", route_key="ETH:binance->okx"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", False, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_persist_attempts", True, raising=False)

    client = TestClient(app)
    submit_response = client.post("/api/v1/execution/live-submit", json={"top_n": 10})
    assert submit_response.status_code == 200
    assert submit_response.json()["stored_count"] == 2

    list_response = client.get("/api/v1/execution/live-submit-attempts", params={"top_n": 10})
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["count"] == 2
    assert payload["items"][0]["created_at_ms"] >= payload["items"][1]["created_at_ms"]

    symbol_filtered = client.get("/api/v1/execution/live-submit-attempts", params={"symbols": "ETH", "top_n": 10}).json()
    assert symbol_filtered["count"] == 1
    assert symbol_filtered["items"][0]["symbol"] == "ETH"

    route_filtered = client.get(
        "/api/v1/execution/live-submit-attempts",
        params=[("route_keys", "BTC:binance->okx"), ("top_n", "10")],
    ).json()
    assert route_filtered["count"] == 1
    assert route_filtered["items"][0]["route_key"] == "BTC:binance->okx"


def test_live_submit_endpoints_have_no_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in guarded live submit APIs")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-submit", json={"top_n": 5})

    assert response.status_code == 200
    assert response.json()["attempt_count"] == 1


def test_live_submit_armed_path_can_reach_binance_pilot_adapter_with_mocked_transport(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="binance")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_total_notional_usd", 5000.0, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_symbol_notional_usd", {"BTC": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_long_exchange_notional_usd", {"binance": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_short_exchange_notional_usd", {"binance": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_fixture_configured_venues", {"binance": True}, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_allowed_venues", ["binance"], raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", False, raising=False)
    monkeypatch.setenv("ARB_BINANCE_API_KEY", "a" * 16)
    monkeypatch.setenv("ARB_BINANCE_API_SECRET", "b" * 32)

    async def fake_request(*args, **kwargs):  # type: ignore[no-untyped-def]
        return {"status": "NEW", "symbol": "BTC", "orderId": 1, "origQty": "1", "executedQty": "0"}

    monkeypatch.setattr("app.execution_adapters.binance_live.HttpxBinanceTransport.request", fake_request)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-submit", json={"top_n": 10})
    payload = response.json()

    assert response.status_code == 200
    assert payload["submitted_count"] == 1
    assert payload["items"][0]["real_adapter_path_attempted"] is True
    assert payload["items"][0]["accepted_leg_count"] == 2


def test_live_submit_mixed_venue_paths_fail_with_explicit_reason(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    monkeypatch.setattr("app.main.settings.execution_policy_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allow_test_execution", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_policy_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_total_notional_usd", 5000.0, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_symbol_notional_usd", {"BTC": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_long_exchange_notional_usd", {"binance": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_account_state_fixture_remaining_short_exchange_notional_usd", {"okx": 5000.0}, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_readiness_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.execution_credential_fixture_configured_venues", {"binance": True, "okx": True}, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.live_execution_allowed_venues", ["binance", "okx"], raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_enabled", True, raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", False, raising=False)

    client = TestClient(app)
    response = client.post("/api/v1/execution/live-submit", json={"top_n": 10})
    payload = response.json()
    assert response.status_code == 200
    assert payload["blocked_count"] == 1
    assert "mixed_live_venue_path_not_supported_yet" in payload["items"][0]["block_reasons"]


def test_live_submit_attempts_schema_exists_and_observations_schema_unchanged(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    with store._connect() as conn:
        observation_columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]
        live_submit_columns = [row[1] for row in conn.execute("PRAGMA table_info(live_submit_attempts)").fetchall()]

    assert observation_columns == EXPECTED_OBSERVATIONS_COLUMNS
    assert live_submit_columns == [
        "id",
        "created_at_ms",
        "attempt_id",
        "route_key",
        "symbol",
        "live_submit_status",
        "block_reasons_json",
        "submitted_leg_count",
        "accepted_leg_count",
        "long_leg_json",
        "short_leg_json",
        "raw_attempt_json",
    ]
