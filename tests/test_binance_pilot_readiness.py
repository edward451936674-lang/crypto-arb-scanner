from fastapi.testclient import TestClient

from app.main import app
from app.models.observation import ObservationRecord
from app.storage.observations import ObservationStore


def _record(*, symbol: str, long_exchange: str = "binance", short_exchange: str = "binance") -> ObservationRecord:
    route = f"{symbol}:{long_exchange}->{short_exchange}"
    return ObservationRecord(
        observed_at_ms=1_700_000_000_000,
        symbol=symbol,
        cluster_id=f"{symbol}|{long_exchange}|{short_exchange}",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        estimated_net_edge_bps=12.0,
        opportunity_grade="tradable",
        execution_mode="normal",
        final_position_pct=0.03,
        why_not_tradable=None,
        replay_net_after_cost_bps=14.0,
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
            "target_notional_usd": 1200.0,
            "long_price": 100.0,
            "short_price": 101.0,
            "replay_passes_min_trade_gate": True,
            "replay_confidence_label": "high",
            "risk_flags": [],
            "test": False,
        },
    )


def test_binance_pilot_readiness_preview_returns_structured_items(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC")])
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
    monkeypatch.setattr("app.main.settings.binance_execution_environment", "testnet", raising=False)
    monkeypatch.setattr("app.main.settings.binance_testnet_base_url", "https://testnet.binancefuture.com", raising=False)
    monkeypatch.setattr("app.main.settings.binance_live_base_url", "https://fapi.binance.com", raising=False)
    monkeypatch.setattr("app.main.settings.binance_pilot_allowed_symbols", ["BTC"], raising=False)
    monkeypatch.setattr("app.main.settings.guarded_live_submit_require_arm_token", False, raising=False)

    client = TestClient(app)
    response = client.post("/api/v1/execution/binance-pilot-readiness-preview", json={"top_n": 10})
    payload = response.json()

    assert response.status_code == 200
    assert isinstance(payload["checklist_items"], list)
    assert any(item["item"] == "environment_mode_resolved" for item in payload["checklist_items"])
    assert "status" in payload
    assert "block_reasons" in payload


def test_binance_pilot_readiness_mixed_venue_still_blocked(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    client = TestClient(app)
    response = client.post("/api/v1/execution/binance-pilot-readiness-preview", json={"top_n": 10})

    assert response.status_code == 200
    assert "mixed_live_venue_path_not_supported_yet" in response.json()["block_reasons"]
