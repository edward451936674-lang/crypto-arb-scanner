from fastapi.testclient import TestClient

from app import main as main_module
from app.main import app
from app.models.observation import ObservationRecord
from app.storage.observations import ObservationStore


def _seed_store(tmp_path) -> ObservationStore:
    store = ObservationStore(str(tmp_path / "research.sqlite3"))
    store.insert_many(
        [
            ObservationRecord(
                observed_at_ms=1000,
                symbol="BTC",
                cluster_id="btc-1",
                long_exchange="binance",
                short_exchange="okx",
                estimated_net_edge_bps=15.0,
                execution_mode="small_probe",
                why_not_tradable="capacity",
                replay_net_after_cost_bps=10.0,
                risk_flags=["capacity_limit", "replay_gate"],
                raw_opportunity_json={"id": 1},
            ),
            ObservationRecord(
                observed_at_ms=2000,
                symbol="BTC",
                cluster_id="btc-2",
                long_exchange="binance",
                short_exchange="okx",
                estimated_net_edge_bps=13.0,
                execution_mode="paper",
                why_not_tradable="replay gate",
                replay_net_after_cost_bps=9.0,
                risk_flags=["replay_gate"],
                raw_opportunity_json={"id": 2},
            ),
            ObservationRecord(
                observed_at_ms=2500,
                symbol="BTC",
                cluster_id="btc-3",
                long_exchange="hyperliquid",
                short_exchange="lighter",
                estimated_net_edge_bps=18.0,
                execution_mode="normal",
                why_not_tradable=None,
                replay_net_after_cost_bps=17.0,
                risk_flags=[],
                raw_opportunity_json={"id": 3},
            ),
            ObservationRecord(
                observed_at_ms=3000,
                symbol="ETH",
                cluster_id="eth-1",
                long_exchange="binance",
                short_exchange="okx",
                estimated_net_edge_bps=11.0,
                execution_mode="paper",
                why_not_tradable="liq buffer",
                replay_net_after_cost_bps=4.0,
                risk_flags=["liquidation_buffer"],
                raw_opportunity_json={"id": 4},
            ),
        ]
    )
    store.insert_alert_event(
        sent_at_ms=4000,
        dedupe_identity="route:BTC:binance->okx",
        cluster_id=None,
        route_key="BTC:binance->okx",
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        execution_mode="small_probe",
        final_position_pct=0.01,
        replay_net_after_cost_bps=10.0,
        replay_passes_min_trade_gate=True,
        message_hash="m1",
    )
    store.insert_alert_event(
        sent_at_ms=5000,
        dedupe_identity="route:BTC:binance->okx",
        cluster_id=None,
        route_key="BTC:binance->okx",
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        execution_mode="paper",
        final_position_pct=0.0,
        replay_net_after_cost_bps=8.0,
        replay_passes_min_trade_gate=False,
        message_hash="m2",
    )
    return store


def test_research_routes_and_symbol_endpoints(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main_module, "observation_store", _seed_store(tmp_path))
    client = TestClient(app)

    routes_response = client.get("/api/v1/research/routes", params={"limit": 5})
    assert routes_response.status_code == 200
    payload = routes_response.json()
    assert payload["count"] == 3
    first = payload["items"][0]
    assert first["route_key"] == "BTC:binance->okx"
    assert first["observation_count"] == 2
    assert first["alert_count"] == 2
    assert first["small_probe_count"] == 1
    assert first["paper_count"] == 1
    assert first["avg_estimated_net_edge_bps"] == 14.0
    assert first["avg_replay_net_after_cost_bps"] == 9.5
    assert first["last_execution_mode"] == "paper"
    assert first["last_seen_at_ms"] == 2000
    assert first["persistence_window_ms"] == 1000

    symbol_response = client.get("/api/v1/research/symbol", params={"symbol": "btc"})
    assert symbol_response.status_code == 200
    symbol_payload = symbol_response.json()
    assert symbol_payload["symbol"] == "BTC"
    assert symbol_payload["count"] == 2
    route_keys = {item["route_key"] for item in symbol_payload["items"]}
    assert route_keys == {"BTC:binance->okx", "BTC:hyperliquid->lighter"}


def test_research_why_not_breakdown_and_replay_calibration(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main_module, "observation_store", _seed_store(tmp_path))
    client = TestClient(app)

    breakdown_response = client.get("/api/v1/research/why-not-breakdown")
    assert breakdown_response.status_code == 200
    breakdown_payload = breakdown_response.json()
    assert breakdown_payload["count"] == 3
    items = {item["risk_flag"]: item for item in breakdown_payload["items"]}
    assert items["replay_gate"]["count"] == 2
    assert items["replay_gate"]["avg_estimated_net_edge_bps"] == 14.0
    assert items["capacity_limit"]["count"] == 1

    calibration_response = client.get("/api/v1/research/replay-calibration", params={"top_n": 2})
    assert calibration_response.status_code == 200
    calibration_payload = calibration_response.json()
    assert calibration_payload["route_count"] == 3
    assert calibration_payload["comparable_route_count"] == 3
    assert calibration_payload["avg_estimated_net_edge_bps"] == 14.333333333333334
    assert calibration_payload["avg_replay_net_after_cost_bps"] == 10.166666666666666
    assert calibration_payload["avg_overestimation_bps"] == 4.166666666666668
    assert calibration_payload["top_overestimated_routes"][0]["route_key"] == "ETH:binance->okx"


def test_research_endpoints_handle_empty_store(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main_module, "observation_store", ObservationStore(str(tmp_path / "empty.sqlite3")))
    client = TestClient(app)

    routes_response = client.get("/api/v1/research/routes")
    assert routes_response.status_code == 200
    assert routes_response.json() == {"count": 0, "sort_by": "observation_count", "items": []}

    symbol_response = client.get("/api/v1/research/symbol", params={"symbol": "BTC"})
    assert symbol_response.status_code == 200
    assert symbol_response.json() == {"symbol": "BTC", "count": 0, "sort_by": "observation_count", "items": []}

    breakdown_response = client.get("/api/v1/research/why-not-breakdown")
    assert breakdown_response.status_code == 200
    assert breakdown_response.json() == {"count": 0, "items": []}

    calibration_response = client.get("/api/v1/research/replay-calibration")
    assert calibration_response.status_code == 200
    assert calibration_response.json() == {
        "route_count": 0,
        "comparable_route_count": 0,
        "avg_estimated_net_edge_bps": None,
        "avg_replay_net_after_cost_bps": None,
        "avg_overestimation_bps": None,
        "top_overestimated_routes": [],
    }
