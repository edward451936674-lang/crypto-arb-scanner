from fastapi.testclient import TestClient

from app.main import app
from app.models.observation import ObservationRecord
from app.storage.observations import ObservationStore


def _record(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    risk_adjusted_edge_bps: float,
    replay_net_after_cost_bps: float,
    estimated_net_edge_bps: float,
    execution_mode: str = "normal",
    funding_confidence_label: str = "high",
    conviction_label: str = "high",
    route_key: str | None = None,
    opportunity_type: str = "tradable",
    observed_at_ms: int = 1_700_000_000_000,
    is_test: bool = True,
) -> ObservationRecord:
    route = route_key or f"{symbol}:{long_exchange}->{short_exchange}"
    return ObservationRecord(
        observed_at_ms=observed_at_ms,
        symbol=symbol,
        cluster_id=f"{symbol}|{long_exchange}|{short_exchange}",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        estimated_net_edge_bps=estimated_net_edge_bps,
        opportunity_grade=opportunity_type,
        execution_mode=execution_mode,
        final_position_pct=0.01,
        replay_net_after_cost_bps=replay_net_after_cost_bps,
        raw_opportunity_json={
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "price_spread_bps": 5.0,
            "funding_spread_bps": 2.0,
            "risk_adjusted_edge_bps": risk_adjusted_edge_bps,
            "net_edge_bps": estimated_net_edge_bps,
            "route_key": route,
            "execution_mode": execution_mode,
            "funding_confidence_label": funding_confidence_label,
            "conviction_label": conviction_label,
            "opportunity_type": opportunity_type,
            "test": is_test,
        },
    )


def test_opportunities_endpoint_returns_200_and_structure(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=20.0,
                replay_net_after_cost_bps=15.0,
                estimated_net_edge_bps=14.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0] == {
        "symbol": "BTC",
        "long_exchange": "binance",
        "short_exchange": "okx",
        "price_spread_bps": 5.0,
        "funding_spread_bps": 2.0,
        "risk_adjusted_edge_bps": 20.0,
        "replay_net_after_cost_bps": 15.0,
        "estimated_net_edge_bps": 14.0,
        "route_key": "BTC:binance->okx",
        "rank": 1,
        "opportunity_type": "tradable",
        "is_test": True,
    }


def test_opportunities_endpoint_applies_top_n(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=30, replay_net_after_cost_bps=10, estimated_net_edge_bps=10),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=20, replay_net_after_cost_bps=10, estimated_net_edge_bps=10),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=10, replay_net_after_cost_bps=10, estimated_net_edge_bps=10),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"top_n": 2})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert [item["symbol"] for item in payload] == ["BTC", "ETH"]


def test_opportunities_endpoint_only_actionable_filter(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=30, replay_net_after_cost_bps=12, estimated_net_edge_bps=11, execution_mode="paper"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=28, replay_net_after_cost_bps=12, estimated_net_edge_bps=11, funding_confidence_label="low"),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=26, replay_net_after_cost_bps=12, estimated_net_edge_bps=11, conviction_label="low"),
            _record(symbol="XRP", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=24, replay_net_after_cost_bps=12, estimated_net_edge_bps=11),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"only_actionable": True})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["symbol"] == "XRP"


def test_opportunities_endpoint_sorts_and_dedupes_by_route(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=10,
                replay_net_after_cost_bps=10,
                estimated_net_edge_bps=9,
                route_key="BTC:binance->okx",
            ),
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=12,
                replay_net_after_cost_bps=8,
                estimated_net_edge_bps=8,
                route_key="BTC:binance->okx",
            ),
            _record(
                symbol="ETH",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=12,
                replay_net_after_cost_bps=9,
                estimated_net_edge_bps=7,
                route_key="ETH:binance->okx",
            ),
            _record(
                symbol="SOL",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=12,
                replay_net_after_cost_bps=9,
                estimated_net_edge_bps=8,
                route_key="SOL:binance->okx",
            ),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"dedupe_by_route": True})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 3
    assert [item["symbol"] for item in payload] == ["SOL", "ETH", "BTC"]
    assert payload[2]["risk_adjusted_edge_bps"] == 12


def test_opportunities_endpoint_sorting_uses_replay_then_estimated_tiebreakers(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=20, replay_net_after_cost_bps=8, estimated_net_edge_bps=11),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=20, replay_net_after_cost_bps=9, estimated_net_edge_bps=10),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=20, replay_net_after_cost_bps=9, estimated_net_edge_bps=12),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities")

    assert response.status_code == 200
    payload = response.json()
    assert [item["symbol"] for item in payload] == ["SOL", "ETH", "BTC"]


def test_opportunities_endpoint_dedupe_prefers_highest_risk_adjusted_edge(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=15,
                replay_net_after_cost_bps=50,
                estimated_net_edge_bps=50,
                route_key="BTC:binance->okx",
            ),
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=18,
                replay_net_after_cost_bps=1,
                estimated_net_edge_bps=1,
                route_key="BTC:binance->okx",
            ),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"dedupe_by_route": True})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["risk_adjusted_edge_bps"] == 18


def test_opportunities_endpoint_combines_filters_sorting_top_n_and_is_deterministic(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(  # excluded by only_actionable due to paper mode
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=60,
                replay_net_after_cost_bps=30,
                estimated_net_edge_bps=25,
                execution_mode="paper",
            ),
            _record(  # duplicate route, lower risk_adjusted_edge_bps should be removed by dedupe
                symbol="ETH",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=40,
                replay_net_after_cost_bps=22,
                estimated_net_edge_bps=21,
                route_key="ETH:binance->okx",
            ),
            _record(
                symbol="ETH",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=45,
                replay_net_after_cost_bps=18,
                estimated_net_edge_bps=17,
                route_key="ETH:binance->okx",
            ),
            _record(
                symbol="SOL",
                long_exchange="okx",
                short_exchange="binance",
                risk_adjusted_edge_bps=45,
                replay_net_after_cost_bps=20,
                estimated_net_edge_bps=18,
                route_key="SOL:okx->binance",
            ),
            _record(  # excluded by only_actionable due to low funding confidence
                symbol="XRP",
                long_exchange="okx",
                short_exchange="binance",
                risk_adjusted_edge_bps=50,
                replay_net_after_cost_bps=19,
                estimated_net_edge_bps=16,
                funding_confidence_label="low",
            ),
            _record(
                symbol="ADA",
                long_exchange="bybit",
                short_exchange="okx",
                risk_adjusted_edge_bps=35,
                replay_net_after_cost_bps=15,
                estimated_net_edge_bps=14,
                route_key="ADA:bybit->okx",
            ),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    params = {"only_actionable": True, "dedupe_by_route": True, "top_n": 2}
    first_response = client.get("/api/v1/opportunities", params=params)
    second_response = client.get("/api/v1/opportunities", params=params)

    assert first_response.status_code == 200
    assert second_response.status_code == 200

    first_payload = first_response.json()
    second_payload = second_response.json()
    assert first_payload == second_payload
    assert len(first_payload) == 2
    assert [item["symbol"] for item in first_payload] == ["SOL", "ETH"]
    assert [item["rank"] for item in first_payload] == [1, 2]


def test_opportunities_endpoint_dashboard_scenario_with_test_snapshots(tmp_path, monkeypatch, capsys) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(  # dynamic actionable test snapshot
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=15,
                replay_net_after_cost_bps=14,
                estimated_net_edge_bps=13,
                route_key="BTC:binance->okx",
                is_test=True,
            ),
            _record(  # dynamic actionable test snapshot
                symbol="ETH",
                long_exchange="okx",
                short_exchange="binance",
                risk_adjusted_edge_bps=12,
                replay_net_after_cost_bps=11,
                estimated_net_edge_bps=10,
                route_key="ETH:okx->binance",
                is_test=True,
            ),
            _record(  # excluded by only_actionable
                symbol="SOL",
                long_exchange="okx",
                short_exchange="bybit",
                risk_adjusted_edge_bps=30,
                replay_net_after_cost_bps=22,
                estimated_net_edge_bps=20,
                execution_mode="small_probe",
                route_key="SOL:okx->bybit",
                is_test=True,
            ),
            _record(  # excluded by symbols filter
                symbol="XRP",
                long_exchange="binance",
                short_exchange="okx",
                risk_adjusted_edge_bps=50,
                replay_net_after_cost_bps=40,
                estimated_net_edge_bps=35,
                route_key="XRP:binance->okx",
                is_test=True,
            ),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get(
        "/api/v1/opportunities",
        params={
            "symbols": "BTC,ETH,SOL",
            "top_n": 5,
            "only_actionable": True,
            "dedupe_by_route": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["symbol"] for item in payload] == ["BTC", "ETH"]
    assert [item["rank"] for item in payload] == [1, 2]
    assert all(item["is_test"] is True for item in payload)
    assert payload[0]["risk_adjusted_edge_bps"] == 15
    assert payload[1]["risk_adjusted_edge_bps"] == 12

    print(payload[0])
    captured = capsys.readouterr()
    assert "BTC" in captured.out


def test_opportunities_endpoint_parses_test_string_flags(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    btc = _record(
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        risk_adjusted_edge_bps=15,
        replay_net_after_cost_bps=14,
        estimated_net_edge_bps=13,
        is_test=True,
    )
    eth = _record(
        symbol="ETH",
        long_exchange="okx",
        short_exchange="binance",
        risk_adjusted_edge_bps=12,
        replay_net_after_cost_bps=11,
        estimated_net_edge_bps=10,
        is_test=True,
    )
    btc.raw_opportunity_json["test"] = "false"
    eth.raw_opportunity_json["test"] = "true"
    store.insert_many([btc, eth])
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"dedupe_by_route": True})

    assert response.status_code == 200
    payload = response.json()
    by_symbol = {item["symbol"]: item["is_test"] for item in payload}
    assert by_symbol["BTC"] is False
    assert by_symbol["ETH"] is True


def test_opportunities_endpoint_falls_back_to_raw_payload_for_test_snapshots_and_primary_sort(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    btc = ObservationRecord(
        observed_at_ms=1_700_000_000_000,
        symbol="BTC",
        cluster_id="BTC|binance|okx",
        long_exchange="binance",
        short_exchange="okx",
        estimated_net_edge_bps=15.0,
        opportunity_grade=None,
        execution_mode="normal",
        final_position_pct=0.01,
        replay_net_after_cost_bps=10.0,
        raw_opportunity_json={
            "symbol": "BTC",
            "long_exchange": "binance",
            "short_exchange": "okx",
            "route_key": "BTC:binance->okx",
            "test": True,
            "price_spread_bps": 7.0,
            "funding_spread_bps": 3.0,
            "risk_adjusted_edge_bps": 5.0,
            "net_edge_bps": 15.0,
            "opportunity_type": "watchlist",
        },
    )
    eth = ObservationRecord(
        observed_at_ms=1_700_000_000_001,
        symbol="ETH",
        cluster_id="ETH|binance|okx",
        long_exchange="binance",
        short_exchange="okx",
        estimated_net_edge_bps=12.0,
        opportunity_grade=None,
        execution_mode="normal",
        final_position_pct=0.01,
        replay_net_after_cost_bps=8.0,
        raw_opportunity_json={
            "symbol": "ETH",
            "long_exchange": "binance",
            "short_exchange": "okx",
            "route_key": "ETH:binance->okx",
            "test": True,
            "price_spread_bps": 8.0,
            "funding_spread_bps": 4.0,
            "risk_adjusted_edge_bps": 12.0,
            "net_edge_bps": 12.0,
            "opportunity_type": "tradable",
        },
    )
    store.insert_many([btc, eth])
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/opportunities", params={"dedupe_by_route": True})

    assert response.status_code == 200
    payload = response.json()
    assert [item["symbol"] for item in payload] == ["ETH", "BTC"]
    assert all(item["is_test"] is True for item in payload)
    assert payload[0]["risk_adjusted_edge_bps"] == 12.0
    assert payload[1]["risk_adjusted_edge_bps"] == 5.0
    assert payload[0]["replay_net_after_cost_bps"] == 8.0
    assert payload[1]["replay_net_after_cost_bps"] == 10.0
    assert payload[0]["price_spread_bps"] == 8.0
    assert payload[1]["price_spread_bps"] == 7.0
    assert payload[0]["funding_spread_bps"] == 4.0
    assert payload[1]["funding_spread_bps"] == 3.0
    assert payload[0]["opportunity_type"] == "tradable"
    assert payload[1]["opportunity_type"] == "watchlist"


def test_opportunities_endpoint_handles_invalid_raw_json_payloads_safely(tmp_path, monkeypatch) -> None:
    invalid_raw_record = ObservationRecord.model_construct(
        observed_at_ms=1_700_000_000_000,
        symbol="BTC",
        cluster_id="BTC|binance|okx",
        long_exchange="binance",
        short_exchange="okx",
        estimated_net_edge_bps=9.0,
        opportunity_grade="fallback-grade",
        execution_mode="normal",
        final_position_pct=0.01,
        replay_net_after_cost_bps=8.0,
        raw_opportunity_json="{not-valid-json",
    )
    monkeypatch.setattr("app.main.observation_store.latest", lambda limit=5000: [invalid_raw_record])
    client = TestClient(app)

    response = client.get("/api/v1/opportunities")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["symbol"] == "BTC"
    assert payload[0]["is_test"] is False
    assert payload[0]["price_spread_bps"] == 0.0
    assert payload[0]["funding_spread_bps"] == 0.0
    assert payload[0]["risk_adjusted_edge_bps"] == 0.0
    assert payload[0]["opportunity_type"] == "fallback-grade"
