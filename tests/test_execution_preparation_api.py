import sqlite3

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
    execution_mode: str = "normal",
    final_position_pct: float | None = 0.03,
    replay_passes_min_trade_gate: bool | None = True,
    why_not_tradable: str | None = None,
    risk_flags: list[str] | None = None,
    risk_adjusted_edge_bps: float = 22.0,
    replay_net_after_cost_bps: float = 16.0,
    estimated_net_edge_bps: float = 14.0,
    route_key: str | None = None,
    is_test: bool = False,
) -> ObservationRecord:
    route = route_key or f"{symbol}:{long_exchange}->{short_exchange}"
    return ObservationRecord(
        observed_at_ms=1_700_000_000_000,
        symbol=symbol,
        cluster_id=f"{symbol}|{long_exchange}|{short_exchange}",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        estimated_net_edge_bps=estimated_net_edge_bps,
        opportunity_grade="tradable",
        execution_mode=execution_mode,
        final_position_pct=final_position_pct,
        why_not_tradable=why_not_tradable,
        replay_net_after_cost_bps=replay_net_after_cost_bps,
        replay_confidence_label="high",
        replay_passes_min_trade_gate=replay_passes_min_trade_gate,
        risk_flags=risk_flags or [],
        raw_opportunity_json={
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "route_key": route,
            "opportunity_type": "tradable",
            "execution_mode": execution_mode,
            "final_position_pct": final_position_pct,
            "risk_adjusted_edge_bps": risk_adjusted_edge_bps,
            "replay_net_after_cost_bps": replay_net_after_cost_bps,
            "estimated_net_edge_bps": estimated_net_edge_bps,
            "replay_passes_min_trade_gate": replay_passes_min_trade_gate,
            "replay_confidence_label": "high",
            "risk_flags": risk_flags or [],
            "test": is_test,
        },
    )


def test_execution_candidates_endpoint_returns_preparation_fields(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(
                symbol="ETH",
                long_exchange="binance",
                short_exchange="okx",
                execution_mode="paper",
                final_position_pct=0.0,
                replay_passes_min_trade_gate=False,
                risk_flags=["funding_stale"],
            ),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.get("/api/v1/execution/candidates", params={"top_n": 5, "include_test": True})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    first = payload[0]
    assert first["symbol"] == "BTC"
    assert first["route_key"] == "BTC:binance->okx"
    assert first["opportunity_type"] == "tradable"
    assert first["execution_mode"] == "normal"
    assert first["expected_edge_bps"] == 14.0
    assert first["replay_net_after_cost_bps"] == 16.0
    assert first["risk_adjusted_edge_bps"] == 22.0
    assert first["target_position_pct"] == 0.03
    assert first["is_executable_now"] is True
    assert first["why_not_executable"] is None
    assert first["replay_confidence_label"] == "high"
    assert first["replay_passes_min_trade_gate"] is True
    assert first["risk_flags"] == []
    assert isinstance(first["generated_at_ms"], int)

    second = payload[1]
    assert second["symbol"] == "ETH"
    assert second["is_executable_now"] is False
    assert "execution_mode_paper" in second["why_not_executable"]
    assert "replay_min_trade_gate_not_passed" in second["why_not_executable"]
    assert "risk_flags_present" in second["why_not_executable"]


def test_execution_candidates_include_test_filter(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", is_test=False),
            _record(symbol="SOL", long_exchange="bybit", short_exchange="okx", is_test=True),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    excluded = client.get("/api/v1/execution/candidates", params={"include_test": False, "top_n": 10})
    included = client.get("/api/v1/execution/candidates", params={"include_test": True, "top_n": 10})

    assert excluded.status_code == 200
    assert included.status_code == 200
    assert [item["symbol"] for item in excluded.json()] == ["BTC"]
    assert {item["symbol"] for item in included.json()} == {"BTC", "SOL"}


def test_paper_executions_from_candidates_stores_new_table_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 5, "include_test": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["stored_count"] == 1
    assert payload["stored_symbols"] == ["BTC"]

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT symbol, route_key, execution_mode, is_executable_now FROM paper_executions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row == ("BTC", "BTC:binance->okx", "normal", 1)


def test_observations_schema_is_unchanged(tmp_path) -> None:
    db_path = tmp_path / "observations.sqlite3"
    ObservationStore(str(db_path))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA table_info(observations)").fetchall()
    columns = [row[1] for row in rows]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS


def test_execution_candidate_endpoints_do_not_require_network_calls(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx")])
    monkeypatch.setattr("app.main.observation_store", store)

    async def fail_fetch_snapshots(*args, **kwargs):
        raise AssertionError("network call should not happen in execution candidate APIs")

    monkeypatch.setattr("app.services.market_data.MarketDataService.fetch_snapshots", fail_fetch_snapshots)
    client = TestClient(app)

    get_response = client.get("/api/v1/execution/candidates")
    post_response = client.post("/api/v1/paper-executions/from-candidates")

    assert get_response.status_code == 200
    assert post_response.status_code == 200


def test_include_test_false_does_not_let_hidden_tests_consume_top_n_for_get_and_post(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(symbol="TST1", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=100.0, is_test=True),
            _record(symbol="TST2", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=99.0, is_test=True),
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=50.0, is_test=False),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=40.0, is_test=False),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", risk_adjusted_edge_bps=30.0, is_test=False),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    candidates_response = client.get(
        "/api/v1/execution/candidates",
        params={"include_test": False, "top_n": 2},
    )

    assert candidates_response.status_code == 200
    candidates_payload = candidates_response.json()
    assert [item["symbol"] for item in candidates_payload] == ["BTC", "ETH"]

    paper_response = client.post(
        "/api/v1/paper-executions/from-candidates",
        params={"include_test": False, "top_n": 2},
    )

    assert paper_response.status_code == 200
    paper_payload = paper_response.json()
    assert paper_payload["stored_count"] == 2
    assert set(paper_payload["stored_symbols"]) == {"BTC", "ETH"}
