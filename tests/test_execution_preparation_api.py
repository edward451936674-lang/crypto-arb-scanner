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
    long_price: float | None = 100.0,
    short_price: float | None = 101.0,
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
            "long_price": long_price,
            "short_price": short_price,
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
            """
            SELECT
                symbol,
                route_key,
                execution_mode,
                is_executable_now,
                status,
                status_updated_at_ms,
                expires_at_ms,
                evaluation_due_at_ms,
                target_notional_usd,
                entry_reference_price_long,
                entry_reference_price_short,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                paper_pnl_usd,
                outcome_status,
                outcome_updated_at_ms
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row[0:5] == ("BTC", "BTC:binance->okx", "normal", 1, "planned")
    assert row[5] == payload["created_at_ms"]
    assert row[6] > payload["created_at_ms"]
    assert row[7] == row[6]
    assert row[8] is None
    assert row[9] == 100.0
    assert row[10] == 101.0
    assert row[11] == 100.0
    assert row[12] == 101.0
    assert row[13] == 0.0
    assert row[14] is None
    assert row[15] == "flat"
    assert row[16] == payload["created_at_ms"]


def test_paper_executions_from_candidates_initializes_unknown_outcome_when_entry_prices_missing(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                long_price=None,
                short_price=None,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 5, "include_test": False})
    assert response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                entry_reference_price_long,
                entry_reference_price_short,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                paper_pnl_usd,
                outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row == (None, None, None, None, None, None, "unknown")


def test_paper_executions_from_candidates_initializes_unknown_outcome_when_entry_price_zero(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                long_price=0.0,
                short_price=101.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 5, "include_test": False})
    assert response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                entry_reference_price_long,
                entry_reference_price_short,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                paper_pnl_usd,
                outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row == (0.0, 101.0, 0.0, 101.0, None, None, "unknown")


def test_paper_executions_from_candidates_initializes_unknown_outcome_when_entry_price_negative(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                long_price=-10.0,
                short_price=101.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)

    response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 5, "include_test": False})
    assert response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                entry_reference_price_long,
                entry_reference_price_short,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                paper_pnl_usd,
                outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row == (-10.0, 101.0, -10.0, 101.0, None, None, "unknown")


def test_get_paper_executions_returns_stored_rows_with_filters(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", is_test=False),
            _record(symbol="SOL", long_exchange="bybit", short_exchange="okx", is_test=True),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": True})

    all_response = client.get("/api/v1/paper-executions", params={"top_n": 10, "include_test": True})
    filtered_response = client.get(
        "/api/v1/paper-executions",
        params={"top_n": 10, "include_test": False, "symbols": "BTC", "status": "planned"},
    )

    assert all_response.status_code == 200
    assert filtered_response.status_code == 200
    assert {item["symbol"] for item in all_response.json()["items"]} == {"BTC", "SOL"}
    filtered_items = filtered_response.json()["items"]
    assert len(filtered_items) == 1
    assert filtered_items[0]["symbol"] == "BTC"
    assert filtered_items[0]["status"] == "planned"
    assert "paper_pnl_bps" in filtered_items[0]


def test_get_paper_executions_supports_outcome_status_filter(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", long_price=100.0, short_price=101.0),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", long_price=100.0, short_price=101.0),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    store.insert_many([_record(symbol="BTC", long_exchange="binance", short_exchange="okx", long_price=102.0, short_price=100.0)])
    mark_response = client.post("/api/v1/paper-executions/mark-to-market", params={"top_n": 10})
    assert mark_response.status_code == 200

    positive_response = client.get("/api/v1/paper-executions", params={"top_n": 10, "outcome_status": "positive"})
    assert positive_response.status_code == 200
    positive_items = positive_response.json()["items"]
    assert len(positive_items) == 1
    assert positive_items[0]["symbol"] == "BTC"


def test_paper_execution_evaluation_marks_expired_invalidated_and_still_valid(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx"),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx"),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx"),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE paper_executions SET expires_at_ms = created_at_ms - 1 WHERE symbol = 'BTC'"
        )
        conn.execute(
            "DELETE FROM observations WHERE symbol = 'ETH'"
        )
    eval_response = client.post("/api/v1/paper-executions/evaluate", params={"top_n": 10})
    assert eval_response.status_code == 200
    assert eval_response.json()["evaluated_count"] == 3

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT symbol, status, closure_reason, closed_at_ms, latest_observed_edge_bps
            FROM paper_executions
            ORDER BY symbol ASC
            """
        ).fetchall()
    assert rows[0][0:3] == ("BTC", "expired", "expired")
    assert rows[1][0:3] == ("ETH", "invalidated", "route_missing")
    assert rows[2][0:3] == ("SOL", "still_valid", "still_valid")
    assert rows[0][3] is not None and rows[1][3] is not None and rows[2][3] is not None
    assert rows[2][4] is not None


def test_observations_schema_is_unchanged(tmp_path) -> None:
    db_path = tmp_path / "observations.sqlite3"
    ObservationStore(str(db_path))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA table_info(observations)").fetchall()
    columns = [row[1] for row in rows]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS


def test_mark_to_market_computes_positive_negative_and_unknown(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", long_price=100.0, short_price=100.0),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", long_price=100.0, short_price=100.0),
            _record(symbol="SOL", long_exchange="binance", short_exchange="okx", long_price=None, short_price=None),
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    store.insert_many(
        [
            _record(symbol="BTC", long_exchange="binance", short_exchange="okx", long_price=101.0, short_price=99.0),
            _record(symbol="ETH", long_exchange="binance", short_exchange="okx", long_price=99.0, short_price=101.0),
        ]
    )

    response = client.post("/api/v1/paper-executions/mark-to-market", params={"top_n": 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_count"] == 3

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT symbol, paper_pnl_bps, outcome_status, latest_reference_price_long, latest_reference_price_short
            FROM paper_executions
            ORDER BY symbol ASC
            """
        ).fetchall()
    by_symbol = {row[0]: row[1:] for row in rows}
    assert round(float(by_symbol["BTC"][0]), 6) == 200.0
    assert by_symbol["BTC"][1] == "positive"
    assert round(float(by_symbol["ETH"][0]), 6) == -200.0
    assert by_symbol["ETH"][1] == "negative"
    assert by_symbol["SOL"][0] is None
    assert by_symbol["SOL"][1] == "unknown"
    assert by_symbol["SOL"][2] is None
    assert by_symbol["SOL"][3] is None


def test_mark_to_market_does_not_fallback_to_symbol_only_route_match(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    original_route = "BTC:binance->okx"
    replacement_route = "BTC:bybit->okx"
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                route_key=original_route,
                long_price=100.0,
                short_price=100.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM observations")
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="bybit",
                short_exchange="okx",
                route_key=replacement_route,
                long_price=120.0,
                short_price=80.0,
            )
        ]
    )

    response = client.post("/api/v1/paper-executions/mark-to-market", params={"top_n": 10})
    assert response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                route_key,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row[0] == original_route
    assert row[1] is None
    assert row[2] is None
    assert row[3] is None
    assert row[4] == "unknown"


def test_mark_to_market_keeps_unknown_when_route_key_is_missing_even_if_symbol_exists(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    original_route = "BTC:binance->okx"
    replacement_route = "BTC:bybit->okx"
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                route_key=original_route,
                long_price=100.0,
                short_price=100.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE paper_executions SET route_key = ''")
        conn.execute("DELETE FROM observations")
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="bybit",
                short_exchange="okx",
                route_key=replacement_route,
                long_price=120.0,
                short_price=80.0,
            )
        ]
    )

    response = client.post("/api/v1/paper-executions/mark-to-market", params={"top_n": 10})
    assert response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                route_key,
                latest_reference_price_long,
                latest_reference_price_short,
                paper_pnl_bps,
                paper_pnl_usd,
                outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row[0] == ""
    assert row[1] is None
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None
    assert row[5] == "unknown"


def test_mark_to_market_matches_route_key_after_whitespace_normalization(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "observations.sqlite3"
    store = ObservationStore(str(db_path))
    route_key = "BTC:binance->okx"
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                route_key=route_key,
                long_price=100.0,
                short_price=100.0,
            )
        ]
    )
    monkeypatch.setattr("app.main.observation_store", store)
    client = TestClient(app)
    create_response = client.post("/api/v1/paper-executions/from-candidates", params={"top_n": 10, "include_test": False})
    assert create_response.status_code == 200

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM observations")
    store.insert_many(
        [
            _record(
                symbol="BTC",
                long_exchange="binance",
                short_exchange="okx",
                route_key=f"  {route_key}  ",
                long_price=101.0,
                short_price=99.0,
            )
        ]
    )

    response = client.post("/api/v1/paper-executions/mark-to-market", params={"top_n": 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload["outcome_counts"].get("positive") == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT latest_reference_price_long, latest_reference_price_short, paper_pnl_bps, outcome_status
            FROM paper_executions
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row[0] == 101.0
    assert row[1] == 99.0
    assert round(float(row[2]), 6) == 200.0
    assert row[3] == "positive"


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
    list_response = client.get("/api/v1/paper-executions")
    eval_response = client.post("/api/v1/paper-executions/evaluate")
    mark_response = client.post("/api/v1/paper-executions/mark-to-market")

    assert get_response.status_code == 200
    assert post_response.status_code == 200
    assert list_response.status_code == 200
    assert eval_response.status_code == 200
    assert mark_response.status_code == 200


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
