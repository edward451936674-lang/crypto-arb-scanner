import asyncio
import sqlite3
import time

from app.main import _ScanContext, get_latest_observations, get_observation_history, run_observation_collection
from app.models.market import MarketSnapshot
from app.models.observation import ObservationRecord
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.opportunity_observer import OpportunityObservationContext, OpportunityObserverService
from app.storage.observations import ObservationStore


def _snapshot(exchange: str, price: float, funding_rate: float) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=exchange,
        venue_type="cex",
        base_symbol="BTC",
        normalized_symbol="BTC-USDT-PERP",
        instrument_id=f"{exchange}-BTC",
        mark_price=price,
        funding_rate=funding_rate,
        funding_rate_source="current",
        funding_period_hours=8,
        timestamp_ms=int(time.time() * 1000),
    )


def _build_opportunity():
    scanner = ArbitrageScannerService()
    return scanner.build_opportunities([_snapshot("binance", 100.0, -0.0002), _snapshot("okx", 100.23, 0.0002)])[0]


def test_observation_store_initializes_table(tmp_path) -> None:
    db_path = tmp_path / "observations.sqlite3"
    ObservationStore(str(db_path))

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='observations'").fetchone()
    assert row is not None


def test_insert_and_latest_history_queries(tmp_path) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    store.insert_many(
        [
            ObservationRecord(
                observed_at_ms=1000,
                symbol="BTC",
                cluster_id="btc-route",
                long_exchange="binance",
                short_exchange="okx",
                estimated_net_edge_bps=12.0,
                opportunity_grade="tradable",
                execution_mode="normal",
                final_position_pct=0.03,
                replay_passes_min_trade_gate=True,
                risk_flags=["flag_a"],
                raw_opportunity_json={"a": 1},
            ),
            ObservationRecord(
                observed_at_ms=2000,
                symbol="ETH",
                cluster_id="eth-route",
                long_exchange="binance",
                short_exchange="okx",
                estimated_net_edge_bps=8.0,
                opportunity_grade="watchlist",
                execution_mode="small_probe",
                final_position_pct=0.01,
                replay_passes_min_trade_gate=False,
                risk_flags=[],
                raw_opportunity_json={"b": 2},
            ),
        ]
    )

    latest = store.latest(limit=1)
    assert len(latest) == 1
    assert latest[0].symbol == "ETH"

    btc_history = store.history(symbol="btc", limit=10)
    assert len(btc_history) == 1
    assert btc_history[0].cluster_id == "btc-route"


def test_select_top_opportunities_filters_low_signal() -> None:
    observer = OpportunityObserverService()
    base = _build_opportunity()

    weak_paper = OpportunityObservationContext(
        opportunity=base.model_copy(update={"execution_mode": "paper", "final_position_pct": 0.0, "net_edge_bps": 2.0}),
        why_not_tradable="",
        replay_net_after_cost_bps=1.0,
        replay_confidence_label="low",
        replay_passes_min_trade_gate=False,
        replay_summary="",
    )
    strong_probe = OpportunityObservationContext(
        opportunity=base.model_copy(update={"execution_mode": "small_probe", "final_position_pct": 0.01, "net_edge_bps": 9.0}),
        why_not_tradable="",
        replay_net_after_cost_bps=7.0,
        replay_confidence_label="medium",
        replay_passes_min_trade_gate=True,
        replay_summary="",
    )

    selected = observer.select_top_opportunities([weak_paper, strong_probe])
    assert len(selected) == 1
    assert selected[0].opportunity.execution_mode == "small_probe"


def test_select_top_opportunities_keeps_multiple_routes_per_symbol() -> None:
    observer = OpportunityObserverService()
    base = _build_opportunity()

    probe_route = OpportunityObservationContext(
        opportunity=base.model_copy(
            update={
                "long_exchange": "hyperliquid",
                "short_exchange": "lighter",
                "execution_mode": "small_probe",
                "final_position_pct": 0.01,
                "net_edge_bps": 11.0,
                "opportunity_grade": "tradable",
                "risk_flags": [],
            }
        ),
        why_not_tradable="small probe only",
        replay_net_after_cost_bps=7.5,
        replay_confidence_label="medium",
        replay_passes_min_trade_gate=True,
        replay_summary="",
    )
    mixed_semantics_watchlist = OpportunityObservationContext(
        opportunity=base.model_copy(
            update={
                "long_exchange": "okx",
                "short_exchange": "lighter",
                "execution_mode": "paper",
                "final_position_pct": 0.0,
                "net_edge_bps": 10.0,
                "opportunity_grade": "watchlist",
                "risk_flags": ["mixed_funding_sources"],
            }
        ),
        why_not_tradable="mixed funding semantics",
        replay_net_after_cost_bps=5.8,
        replay_confidence_label="low",
        replay_passes_min_trade_gate=False,
        replay_summary="",
    )
    period_mismatch_watchlist = OpportunityObservationContext(
        opportunity=base.model_copy(
            update={
                "long_exchange": "binance",
                "short_exchange": "lighter",
                "execution_mode": "paper",
                "final_position_pct": 0.0,
                "net_edge_bps": 9.2,
                "opportunity_grade": "watchlist",
                "risk_flags": ["different_funding_periods"],
            }
        ),
        why_not_tradable="funding period mismatch",
        replay_net_after_cost_bps=4.4,
        replay_confidence_label="low",
        replay_passes_min_trade_gate=False,
        replay_summary="",
    )

    selected = observer.select_top_opportunities([probe_route, mixed_semantics_watchlist, period_mismatch_watchlist])
    assert len(selected) == 3
    routes = {(item.opportunity.long_exchange, item.opportunity.short_exchange) for item in selected}
    assert ("hyperliquid", "lighter") in routes
    assert ("okx", "lighter") in routes
    assert ("binance", "lighter") in routes


def test_select_top_opportunities_enforces_caps_and_filters_noise() -> None:
    observer = OpportunityObserverService()
    base = _build_opportunity()
    contexts: list[OpportunityObservationContext] = []

    for index in range(5):
        contexts.append(
            OpportunityObservationContext(
                opportunity=base.model_copy(
                    update={
                        "symbol": "BTC",
                        "long_exchange": f"long{index}",
                        "short_exchange": "lighter",
                        "execution_mode": "paper",
                        "final_position_pct": 0.0,
                        "net_edge_bps": 9.0 - index * 0.5,
                        "opportunity_grade": "watchlist",
                        "risk_flags": ["mixed_funding_sources"] if index < 4 else [],
                    }
                ),
                why_not_tradable="",
                replay_net_after_cost_bps=4.5 - index * 0.3,
                replay_confidence_label="low",
                replay_passes_min_trade_gate=False,
                replay_summary="",
            )
        )

    # Explicit low-value paper noise should be excluded.
    contexts.append(
        OpportunityObservationContext(
            opportunity=base.model_copy(
                update={
                    "symbol": "ETH",
                    "execution_mode": "paper",
                    "final_position_pct": 0.0,
                    "net_edge_bps": 1.5,
                    "opportunity_grade": "watchlist",
                    "risk_flags": [],
                }
            ),
            why_not_tradable="",
            replay_net_after_cost_bps=0.5,
            replay_confidence_label="low",
            replay_passes_min_trade_gate=False,
            replay_summary="",
        )
    )

    for index in range(10):
        contexts.append(
            OpportunityObservationContext(
                opportunity=base.model_copy(
                    update={
                        "symbol": f"ALT{index}",
                        "long_exchange": "binance",
                        "short_exchange": "okx",
                        "execution_mode": "small_probe",
                        "final_position_pct": 0.01,
                        "net_edge_bps": 8.0 + (index / 10),
                        "opportunity_grade": "watchlist",
                        "risk_flags": [],
                    }
                ),
                why_not_tradable="",
                replay_net_after_cost_bps=6.0 + (index / 10),
                replay_confidence_label="medium",
                replay_passes_min_trade_gate=True,
                replay_summary="",
            )
        )

    selected = observer.select_top_opportunities(contexts, max_global=20, max_per_symbol=3)
    assert len(selected) == 13
    assert sum(1 for item in selected if item.opportunity.symbol == "BTC") == 3
    assert all(not (item.opportunity.symbol == "ETH" and item.opportunity.net_edge_bps == 1.5) for item in selected)


def test_observe_routes_return_expected_fields(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    monkeypatch.setattr("app.main.observation_store", store)

    opportunity = _build_opportunity()
    snapshots = [_snapshot("binance", 100.0, -0.0002), _snapshot("okx", 100.23, 0.0002)]

    async def fake_build_scan_context(requested_symbols: list[str]) -> _ScanContext:
        return _ScanContext(
            requested_symbols=requested_symbols,
            opportunities=[opportunity],
            snapshot_errors=[],
            accepted_snapshots=snapshots,
        )

    monkeypatch.setattr("app.main._build_scan_context", fake_build_scan_context)

    run_response = asyncio.run(run_observation_collection(symbols="BTC"))
    assert run_response["evaluated_count"] == 1
    assert run_response["stored_count"] == 1

    latest = asyncio.run(get_latest_observations(limit=20))
    assert latest["count"] == 1
    assert "symbol" in latest["items"][0]
    assert "raw_opportunity_json" in latest["items"][0]

    history = asyncio.run(get_observation_history(symbol="BTC", limit=100))
    assert history["count"] == 1
    assert history["symbol"] == "BTC"


def test_run_observation_collection_persists_richer_raw_opportunity_json_for_real_rows(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    monkeypatch.setattr("app.main.observation_store", store)

    opportunity = _build_opportunity().model_copy(
        update={
            "route_key": None,
            "opportunity_type": None,
            "opportunity_grade": "tradable",
            "execution_mode": "normal",
            "final_position_pct": 0.025,
            "risk_flags": ["mixed_funding_sources"],
            "risk_adjusted_edge_bps": 10.8,
        }
    )
    snapshots = [_snapshot("binance", 100.0, -0.0002), _snapshot("okx", 100.23, 0.0002)]

    async def fake_build_scan_context(requested_symbols: list[str]) -> _ScanContext:
        return _ScanContext(
            requested_symbols=requested_symbols,
            opportunities=[opportunity],
            snapshot_errors=[],
            accepted_snapshots=snapshots,
        )

    monkeypatch.setattr("app.main._build_scan_context", fake_build_scan_context)

    run_response = asyncio.run(run_observation_collection(symbols="BTC"))
    assert run_response["stored_count"] == 1

    latest = store.latest(limit=1)[0]
    raw = latest.raw_opportunity_json

    assert raw["price_spread_bps"] == opportunity.price_spread_bps
    assert raw["funding_spread_bps"] == opportunity.funding_spread_bps
    assert raw["risk_adjusted_edge_bps"] == 10.8
    assert raw["estimated_net_edge_bps"] == opportunity.net_edge_bps
    assert raw["route_key"] == "BTC:binance->okx"
    assert raw["opportunity_type"] == "tradable"
    assert raw["execution_mode"] == "normal"
    assert raw["final_position_pct"] == 0.025
    assert raw["why_not_tradable"] in {"live candidate", "mixed funding semantics"}
    assert raw["replay_confidence_label"] in {"high", "medium", "low", "very_low"}
    assert isinstance(raw["replay_passes_min_trade_gate"], bool)
    assert raw["risk_flags"] == ["mixed_funding_sources"]
    assert isinstance(raw["replay_summary"], str)
    assert isinstance(raw["replay_net_after_cost_bps"], float)


def test_opportunities_reads_rich_fields_from_real_observe_run_row(tmp_path, monkeypatch) -> None:
    store = ObservationStore(str(tmp_path / "observations.sqlite3"))
    monkeypatch.setattr("app.main.observation_store", store)

    opportunity = _build_opportunity().model_copy(
        update={
            "route_key": None,
            "opportunity_type": None,
            "opportunity_grade": "tradable",
            "execution_mode": "normal",
            "final_position_pct": 0.02,
            "risk_flags": [],
            "risk_adjusted_edge_bps": 9.6,
        }
    )
    snapshots = [_snapshot("binance", 100.0, -0.0002), _snapshot("okx", 100.23, 0.0002)]

    async def fake_build_scan_context(requested_symbols: list[str]) -> _ScanContext:
        return _ScanContext(
            requested_symbols=requested_symbols,
            opportunities=[opportunity],
            snapshot_errors=[],
            accepted_snapshots=snapshots,
        )

    monkeypatch.setattr("app.main._build_scan_context", fake_build_scan_context)
    asyncio.run(run_observation_collection(symbols="BTC"))

    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    response = client.get("/api/v1/opportunities", params={"symbols": "BTC", "top_n": 1})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    item = payload[0]

    assert item["price_spread_bps"] == opportunity.price_spread_bps
    assert item["funding_spread_bps"] == opportunity.funding_spread_bps
    assert item["risk_adjusted_edge_bps"] == 9.6
    assert item["estimated_net_edge_bps"] == opportunity.net_edge_bps
    assert item["route_key"] == "BTC:binance->okx"
    assert item["opportunity_type"] == "tradable"
    assert item["execution_mode"] == "normal"
    assert item["final_position_pct"] == 0.02
    assert item["replay_confidence_label"] in {"high", "medium", "low", "very_low"}
    assert isinstance(item["replay_passes_min_trade_gate"], bool)
    assert isinstance(item["replay_summary"], str)
    assert item["is_test"] is False
