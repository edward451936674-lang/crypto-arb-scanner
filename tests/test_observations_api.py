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
