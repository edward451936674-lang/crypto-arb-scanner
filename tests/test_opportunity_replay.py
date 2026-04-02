import time

from app.models.market import MarketSnapshot, ReplayAssumptions
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.opportunity_replay import OpportunityReplayService



def _snapshot(
    exchange: str,
    mark_price: float,
    *,
    funding_rate: float,
    funding_period_hours: int,
    next_funding_minutes: int | None = None,
) -> MarketSnapshot:
    ts_ms = int(time.time() * 1000)
    return MarketSnapshot(
        exchange=exchange,
        venue_type="cex",
        base_symbol="BTC",
        normalized_symbol="BTC-USDT-PERP",
        instrument_id=f"{exchange}-BTC",
        mark_price=mark_price,
        funding_rate=funding_rate,
        funding_period_hours=funding_period_hours,
        next_funding_time_ms=None if next_funding_minutes is None else ts_ms + next_funding_minutes * 60_000,
        timestamp_ms=ts_ms,
    )



def _build_pair(
    *,
    long_period_hours: int = 8,
    short_period_hours: int = 8,
    next_funding_minutes: int | None = 30,
):
    scanner = ArbitrageScannerService()
    long_snapshot = _snapshot(
        "binance",
        100.0,
        funding_rate=-0.0002,
        funding_period_hours=long_period_hours,
        next_funding_minutes=next_funding_minutes,
    )
    short_snapshot = _snapshot(
        "okx",
        100.25,
        funding_rate=0.0002,
        funding_period_hours=short_period_hours,
        next_funding_minutes=next_funding_minutes,
    )
    candidate = scanner.build_opportunities([long_snapshot, short_snapshot])[0]
    return candidate, long_snapshot, short_snapshot



def test_replay_output_shape_has_required_fields() -> None:
    opportunity, long_snapshot, short_snapshot = _build_pair()
    replay = OpportunityReplayService().replay(
        opportunity,
        long_snapshot,
        short_snapshot,
        ReplayAssumptions(
            holding_mode="to_next_funding",
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
        ),
    )

    assert replay.symbol == "BTC"
    assert replay.long_exchange == "binance"
    assert replay.short_exchange == "okx"
    assert replay.entry_price_edge_bps == replay.gross_price_edge_bps
    assert replay.entry_net_edge_bps == opportunity.net_edge_bps
    assert isinstance(replay.net_realized_edge_bps, float)
    assert 0.0 <= replay.long_funding_capture_fraction <= 1.0
    assert 0.0 <= replay.short_funding_capture_fraction <= 1.0
    assert 0.0 <= replay.pair_funding_capture_fraction <= 1.0



def test_funding_capture_respects_8h_period() -> None:
    opportunity, long_snapshot, short_snapshot = _build_pair(long_period_hours=8, short_period_hours=8, next_funding_minutes=60)

    replay = OpportunityReplayService().replay(
        opportunity,
        long_snapshot,
        short_snapshot,
        ReplayAssumptions(
            holding_mode="to_next_funding",
            slippage_bps_per_leg=0.0,
            extra_exit_slippage_bps_per_leg=0.0,
            latency_decay_bps=0.0,
        ),
    )

    assert replay.holding_minutes == 60
    assert round(replay.long_funding_capture_fraction, 6) == round(60 / (8 * 60), 6)
    assert round(replay.short_funding_capture_fraction, 6) == round(60 / (8 * 60), 6)
    assert round(replay.pair_funding_capture_fraction, 6) == round(60 / (8 * 60), 6)



def test_funding_capture_respects_4h_period() -> None:
    opportunity, long_snapshot, short_snapshot = _build_pair(long_period_hours=4, short_period_hours=4, next_funding_minutes=60)

    replay = OpportunityReplayService().replay(
        opportunity,
        long_snapshot,
        short_snapshot,
        ReplayAssumptions(
            holding_mode="to_next_funding",
            slippage_bps_per_leg=0.0,
            extra_exit_slippage_bps_per_leg=0.0,
            latency_decay_bps=0.0,
        ),
    )

    assert replay.holding_minutes == 60
    assert round(replay.long_funding_capture_fraction, 6) == round(60 / (4 * 60), 6)
    assert round(replay.short_funding_capture_fraction, 6) == round(60 / (4 * 60), 6)
    assert round(replay.pair_funding_capture_fraction, 6) == round(60 / (4 * 60), 6)



def test_fixed_minutes_holding_mode_uses_explicit_window() -> None:
    opportunity, long_snapshot, short_snapshot = _build_pair(next_funding_minutes=5)

    replay = OpportunityReplayService().replay(
        opportunity,
        long_snapshot,
        short_snapshot,
        ReplayAssumptions(
            holding_mode="fixed_minutes",
            holding_minutes=120,
            slippage_bps_per_leg=0.0,
            extra_exit_slippage_bps_per_leg=0.0,
            latency_decay_bps=0.0,
        ),
    )

    assert replay.holding_minutes == 120
    assert round(replay.long_funding_capture_fraction, 6) == round(120 / (8 * 60), 6)
    assert round(replay.short_funding_capture_fraction, 6) == round(120 / (8 * 60), 6)
    assert round(replay.pair_funding_capture_fraction, 6) == round(120 / (8 * 60), 6)



def test_replay_costs_reduce_net_edge() -> None:
    opportunity, long_snapshot, short_snapshot = _build_pair(next_funding_minutes=60)
    assumptions = ReplayAssumptions(
        holding_mode="to_next_funding",
        slippage_bps_per_leg=2.0,
        extra_exit_slippage_bps_per_leg=1.0,
        latency_decay_bps=0.8,
        borrow_or_misc_cost_bps=0.6,
    )

    replay = OpportunityReplayService().replay(opportunity, long_snapshot, short_snapshot, assumptions)

    gross = replay.gross_price_edge_bps + replay.realized_funding_bps
    assert replay.net_realized_edge_bps < gross
    assert replay.slippage_bps == 6.0



def test_replay_has_no_external_exchange_dependencies(monkeypatch) -> None:
    def _raise(*args, **kwargs):
        raise AssertionError("external exchange call attempted")

    monkeypatch.setattr("app.exchanges.binance.BinanceClient.fetch_snapshots", _raise)
    monkeypatch.setattr("app.exchanges.okx.OkxClient.fetch_snapshots", _raise)

    opportunity, long_snapshot, short_snapshot = _build_pair()
    replay = OpportunityReplayService().replay(
        opportunity,
        long_snapshot,
        short_snapshot,
        ReplayAssumptions(
            holding_mode="to_next_funding",
            slippage_bps_per_leg=0.0,
            extra_exit_slippage_bps_per_leg=0.0,
            latency_decay_bps=0.0,
        ),
    )

    assert replay.replay_confidence_label in {"high", "medium", "low"}
