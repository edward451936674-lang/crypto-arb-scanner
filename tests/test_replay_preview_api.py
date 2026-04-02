import asyncio
import time

import pytest
from fastapi import HTTPException

from app.main import get_opportunities, get_replay_preview
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.market_data import MarketDataService


def _snapshot(
    exchange: str,
    mark_price: float,
    *,
    base_symbol: str = "BTC",
    funding_rate: float = 0.0,
    funding_rate_source: str = "current",
    funding_period_hours: int = 8,
    next_funding_minutes: int = 60,
    open_interest_usd: float | None = 20_000_000.0,
    quote_volume_24h_usd: float | None = 20_000_000.0,
) -> MarketSnapshot:
    ts_ms = int(time.time() * 1000)
    return MarketSnapshot(
        exchange=exchange,
        venue_type="cex",
        base_symbol=base_symbol,
        normalized_symbol=f"{base_symbol}-USDT-PERP",
        instrument_id=f"{exchange}-{base_symbol}",
        mark_price=mark_price,
        funding_rate=funding_rate,
        funding_rate_source=funding_rate_source,
        funding_period_hours=funding_period_hours,
        open_interest_usd=open_interest_usd,
        quote_volume_24h_usd=quote_volume_24h_usd,
        next_funding_time_ms=ts_ms + next_funding_minutes * 60_000,
        timestamp_ms=ts_ms,
    )


def test_replay_preview_endpoint_returns_expected_shape(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002),
                _snapshot("okx", 100.25, funding_rate=0.0002),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(
        get_replay_preview(
            symbols="BTC",
            limit=5,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    assert response["preview_count"] == 1
    item = response["items"][0]
    assert item["cluster_id"] == "BTC|binance|funding_capture"
    assert item["route_rank"] == 1
    assert item["symbol"] == "BTC"
    assert item["long_exchange"] == "binance"
    assert item["short_exchange"] == "okx"
    assert "execution_mode" in item
    assert "opportunity_grade" in item

    replay = item["replay"]
    assert "entry_price_edge_bps" in replay
    assert "entry_expected_funding_edge_bps" in replay
    assert "entry_net_edge_bps" in replay
    assert "long_funding_capture_fraction" in replay
    assert "short_funding_capture_fraction" in replay
    assert "pair_funding_capture_fraction" in replay


def test_replay_preview_top_n_limiting(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002),
                _snapshot("okx", 100.25, funding_rate=0.0002),
                _snapshot("hyperliquid", 100.1, funding_rate=-0.0001),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(
        get_replay_preview(
            symbols="BTC",
            limit=1,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    assert response["preview_count"] == 1
    assert len(response["items"]) == 1


def test_replay_preview_fixed_minutes_mode(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002, funding_period_hours=8, next_funding_minutes=10),
                _snapshot("okx", 100.25, funding_rate=0.0002, funding_period_hours=8, next_funding_minutes=10),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(
        get_replay_preview(
            symbols="BTC",
            limit=5,
            holding_mode="fixed_minutes",
            holding_minutes=120,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )
    replay = response["items"][0]["replay"]
    assert replay["holding_minutes"] == 120
    assert round(replay["pair_funding_capture_fraction"], 6) == round(120 / (8 * 60), 6)


def test_replay_preview_fixed_minutes_requires_holding_minutes(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(requested_symbols=symbols, snapshots=[], errors=[])

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            get_replay_preview(
                symbols="BTC",
                limit=5,
                holding_mode="fixed_minutes",
                holding_minutes=None,
                slippage_bps_per_leg=1.0,
                extra_exit_slippage_bps_per_leg=0.5,
                latency_decay_bps=0.2,
                borrow_or_misc_cost_bps=0.0,
            )
        )

    assert exc.value.status_code == 400


def test_replay_preview_fixed_minutes_rejects_zero_holding_minutes(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(requested_symbols=symbols, snapshots=[], errors=[])

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            get_replay_preview(
                symbols="BTC",
                limit=5,
                holding_mode="fixed_minutes",
                holding_minutes=0,
                slippage_bps_per_leg=1.0,
                extra_exit_slippage_bps_per_leg=0.5,
                latency_decay_bps=0.2,
                borrow_or_misc_cost_bps=0.0,
            )
        )

    assert exc.value.status_code == 400


def test_replay_preview_to_next_funding_mode(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002, funding_period_hours=8, next_funding_minutes=30),
                _snapshot("okx", 100.25, funding_rate=0.0002, funding_period_hours=8, next_funding_minutes=45),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(
        get_replay_preview(
            symbols="BTC",
            limit=5,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )
    replay = response["items"][0]["replay"]
    assert replay["holding_minutes"] == 30
    assert round(replay["pair_funding_capture_fraction"], 6) == round(30 / (8 * 60), 6)


def test_replay_preview_has_no_external_exchange_dependencies(monkeypatch) -> None:
    def _raise(*args, **kwargs):
        raise AssertionError("external exchange call attempted")

    monkeypatch.setattr("app.exchanges.binance.BinanceClient.fetch_snapshots", _raise)
    monkeypatch.setattr("app.exchanges.okx.OkxClient.fetch_snapshots", _raise)

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002),
                _snapshot("okx", 100.25, funding_rate=0.0002),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(
        get_replay_preview(
            symbols="BTC",
            limit=5,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )
    assert response["preview_count"] == 1


def test_opportunities_api_behavior_remains_unchanged(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="latest_reported"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["execution_mode"] in {"paper", "small_probe", "normal", "size_up"}
    assert item["suggested_position_pct"] >= item["final_position_pct"]
    assert "replay" not in item
