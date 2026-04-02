import asyncio
import time

import pytest
from fastapi import HTTPException

from app import main as main_module
from app.main import get_opportunities, get_replay_preview, get_replay_profile_compare
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.arbitrage_scanner import ArbitrageScannerService
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
    assert "research_metrics" in replay
    assert "edge_retention_rate" in replay["research_metrics"]
    assert "funding_capture_rate" in replay["research_metrics"]
    assert "replay_cost_drag_bps" in replay["research_metrics"]
    assert "research_confidence_score" in replay["research_metrics"]


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


def test_replay_profile_compare_endpoint_response_shape(monkeypatch) -> None:
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
        get_replay_profile_compare(
            symbols="BTC",
            limit=5,
            profiles=None,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    assert response["compare_count"] == 1
    item = response["items"][0]
    assert item["cluster_id"] == "BTC|binance|funding_capture"
    assert item["route_rank"] == 1
    assert item["symbol"] == "BTC"
    assert item["long_exchange"] == "binance"
    assert item["short_exchange"] == "okx"
    assert "execution_mode" in item
    assert "opportunity_grade" in item
    assert "normal_blockers" in item
    assert "normal_promotion_reasons" in item
    assert "size_up_blockers" in item
    assert "size_up_promotion_reasons" in item
    assert "extended_size_up_risk_eligible" in item
    assert "extended_size_up_risk_blockers" in item

    profile_result = item["profile_results"][0]
    assert "profile_name" in profile_result
    assert "resolved_execution_extended_size_up_enabled" in profile_result
    assert "resolved_execution_target_leverage" in profile_result
    assert "resolved_execution_max_allowed_leverage" in profile_result
    assert "resolved_execution_required_liquidation_buffer_pct" in profile_result
    assert "extended_size_up_execution_ready" in profile_result
    assert "extended_size_up_execution_blockers" in profile_result
    assert "why_not_explainability" in profile_result
    assert "opportunity_blockers" in profile_result["why_not_explainability"]
    assert "profile_policy_blockers" in profile_result["why_not_explainability"]
    assert "execution_capacity_blockers" in profile_result["why_not_explainability"]
    assert "execution_max_single_cap_pct" in profile_result
    assert "execution_cap_reasons" in profile_result
    assert "replay" in profile_result


def test_replay_profile_compare_defaults_to_three_profiles(monkeypatch) -> None:
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
        get_replay_profile_compare(
            symbols="BTC",
            limit=5,
            profiles=None,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    assert response["compared_profiles"] == ["dev_default", "paper_conservative", "live_conservative"]
    assert [result["profile_name"] for result in response["items"][0]["profile_results"]] == [
        "dev_default",
        "paper_conservative",
        "live_conservative",
    ]


def test_replay_profile_compare_profiles_can_differ_on_execution_readiness_and_cap(monkeypatch) -> None:
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

    original_build_opportunities = ArbitrageScannerService.build_opportunities

    def fake_build_opportunities(self: ArbitrageScannerService, snapshots: list[MarketSnapshot]):
        opportunities = original_build_opportunities(self, snapshots)
        return [
            opportunity.model_copy(
                update={
                    "remaining_total_cap_pct": 0.0,
                    "remaining_symbol_cap_pct": 0.0,
                    "remaining_long_exchange_cap_pct": 0.0,
                    "remaining_short_exchange_cap_pct": 0.0,
                }
            )
            for opportunity in opportunities
        ]

    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(
        get_replay_profile_compare(
            symbols="BTC",
            limit=1,
            profiles="dev_default,paper_conservative,live_conservative",
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    profile_results = {
        result["profile_name"]: result
        for result in response["items"][0]["profile_results"]
    }

    assert profile_results["dev_default"]["extended_size_up_execution_ready"] is False
    assert profile_results["dev_default"]["execution_max_single_cap_pct"] == 0.05
    assert profile_results["paper_conservative"]["extended_size_up_execution_ready"] is False
    assert profile_results["paper_conservative"]["execution_max_single_cap_pct"] == 0.05
    assert profile_results["live_conservative"]["extended_size_up_execution_ready"] is False
    assert profile_results["live_conservative"]["execution_max_single_cap_pct"] == 0.03


def test_replay_profile_compare_why_not_groups_policy_and_capacity_blockers(monkeypatch) -> None:
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
    monkeypatch.setattr(main_module.settings, "execution_extended_size_up_enabled", True)
    monkeypatch.setattr(main_module.settings, "execution_live_remaining_total_cap_pct", 0.07)
    original_build_opportunities = ArbitrageScannerService.build_opportunities

    def fake_build_opportunities(self: ArbitrageScannerService, snapshots: list[MarketSnapshot]):
        opportunities = original_build_opportunities(self, snapshots)
        return [
            opportunity.model_copy(
                update={
                    "execution_mode": "size_up",
                    "extended_size_up_risk_eligible": True,
                    "extended_size_up_risk_blockers": [],
                    "remaining_total_cap_pct": 0.0,
                    "remaining_symbol_cap_pct": 0.0,
                    "remaining_long_exchange_cap_pct": 0.0,
                    "remaining_short_exchange_cap_pct": 0.0,
                }
            )
            for opportunity in opportunities
        ]

    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(
        get_replay_profile_compare(
            symbols="BTC",
            limit=1,
            profiles="dev_default,paper_conservative",
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    item = response["items"][0]
    assert isinstance(item["normal_blockers"], list)
    assert isinstance(item["normal_promotion_reasons"], list)
    assert isinstance(item["size_up_blockers"], list)
    assert isinstance(item["size_up_promotion_reasons"], list)
    assert isinstance(item["extended_size_up_risk_eligible"], bool)
    assert isinstance(item["extended_size_up_risk_blockers"], list)

    profile_results = {result["profile_name"]: result for result in item["profile_results"]}
    dev_result = profile_results["dev_default"]
    paper_result = profile_results["paper_conservative"]

    assert dev_result["why_not_explainability"]["profile_policy_blockers"] == []
    assert "extended_size_up_not_enabled_in_execution_policy" in paper_result["why_not_explainability"][
        "profile_policy_blockers"
    ]
    assert "insufficient_live_total_capacity_for_extended_size_up" in dev_result["why_not_explainability"][
        "execution_capacity_blockers"
    ]
    assert paper_result["why_not_explainability"]["execution_capacity_blockers"] == []
    assert "not_in_size_up_mode" not in dev_result["why_not_explainability"]["profile_policy_blockers"]
    assert "not_in_size_up_mode" not in dev_result["why_not_explainability"]["execution_capacity_blockers"]


def test_replay_profile_compare_includes_replay_for_each_profile_result(monkeypatch) -> None:
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
        get_replay_profile_compare(
            symbols="BTC",
            limit=1,
            profiles=None,
            holding_mode="to_next_funding",
            holding_minutes=None,
            slippage_bps_per_leg=1.0,
            extra_exit_slippage_bps_per_leg=0.5,
            latency_decay_bps=0.2,
            borrow_or_misc_cost_bps=0.0,
        )
    )

    for profile_result in response["items"][0]["profile_results"]:
        replay = profile_result["replay"]
        assert "entry_net_edge_bps" in replay
        assert "net_realized_edge_bps" in replay
