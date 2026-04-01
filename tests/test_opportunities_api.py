import asyncio
import time

from app.main import get_opportunities
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.arbitrage_scanner import ArbitrageScannerService, EXECUTION_RISK_CONFIGS, ExecutionRiskConfig
from app.services.execution_sizing_policy import (
    ExecutionAccountInputs,
    ExecutionSizingPolicyEvaluator,
)
from app.services.market_data import MarketDataService


def _snapshot(
    exchange: str,
    mark_price: float,
    *,
    base_symbol: str = "BTC",
    index_price: float | None = None,
    last_price: float | None = None,
    funding_rate: float = 0.0,
    funding_rate_source: str = "current",
    funding_period_hours: int = 8,
    open_interest_usd: float | None = 15_000_000.0,
    quote_volume_24h_usd: float | None = 25_000_000.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=exchange,
        venue_type="dex" if exchange in {"hyperliquid", "lighter"} else "cex",
        base_symbol=base_symbol,
        normalized_symbol=f"{base_symbol}-USDT-PERP",
        instrument_id=f"{exchange}-{base_symbol}",
        mark_price=mark_price,
        index_price=index_price,
        last_price=last_price,
        funding_rate=funding_rate,
        funding_rate_source=funding_rate_source,
        funding_period_hours=funding_period_hours,
        open_interest_usd=open_interest_usd,
        quote_volume_24h_usd=quote_volume_24h_usd,
        timestamp_ms=int(time.time() * 1000),
    )


def test_get_opportunities_returns_ranked_items(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["symbol"] == "BTC"
    assert item["cluster_id"] == "BTC|binance|funding_capture"
    assert item["is_primary_route"] is True
    assert item["route_rank"] == 1
    assert item["conviction_label"] == "high"
    assert item["size_up_eligible"] is True
    assert item["execution_mode"] == "size_up"
    assert item["is_executable_now"] is True
    assert item["suggested_position_pct"] == item["max_position_pct"]
    assert item["final_position_pct"] <= item["suggested_position_pct"]
    assert item["portfolio_rank"] == 1


def test_get_opportunities_filters_non_positive_net_edge(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.06,
                    funding_rate_source="current",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []


def test_strong_primary_route_can_be_medium_conviction_and_normal(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate=-0.0002,
                    funding_rate_source="latest_reported",
                ),
                _snapshot(
                    "okx",
                    100.22,
                    funding_rate=0.0002,
                    funding_rate_source="current",
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "tradable"
    assert item["is_primary_route"] is True
    assert item["conviction_score"] >= 0.50
    assert item["execution_mode"] == "normal"
    assert item["is_executable_now"] is True
    assert item["size_up_eligible"] is False
    assert "adequate_liquidity" in item["conviction_drivers"]
    assert "primary_route" in item["conviction_drivers"]


def test_missing_liquidity_routes_score_lower_than_clean_routes() -> None:
    scanner = ArbitrageScannerService()

    clean_route = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported", funding_period_hours=8),
            _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current", funding_period_hours=8),
        ]
    )[0]
    missing_liquidity_route = scanner.build_opportunities(
        [
            _snapshot(
                "lighter",
                100.0,
                funding_rate=-0.0002,
                funding_rate_source="latest_reported",
                funding_period_hours=8,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.22,
                funding_rate=0.0002,
                funding_rate_source="current",
                funding_period_hours=8,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )[0]

    assert clean_route.conviction_score > missing_liquidity_route.conviction_score
    assert clean_route.suggested_position_pct > missing_liquidity_route.suggested_position_pct
    assert missing_liquidity_route.execution_mode == "small_probe"
    assert missing_liquidity_route.is_executable_now is True
    assert "small_probe_despite_missing_liquidity_data" in missing_liquidity_route.execution_mode_drivers
    assert "blocked_from_normal_due_to_missing_liquidity_data" in missing_liquidity_route.execution_mode_drivers


def test_get_opportunities_keeps_watchlist_items_as_small_probe(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate_source="latest_reported"),
                _snapshot("okx", 100.17, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["opportunity_grade"] == "watchlist"
    assert item["execution_mode"] == "small_probe"
    assert item["is_executable_now"] is True
    assert 0.0 < item["suggested_position_pct"] < item["max_position_pct"]
    assert "below_normal_edge_threshold" in item["execution_mode_drivers"]


def test_get_opportunities_sets_paper_mode_to_zero_position(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.24,
                    funding_rate_source="last_settled_fallback",
                    funding_period_hours=8,
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["execution_mode"] == "paper"
    assert item["is_executable_now"] is False
    assert item["suggested_position_pct"] == 0.0
    assert item["final_position_pct"] == 0.0
    assert "paper_mode" in item["portfolio_reject_reasons"]




def _assert_execution_overlay_fields(item: dict) -> None:
    assert "extended_size_up_execution_ready" in item
    assert "extended_size_up_execution_blockers" in item
    assert "execution_max_single_cap_pct" in item
    assert "execution_cap_reasons" in item


def test_get_opportunities_controlled_strong_size_up_candidate(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    _assert_execution_overlay_fields(item)
    assert item["execution_mode"] == "size_up"
    assert item["size_up_eligible"] is True
    assert item["size_up_promotion_reasons"]
    assert item["mode_base_cap_pct"] == 0.05
    assert item["final_single_cap_pct"] <= 0.05


def test_get_opportunities_controlled_normal_not_size_up_candidate(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported"),
                _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    _assert_execution_overlay_fields(item)
    assert item["execution_mode"] == "normal"
    assert item["size_up_eligible"] is False
    assert item["size_up_blockers"]
    assert item["normal_promotion_reasons"]


def test_get_opportunities_controlled_paper_watchlist_candidate(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.24,
                    funding_rate_source="last_settled_fallback",
                    funding_period_hours=8,
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    _assert_execution_overlay_fields(item)
    assert item["execution_mode"] == "paper"
    assert item["final_position_pct"] == 0.0
    assert item["portfolio_reject_reasons"]


def test_get_opportunities_exposes_execution_readiness_overlay(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    from app.services.execution_sizing_policy import ExecutionSizingDecision

    monkeypatch.setattr(
        ExecutionSizingPolicyEvaluator,
        "evaluate",
        staticmethod(
            lambda opportunity, account_inputs: ExecutionSizingDecision(
                extended_size_up_execution_ready=True,
                extended_size_up_execution_blockers=["none"],
                execution_max_single_cap_pct=0.037,
                execution_cap_reasons=["capped_by_live_remaining_symbol"],
            )
        ),
    )

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["extended_size_up_execution_ready"] is True
    assert item["extended_size_up_execution_blockers"] == ["none"]
    assert item["execution_max_single_cap_pct"] == 0.037
    assert item["execution_cap_reasons"] == ["capped_by_live_remaining_symbol"]


def test_get_opportunities_natural_extended_size_up_risk_eligible(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    original_size_up_config = EXECUTION_RISK_CONFIGS["size_up"]
    EXECUTION_RISK_CONFIGS["size_up"] = ExecutionRiskConfig(1.5, 2.0, 28.0)
    try:
        response = asyncio.run(get_opportunities(symbols="BTC"))
    finally:
        EXECUTION_RISK_CONFIGS["size_up"] = original_size_up_config

    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["execution_mode"] == "size_up"
    assert item["size_up_eligible"] is True
    assert item["extended_size_up_risk_eligible"] is True
    assert item["extended_size_up_risk_blockers"] == []
    assert "extended_size_up_execution_ready" in item
    assert "execution_max_single_cap_pct" in item
    assert item["final_single_cap_pct"] <= 0.05


def test_routes_with_same_long_exchange_share_cluster_and_single_primary() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
        ]
    )

    lighter_cluster = [item for item in opportunities if item.cluster_id == "BTC|lighter|funding_capture"]
    assert len(lighter_cluster) == 2
    assert sum(1 for item in lighter_cluster if item.is_primary_route) == 1
    assert [item.route_rank for item in lighter_cluster] == [1, 2]
    assert all(item.long_exchange == "lighter" for item in lighter_cluster)


def test_only_primary_routes_with_medium_plus_conviction_can_be_normal() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
            _snapshot(
                "hyperliquid",
                103.0,
                funding_rate=0.002,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    normal_routes = [item for item in opportunities if item.execution_mode == "normal"]
    assert all(item.is_primary_route for item in normal_routes)
    assert all(item.conviction_score >= 0.50 for item in normal_routes)


def test_secondary_routes_usually_fall_to_small_probe() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot("lighter", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("binance", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("okx", 102.0, funding_rate=0.0012, funding_rate_source="current"),
        ]
    )

    secondary_routes = [item for item in opportunities if item.route_rank and item.route_rank > 1]
    assert secondary_routes
    assert all(item.execution_mode == "small_probe" for item in secondary_routes)


def test_size_up_remains_strict_for_noisy_mismatched_routes() -> None:
    scanner = ArbitrageScannerService()

    opportunities = scanner.build_opportunities(
        [
            _snapshot(
                "lighter",
                100.0,
                funding_rate=-0.0004,
                funding_rate_source="estimated_current",
                funding_period_hours=4,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.32,
                funding_rate=0.0004,
                funding_rate_source="last_settled_fallback",
                funding_period_hours=8,
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.opportunity_grade == "tradable"
    assert item.size_up_eligible is False
    assert item.execution_mode != "size_up"


def test_execution_mode_paper_when_risk_adjusted_edge_below_six() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.00125, funding_rate_source=None),
            _snapshot("okx", 100.045, funding_rate=0.00125, funding_rate_source=None),
        ]
    )
    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.risk_adjusted_edge_bps < 6
    assert item.execution_mode == "paper"
    assert item.is_executable_now is False
    assert "paper_due_to_low_risk_adjusted_edge" in item.execution_mode_drivers


def test_execution_mode_normal_only_when_thresholds_met() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported", funding_period_hours=8),
            _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current", funding_period_hours=8),
        ]
    )
    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.execution_mode == "normal"
    assert item.is_primary_route is True
    assert item.conviction_score >= 0.50
    assert item.funding_confidence_score >= 0.55
    assert item.normal_required_edge_bps == 10.0
    assert item.edge_buffer_bps >= 0.0
    assert item.normal_eligibility_score > 0.0
    assert item.mode_base_cap_pct == 0.02
    assert item.is_executable_now is True


def test_execution_mode_size_up_requires_all_strict_conditions() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.execution_mode == "size_up"
    assert item.size_up_eligible is True
    assert item.mode_base_cap_pct == 0.05
    assert item.is_executable_now is True
    assert "strong_risk_adjusted_edge" in item.execution_mode_drivers


def test_is_executable_now_matches_execution_mode() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 100.2, funding_rate=0.0002, funding_rate_source="estimated_current", open_interest_usd=None, quote_volume_24h_usd=None),
        ]
    )
    assert opportunities
    for item in opportunities:
        expected = item.execution_mode in {"small_probe", "normal", "size_up"}
        assert item.is_executable_now is expected


def test_portfolio_total_allocated_never_exceeds_cap() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    total_final = sum(item.final_position_pct for item in opportunities)
    assert total_final <= 0.20 + 1e-12
    assert all(item.final_position_pct <= item.suggested_position_pct + 1e-12 for item in opportunities)


def test_portfolio_symbol_cap_is_enforced() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    symbol_total = sum(item.final_position_pct for item in opportunities if item.symbol == "BTC")
    assert symbol_total <= 0.08 + 1e-12
    capped = [item for item in opportunities if "capped_by_symbol_limit" in item.portfolio_clamp_reasons]
    assert capped


def test_portfolio_exchange_cap_is_enforced() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("binance", 200.0, base_symbol="ETH", funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 202.0, base_symbol="ETH", funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 204.0, base_symbol="ETH", funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    assert all(item.portfolio_long_exchange_used_after <= 0.10 + 1e-12 for item in opportunities)
    assert all(item.portfolio_short_exchange_used_after <= 0.10 + 1e-12 for item in opportunities)
    has_exchange_clamp = any(
        "capped_by_long_exchange_limit" in item.portfolio_clamp_reasons
        or "capped_by_short_exchange_limit" in item.portfolio_clamp_reasons
        for item in opportunities
    )
    assert has_exchange_clamp


def test_opportunities_still_visible_when_final_allocation_zero() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="current"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("hyperliquid", 102.0, funding_rate=0.001, funding_rate_source="current"),
            _snapshot("lighter", 103.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )
    zero_alloc = [item for item in opportunities if item.final_position_pct == 0.0]
    assert zero_alloc
    assert all(item.portfolio_rank is not None for item in zero_alloc)


def test_get_opportunities_excludes_invalid_snapshots_before_scanner(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        invalid = _snapshot("hyperliquid", 102.0)
        invalid.mark_price = -1.0
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
                invalid,
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert len(response["opportunities"]) == 1


def test_get_opportunities_excludes_suspicious_snapshots_before_scanner(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        suspicious = _snapshot(
            "hyperliquid",
            100.5,
            index_price=200.0,
            last_price=200.0,
        )
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
                suspicious,
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert len(response["opportunities"]) == 1


def test_get_opportunities_accepts_healthy_and_degraded_snapshots(monkeypatch) -> None:
    now_ms = int(time.time() * 1000)

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        healthy = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported")
        healthy.timestamp_ms = now_ms
        degraded = _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current")
        degraded.timestamp_ms = now_ms - 130_000
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[healthy, degraded],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert len(response["opportunities"]) == 1


def test_get_opportunities_mixed_collection_uses_only_gate_accepted(monkeypatch) -> None:
    now_ms = int(time.time() * 1000)

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        healthy = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported")
        healthy.timestamp_ms = now_ms
        degraded = _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current")
        degraded.timestamp_ms = now_ms - 130_000
        suspicious = _snapshot("hyperliquid", 100.5, index_price=200.0, last_price=200.0)
        invalid = _snapshot("lighter", 99.5)
        invalid.mark_price = -2.0
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[healthy, degraded, suspicious, invalid],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert len(response["opportunities"]) == 1


def test_get_opportunities_passes_only_gate_accepted_snapshots_to_scanner(monkeypatch) -> None:
    now_ms = int(time.time() * 1000)
    healthy = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported")
    healthy.timestamp_ms = now_ms
    degraded = _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current")
    degraded.timestamp_ms = now_ms - 130_000
    suspicious = _snapshot("hyperliquid", 100.5, index_price=200.0, last_price=200.0)
    suspicious.timestamp_ms = now_ms
    invalid = _snapshot("lighter", 99.5)
    invalid.timestamp_ms = now_ms
    invalid.mark_price = -2.0

    captured: dict[str, list[MarketSnapshot]] = {}

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[healthy, degraded, suspicious, invalid],
            errors=[],
        )

    def fake_build_opportunities(
        self: ArbitrageScannerService,
        snapshots: list[MarketSnapshot],
    ) -> list[object]:
        captured["snapshots"] = snapshots
        return []

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(get_opportunities(symbols="BTC"))

    assert response["opportunities"] == []
    assert [snapshot.exchange for snapshot in captured["snapshots"]] == ["binance", "okx"]
    assert captured["snapshots"][0].data_quality_status == "healthy"
    assert captured["snapshots"][1].data_quality_status == "degraded"


def test_degraded_quality_propagates_to_opportunity_risk_ranking_and_sizing() -> None:
    scanner = ArbitrageScannerService()
    healthy_opportunity = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0]
    degraded_leg = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported").model_copy(
        update={
            "data_quality_status": "degraded",
            "data_quality_score": 0.75,
            "data_quality_flags": ["cross_exchange_price_outlier"],
        }
    )
    degraded_opportunity = scanner.build_opportunities(
        [
            degraded_leg,
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0]

    assert degraded_opportunity.data_quality_status == "degraded"
    assert degraded_opportunity.data_quality_score == 0.75
    assert "cross_exchange_price_outlier" in degraded_opportunity.data_quality_flags
    assert "one_leg_degraded" in degraded_opportunity.data_quality_drivers
    assert "degraded_cross_exchange_price_signal" in degraded_opportunity.data_quality_drivers
    assert degraded_opportunity.data_quality_penalty_multiplier == 0.85
    assert degraded_opportunity.data_quality_adjusted_edge_bps == (
        degraded_opportunity.risk_adjusted_edge_bps * degraded_opportunity.data_quality_penalty_multiplier
    )
    assert degraded_opportunity.edge_buffer_bps == (
        degraded_opportunity.data_quality_adjusted_edge_bps - degraded_opportunity.normal_required_edge_bps
    )
    assert "degraded_data_quality" in degraded_opportunity.risk_flags
    assert "cross_exchange_price_quality_risk" in degraded_opportunity.risk_flags
    assert degraded_opportunity.suggested_position_pct < healthy_opportunity.suggested_position_pct
    assert degraded_opportunity.execution_mode != "size_up"
    assert degraded_opportunity.size_up_eligible is False


def test_degraded_quality_blocks_size_up_for_otherwise_size_up_candidate() -> None:
    scanner = ArbitrageScannerService()
    degraded_long = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported").model_copy(
        update={
            "data_quality_status": "degraded",
            "data_quality_score": 0.8,
            "data_quality_flags": ["timestamp_stale"],
        }
    )
    opportunities = scanner.build_opportunities(
        [
            degraded_long,
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )

    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.data_quality_status == "degraded"
    assert item.execution_mode in {"small_probe", "normal"}
    assert item.execution_mode != "size_up"
    assert item.size_up_eligible is False


def test_normal_mode_exposes_explainability_metadata(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported"),
                _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(get_opportunities(symbols="BTC"))

    item = response["opportunities"][0]
    assert item["execution_mode"] == "normal"
    assert item["soft_risk_flag_count"] == 1
    assert item["normal_blockers"] == []
    assert "meets_normal_thresholds" in item["normal_promotion_reasons"]


def test_negative_edge_buffer_stays_small_probe_with_explicit_normal_blockers(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=0.0, funding_rate_source="current"),
                _snapshot("okx", 100.18, funding_rate=0.0, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(get_opportunities(symbols="BTC"))

    item = response["opportunities"][0]
    assert item["execution_mode"] == "small_probe"
    assert "below_normal_edge_threshold" in item["normal_blockers"]
    assert "below_normal_edge_threshold" in item["execution_mode_drivers"]
    assert "meets_normal_thresholds" not in item["normal_promotion_reasons"]


def test_missing_liquidity_small_probe_exposes_liquidity_normal_blocker(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot(
                    "binance",
                    100.0,
                    funding_rate=-0.0002,
                    funding_rate_source="latest_reported",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
                _snapshot(
                    "okx",
                    100.22,
                    funding_rate=0.0002,
                    funding_rate_source="current",
                    open_interest_usd=None,
                    quote_volume_24h_usd=None,
                ),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    response = asyncio.run(get_opportunities(symbols="BTC"))

    item = response["opportunities"][0]
    assert item["execution_mode"] == "small_probe"
    assert "missing_liquidity_data_blocks_normal" in item["normal_blockers"]
    assert "meets_normal_thresholds" not in item["normal_promotion_reasons"]


def test_exactly_two_soft_risks_not_labeled_too_many() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported", funding_period_hours=8),
            _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current", funding_period_hours=4),
        ]
    )

    item = opportunities[0]
    assert item.soft_risk_flag_count == 2
    assert "too_many_soft_risk_flags" not in item.normal_blockers


def test_three_or_more_soft_risks_blocks_normal_with_too_many_flag() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.002, funding_rate_source="latest_reported", funding_period_hours=8),
            _snapshot("okx", 100.32, funding_rate=0.002, funding_rate_source="current", funding_period_hours=4),
        ]
    )

    item = opportunities[0]
    assert item.soft_risk_flag_count >= 3
    assert item.execution_mode == "small_probe"
    assert "too_many_soft_risk_flags" in item.normal_blockers
    assert "too_many_soft_risk_flags" in item.execution_mode_drivers


def test_size_up_clean_case_includes_size_up_metadata_and_reasons() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )

    assert len(opportunities) == 1
    item = opportunities[0]
    assert item.execution_mode == "size_up"
    assert item.size_up_required_edge_bps == 18.0
    assert item.size_up_edge_buffer_bps == (item.data_quality_adjusted_edge_bps - item.size_up_required_edge_bps)
    assert item.size_up_blockers == []
    assert "meets_size_up_thresholds" in item.size_up_promotion_reasons
    assert item.configured_target_leverage == 2.0
    assert item.configured_max_allowed_leverage == 2.5
    assert item.configured_min_required_liquidation_buffer_pct == 22.0
    assert item.extended_size_up_risk_eligible is False
    assert "configured_liquidation_buffer_requirement_not_strict_enough" in item.extended_size_up_risk_blockers


def test_normal_only_case_has_explicit_size_up_blockers() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot(
                "binance",
                100.0,
                funding_rate=-0.0002,
                funding_rate_source="latest_reported",
            ),
            _snapshot(
                "okx",
                100.22,
                funding_rate=0.0002,
                funding_rate_source="current",
            ),
        ]
    )

    item = opportunities[0]
    assert item.execution_mode == "normal"
    assert "insufficient_size_up_edge_buffer" in item.size_up_blockers
    assert "size_up_not_achieved_blocks_extended_size_up" in item.extended_size_up_risk_blockers
    assert item.size_up_promotion_reasons == []


def test_degraded_data_quality_explicitly_blocks_size_up() -> None:
    scanner = ArbitrageScannerService()
    degraded_long = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported").model_copy(
        update={"data_quality_status": "degraded", "data_quality_score": 0.8}
    )
    opportunities = scanner.build_opportunities(
        [
            degraded_long,
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )

    item = opportunities[0]
    assert item.execution_mode != "size_up"
    assert "degraded_data_quality_blocks_size_up" in item.size_up_blockers


def test_missing_liquidity_explicitly_blocks_size_up() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot(
                "binance",
                100.0,
                funding_rate=-0.0002,
                funding_rate_source="latest_reported",
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.22,
                funding_rate=0.0002,
                funding_rate_source="current",
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )

    item = opportunities[0]
    assert item.execution_mode == "small_probe"
    assert "missing_liquidity_data_blocks_size_up" in item.size_up_blockers
    assert item.extended_size_up_risk_eligible is False
    assert "missing_liquidity_blocks_extended_size_up" in item.extended_size_up_risk_blockers


def test_clean_size_up_can_be_extended_risk_eligible_when_policy_is_strict() -> None:
    scanner = ArbitrageScannerService()
    original_size_up_config = EXECUTION_RISK_CONFIGS["size_up"]
    EXECUTION_RISK_CONFIGS["size_up"] = ExecutionRiskConfig(1.5, 2.0, 28.0)
    try:
        opportunities = scanner.build_opportunities(
            [
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ]
        )
    finally:
        EXECUTION_RISK_CONFIGS["size_up"] = original_size_up_config

    item = opportunities[0]
    assert item.execution_mode == "size_up"
    assert item.extended_size_up_risk_eligible is True
    assert item.extended_size_up_risk_blockers == []


def test_extended_risk_scaffolding_does_not_change_final_single_cap_pct_behavior() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )

    item = opportunities[0]
    assert item.absolute_single_opportunity_cap_pct == 0.05
    assert item.final_single_cap_pct <= 0.05 + 1e-12


def test_execution_policy_blocks_when_risk_not_eligible() -> None:
    scanner = ArbitrageScannerService()
    item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0].model_copy(update={"extended_size_up_risk_eligible": False})

    decision = ExecutionSizingPolicyEvaluator.evaluate(
        item,
        ExecutionAccountInputs(
            extended_size_up_enabled=True,
            live_target_leverage=1.5,
            live_max_allowed_leverage=2.0,
            live_required_liquidation_buffer_pct=28.0,
            live_remaining_total_cap_pct=0.2,
            live_remaining_symbol_cap_pct=0.08,
            live_remaining_long_exchange_cap_pct=0.1,
            live_remaining_short_exchange_cap_pct=0.1,
        ),
    )
    assert decision.extended_size_up_execution_ready is False
    assert "extended_size_up_risk_not_eligible" in decision.extended_size_up_execution_blockers


def test_execution_policy_fully_ready_allows_up_to_eight_percent_cap() -> None:
    scanner = ArbitrageScannerService()
    item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0].model_copy(update={"extended_size_up_risk_eligible": True})

    decision = ExecutionSizingPolicyEvaluator.evaluate(
        item,
        ExecutionAccountInputs(
            extended_size_up_enabled=True,
            live_target_leverage=1.5,
            live_max_allowed_leverage=2.0,
            live_required_liquidation_buffer_pct=28.0,
            live_remaining_total_cap_pct=0.2,
            live_remaining_symbol_cap_pct=0.08,
            live_remaining_long_exchange_cap_pct=0.1,
            live_remaining_short_exchange_cap_pct=0.1,
        ),
    )
    assert decision.extended_size_up_execution_ready is True
    assert decision.execution_max_single_cap_pct == 0.08


def test_execution_policy_disabled_blocks_extended_execution_readiness() -> None:
    scanner = ArbitrageScannerService()
    item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0].model_copy(update={"extended_size_up_risk_eligible": True})

    decision = ExecutionSizingPolicyEvaluator.evaluate(
        item,
        ExecutionAccountInputs(
            extended_size_up_enabled=False,
            live_target_leverage=1.5,
            live_max_allowed_leverage=2.0,
            live_required_liquidation_buffer_pct=28.0,
            live_remaining_total_cap_pct=0.2,
            live_remaining_symbol_cap_pct=0.08,
            live_remaining_long_exchange_cap_pct=0.1,
            live_remaining_short_exchange_cap_pct=0.1,
        ),
    )
    assert decision.extended_size_up_execution_ready is False
    assert "extended_size_up_not_enabled_in_execution_policy" in decision.extended_size_up_execution_blockers


def test_execution_policy_excessive_live_leverage_blocks_readiness() -> None:
    scanner = ArbitrageScannerService()
    item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0].model_copy(update={"extended_size_up_risk_eligible": True})

    decision = ExecutionSizingPolicyEvaluator.evaluate(
        item,
        ExecutionAccountInputs(
            extended_size_up_enabled=True,
            live_target_leverage=2.1,
            live_max_allowed_leverage=2.1,
            live_required_liquidation_buffer_pct=28.0,
            live_remaining_total_cap_pct=0.2,
            live_remaining_symbol_cap_pct=0.08,
            live_remaining_long_exchange_cap_pct=0.1,
            live_remaining_short_exchange_cap_pct=0.1,
        ),
    )
    assert decision.extended_size_up_execution_ready is False
    assert "live_target_leverage_too_high" in decision.extended_size_up_execution_blockers
    assert "live_max_allowed_leverage_too_high" in decision.extended_size_up_execution_blockers


def test_execution_policy_insufficient_capacity_caps_execution_single_cap() -> None:
    scanner = ArbitrageScannerService()
    item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0].model_copy(update={"extended_size_up_risk_eligible": True})

    decision = ExecutionSizingPolicyEvaluator.evaluate(
        item,
        ExecutionAccountInputs(
            extended_size_up_enabled=True,
            live_target_leverage=1.5,
            live_max_allowed_leverage=2.0,
            live_required_liquidation_buffer_pct=28.0,
            live_remaining_total_cap_pct=0.07,
            live_remaining_symbol_cap_pct=0.08,
            live_remaining_long_exchange_cap_pct=0.1,
            live_remaining_short_exchange_cap_pct=0.1,
        ),
    )
    assert decision.extended_size_up_execution_ready is False
    assert decision.execution_max_single_cap_pct == 0.05
    assert "insufficient_live_total_capacity_for_extended_size_up" in decision.extended_size_up_execution_blockers


def test_exactly_two_soft_risks_can_be_normal_but_not_size_up() -> None:
    scanner = ArbitrageScannerService()
    opportunities = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported", funding_period_hours=8),
            _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current", funding_period_hours=4),
        ]
    )

    item = opportunities[0]
    assert item.soft_risk_flag_count == 2
    assert item.execution_mode == "normal"
    assert item.size_up_eligible is False
    assert "too_many_soft_risk_flags_for_size_up" in item.size_up_blockers


def test_size_up_promotion_reasons_only_present_for_size_up_mode() -> None:
    scanner = ArbitrageScannerService()
    size_up_item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0]
    normal_item = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.0002, funding_rate_source="latest_reported"),
            _snapshot("okx", 100.22, funding_rate=0.0002, funding_rate_source="current"),
        ]
    )[0]
    small_probe_item = scanner.build_opportunities(
        [
            _snapshot(
                "binance",
                100.0,
                funding_rate=-0.0002,
                funding_rate_source="latest_reported",
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
            _snapshot(
                "okx",
                100.22,
                funding_rate=0.0002,
                funding_rate_source="current",
                open_interest_usd=None,
                quote_volume_24h_usd=None,
            ),
        ]
    )[0]

    assert size_up_item.execution_mode == "size_up"
    assert size_up_item.size_up_promotion_reasons
    assert normal_item.execution_mode == "normal"
    assert normal_item.size_up_promotion_reasons == []
    assert small_probe_item.execution_mode == "small_probe"
    assert small_probe_item.size_up_promotion_reasons == []


def test_get_opportunities_hydrates_execution_sizing_fields(monkeypatch) -> None:
    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(
            requested_symbols=symbols,
            snapshots=[
                _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
                _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
            ],
            errors=[],
        )

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    item = response["opportunities"][0]

    assert "extended_size_up_execution_ready" in item
    assert "extended_size_up_execution_blockers" in item
    assert "execution_max_single_cap_pct" in item
    assert "execution_cap_reasons" in item
    assert item["final_single_cap_pct"] <= 0.05 + 1e-12


def test_risk_eligible_size_up_can_be_execution_ready_under_default_inputs(monkeypatch) -> None:
    scanner = ArbitrageScannerService()
    base_opportunity = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0]
    execution_ready_opportunity = base_opportunity.model_copy(
        update={
            "execution_mode": "size_up",
            "extended_size_up_risk_eligible": True,
            "remaining_total_cap_pct": 0.20,
            "remaining_symbol_cap_pct": 0.08,
            "remaining_long_exchange_cap_pct": 0.10,
            "remaining_short_exchange_cap_pct": 0.10,
            "final_single_cap_pct": 0.05,
        }
    )

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(requested_symbols=symbols, snapshots=[], errors=[])

    def fake_build_opportunities(self: ArbitrageScannerService, snapshots: list[MarketSnapshot]):
        return [execution_ready_opportunity]

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    item = response["opportunities"][0]
    assert item["extended_size_up_execution_ready"] is True
    assert item["execution_max_single_cap_pct"] == 0.08
    assert item["final_single_cap_pct"] == 0.05


def test_non_risk_eligible_opportunity_remains_execution_not_ready(monkeypatch) -> None:
    scanner = ArbitrageScannerService()
    base_opportunity = scanner.build_opportunities(
        [
            _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported"),
            _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current"),
        ]
    )[0]
    not_ready_opportunity = base_opportunity.model_copy(
        update={
            "execution_mode": "size_up",
            "extended_size_up_risk_eligible": False,
            "remaining_total_cap_pct": 0.20,
            "remaining_symbol_cap_pct": 0.08,
            "remaining_long_exchange_cap_pct": 0.10,
            "remaining_short_exchange_cap_pct": 0.10,
        }
    )

    async def fake_fetch_snapshots(self: MarketDataService, symbols: list[str]) -> MarketDataResponse:
        return MarketDataResponse(requested_symbols=symbols, snapshots=[], errors=[])

    def fake_build_opportunities(self: ArbitrageScannerService, snapshots: list[MarketSnapshot]):
        return [not_ready_opportunity]

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    item = response["opportunities"][0]
    assert item["extended_size_up_execution_ready"] is False
    assert "extended_size_up_risk_not_eligible" in item["extended_size_up_execution_blockers"]
