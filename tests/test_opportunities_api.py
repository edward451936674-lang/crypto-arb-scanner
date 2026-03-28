import asyncio
import time

from app.main import get_opportunities
from app.models.market import MarketDataResponse, MarketSnapshot
from app.services.arbitrage_scanner import ArbitrageScannerService
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
    assert item["data_quality_status"] == "healthy"
    assert item["data_quality_penalty_multiplier"] == 1.0
    assert "both_legs_healthy" in item["data_quality_drivers"]
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


def test_degraded_quality_propagates_to_opportunity_risk_and_sizing() -> None:
    scanner = ArbitrageScannerService()
    now_ms = int(time.time() * 1000)

    healthy_left = _snapshot("binance", 100.0, funding_rate=-0.001, funding_rate_source="latest_reported")
    healthy_right = _snapshot("okx", 101.0, funding_rate=0.001, funding_rate_source="current")
    healthy_left.timestamp_ms = now_ms
    healthy_right.timestamp_ms = now_ms

    degraded_left = healthy_left.model_copy(
        update={
            "timestamp_ms": now_ms - 130_000,
            "data_quality_status": "degraded",
            "data_quality_score": 0.8,
            "data_quality_flags": ["timestamp_stale", "cross_exchange_price_outlier"],
        }
    )
    healthy_right_with_quality = healthy_right.model_copy(
        update={
            "data_quality_status": "healthy",
            "data_quality_score": 1.0,
            "data_quality_flags": [],
        }
    )

    healthy_opportunity = scanner.build_opportunities(
        [
            healthy_left.model_copy(
                update={
                    "data_quality_status": "healthy",
                    "data_quality_score": 1.0,
                    "data_quality_flags": [],
                }
            ),
            healthy_right_with_quality,
        ]
    )[0]
    degraded_opportunity = scanner.build_opportunities([degraded_left, healthy_right_with_quality])[0]

    assert healthy_opportunity.data_quality_status == "healthy"
    assert degraded_opportunity.data_quality_status == "degraded"
    assert degraded_opportunity.data_quality_score == 0.8
    assert "timestamp_stale" in degraded_opportunity.data_quality_flags
    assert "one_leg_degraded" in degraded_opportunity.data_quality_drivers
    assert "degraded_cross_exchange_price_signal" in degraded_opportunity.data_quality_drivers
    assert "degraded_data_quality" in degraded_opportunity.risk_flags
    assert "cross_exchange_price_quality_risk" in degraded_opportunity.risk_flags
    assert degraded_opportunity.data_quality_penalty_multiplier == 0.85
    assert degraded_opportunity.data_quality_adjusted_edge_bps == (
        degraded_opportunity.risk_adjusted_edge_bps * degraded_opportunity.data_quality_penalty_multiplier
    )
    assert degraded_opportunity.suggested_position_pct < healthy_opportunity.suggested_position_pct
    assert degraded_opportunity.execution_mode != "size_up"
    assert degraded_opportunity.size_up_eligible is False


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
    item = response["opportunities"][0]
    assert item["data_quality_status"] == "degraded"
    assert "degraded_data_quality" in item["risk_flags"]
    assert item["execution_mode"] != "size_up"


def test_get_opportunities_mixed_collection_uses_only_gate_accepted(monkeypatch) -> None:
    now_ms = int(time.time() * 1000)
    captured_scanner_input: list[MarketSnapshot] = []

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

    def fake_build_opportunities(
        self: ArbitrageScannerService,
        snapshots: list[MarketSnapshot],
    ) -> list[object]:
        captured_scanner_input.extend(snapshots)
        return []

    monkeypatch.setattr(MarketDataService, "fetch_snapshots", fake_fetch_snapshots)
    monkeypatch.setattr(ArbitrageScannerService, "build_opportunities", fake_build_opportunities)

    response = asyncio.run(get_opportunities(symbols="BTC"))
    assert response["opportunities"] == []
    assert len(captured_scanner_input) == 2

    exchanges = {snapshot.exchange for snapshot in captured_scanner_input}
    assert exchanges == {"binance", "okx"}

    statuses = {snapshot.exchange: snapshot.data_quality_status for snapshot in captured_scanner_input}
    assert statuses["binance"] == "healthy"
    assert statuses["okx"] == "degraded"
    assert "hyperliquid" not in exchanges
    assert "lighter" not in exchanges
