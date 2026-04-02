from fastapi import FastAPI, HTTPException, Query
from pydantic import ValidationError

from app.core.config import get_settings
from app.core.symbols import parse_symbols, supported_symbols
from app.models.market import (
    MarketSnapshot,
    OpportunitiesResponse,
    Opportunity,
    ReplayAssumptions,
    ReplayPreviewItem,
    ReplayPreviewResponse,
)
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.data_quality_gate import MarketDataQualityGate
from app.services.execution_sizing_policy import (
    ExecutionSizingPolicyEvaluator,
    build_execution_account_inputs,
    resolve_execution_policy_profile,
)
from app.services.market_data import MarketDataService
from app.services.opportunity_replay import OpportunityReplayService

settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Cross-exchange perpetual market data collector for arbitrage discovery.",
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "env": settings.app_env}


@app.get("/api/v1/meta")
async def meta() -> dict[str, object]:
    resolved_execution_policy = resolve_execution_policy_profile(settings)
    return {
        "supported_symbols": supported_symbols(),
        "enabled_exchanges": {
            "binance": settings.enable_binance,
            "okx": settings.enable_okx,
            "hyperliquid": settings.enable_hyperliquid,
            "lighter": settings.enable_lighter,
        },
        "default_symbols": settings.default_symbols,
        "execution_policy_profile": settings.execution_policy_profile,
        "execution_policy_resolved": {
            "extended_size_up_enabled": resolved_execution_policy.extended_size_up_enabled,
            "live_target_leverage": resolved_execution_policy.live_target_leverage,
            "live_max_allowed_leverage": resolved_execution_policy.live_max_allowed_leverage,
            "live_required_liquidation_buffer_pct": resolved_execution_policy.live_required_liquidation_buffer_pct,
            "live_remaining_total_cap_pct": resolved_execution_policy.live_remaining_total_cap_pct,
            "live_remaining_symbol_cap_pct": resolved_execution_policy.live_remaining_symbol_cap_pct,
            "live_remaining_long_exchange_cap_pct": resolved_execution_policy.live_remaining_long_exchange_cap_pct,
            "live_remaining_short_exchange_cap_pct": resolved_execution_policy.live_remaining_short_exchange_cap_pct,
        },
    }


@app.get("/api/v1/snapshots")
async def get_snapshots(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    )
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    service = MarketDataService(settings)

    try:
        result = await service.fetch_snapshots(requested_symbols)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result.model_dump()


@app.get("/api/v1/opportunities")
async def get_opportunities(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    )
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    market_data_service = MarketDataService(settings)
    scanner = ArbitrageScannerService()
    quality_gate = MarketDataQualityGate()

    try:
        market_data = await market_data_service.fetch_snapshots(requested_symbols)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    quality_result = quality_gate.evaluate(market_data.snapshots)
    accepted_snapshots = quality_result.accepted_snapshots

    response = OpportunitiesResponse(
        requested_symbols=market_data.requested_symbols,
        opportunities=_hydrate_execution_sizing_outputs(
            scanner.build_opportunities(accepted_snapshots),
            active_execution_policy_profile=settings.execution_policy_profile,
        ),
        snapshot_errors=market_data.errors,
    )
    return response.model_dump()


@app.get("/api/v1/replay-preview")
async def get_replay_preview(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    ),
    limit: int = Query(default=5, ge=1, le=100),
    holding_mode: str = Query(default="to_next_funding"),
    holding_minutes: int | None = Query(default=None, ge=0),
    slippage_bps_per_leg: float = Query(default=1.0, ge=0.0),
    extra_exit_slippage_bps_per_leg: float = Query(default=0.5, ge=0.0),
    latency_decay_bps: float = Query(default=0.2, ge=0.0),
    borrow_or_misc_cost_bps: float = Query(default=0.0, ge=0.0),
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    market_data_service = MarketDataService(settings)
    scanner = ArbitrageScannerService()
    quality_gate = MarketDataQualityGate()
    replay_service = OpportunityReplayService()

    try:
        assumptions = ReplayAssumptions(
            holding_mode=holding_mode,
            holding_minutes=holding_minutes,
            slippage_bps_per_leg=slippage_bps_per_leg,
            extra_exit_slippage_bps_per_leg=extra_exit_slippage_bps_per_leg,
            latency_decay_bps=latency_decay_bps,
            borrow_or_misc_cost_bps=borrow_or_misc_cost_bps,
        )
        market_data = await market_data_service.fetch_snapshots(requested_symbols)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    quality_result = quality_gate.evaluate(market_data.snapshots)
    accepted_snapshots = quality_result.accepted_snapshots
    opportunities = scanner.build_opportunities(accepted_snapshots)

    snapshot_lookup = _snapshot_lookup(accepted_snapshots)
    preview_items: list[ReplayPreviewItem] = []
    for opportunity in opportunities[:limit]:
        long_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.long_exchange.lower()))
        short_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.short_exchange.lower()))
        if long_snapshot is None or short_snapshot is None:
            continue
        preview_items.append(
            ReplayPreviewItem(
                cluster_id=opportunity.cluster_id,
                route_rank=opportunity.route_rank,
                symbol=opportunity.symbol,
                long_exchange=opportunity.long_exchange,
                short_exchange=opportunity.short_exchange,
                execution_mode=opportunity.execution_mode,
                opportunity_grade=opportunity.opportunity_grade,
                replay=replay_service.replay(opportunity, long_snapshot, short_snapshot, assumptions),
            )
        )

    response = ReplayPreviewResponse(
        requested_symbols=market_data.requested_symbols,
        replay_assumptions=assumptions,
        preview_count=len(preview_items),
        items=preview_items,
        snapshot_errors=market_data.errors,
    )
    return response.model_dump()


def _hydrate_execution_sizing_outputs(
    opportunities: list[Opportunity],
    *,
    active_execution_policy_profile: str,
) -> list[Opportunity]:
    resolved_execution_policy = resolve_execution_policy_profile(settings)
    hydrated = []
    for opportunity in opportunities:
        decision = ExecutionSizingPolicyEvaluator.evaluate(
            opportunity=opportunity,
            account_inputs=build_execution_account_inputs(settings, opportunity),
        )
        hydrated.append(
            opportunity.model_copy(
                update={
                    "extended_size_up_execution_ready": decision.extended_size_up_execution_ready,
                    "extended_size_up_execution_blockers": decision.extended_size_up_execution_blockers,
                    "execution_max_single_cap_pct": decision.execution_max_single_cap_pct,
                    "execution_cap_reasons": decision.execution_cap_reasons,
                    "active_execution_policy_profile": active_execution_policy_profile,
                    "resolved_execution_extended_size_up_enabled": resolved_execution_policy.extended_size_up_enabled,
                    "resolved_execution_target_leverage": resolved_execution_policy.live_target_leverage,
                    "resolved_execution_max_allowed_leverage": resolved_execution_policy.live_max_allowed_leverage,
                    "resolved_execution_required_liquidation_buffer_pct": (
                        resolved_execution_policy.live_required_liquidation_buffer_pct
                    ),
                }
            )
        )
    return hydrated


def _snapshot_lookup(snapshots: list[MarketSnapshot]) -> dict[tuple[str, str], MarketSnapshot]:
    lookup: dict[tuple[str, str], MarketSnapshot] = {}
    for snapshot in snapshots:
        lookup[(snapshot.base_symbol.upper(), snapshot.exchange.lower())] = snapshot
    return lookup
