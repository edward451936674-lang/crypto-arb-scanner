from html import escape
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import ValidationError

from app.core.config import get_settings
from app.core.symbols import parse_symbols, supported_symbols
from app.models.market import (
    MarketSnapshot,
    OpportunitiesResponse,
    Opportunity,
    ProfileWhyNotExplainability,
    ReplayAssumptions,
    ReplayProfileCompareItem,
    ReplayProfileCompareResponse,
    ReplayProfileComparisonResult,
    ReplayPreviewItem,
    ReplayPreviewResponse,
)
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.data_quality_gate import MarketDataQualityGate
from app.services.execution_sizing_policy import (
    ExecutionAccountState,
    ExecutionSizingDecision,
    ExecutionSizingPolicyEvaluator,
    build_execution_account_inputs,
    build_execution_account_inputs_for_profile,
    resolve_execution_policy_profile_name,
    resolve_execution_policy_profile,
)
from app.services.execution_account_state import (
    ExecutionAccountStateProvider,
    get_execution_account_state_provider,
    resolve_execution_account_state_fixture_scenario,
    resolve_execution_account_state_provider_name,
    resolve_fixed_fixture_remaining_caps,
)
from app.services.market_data import MarketDataService
from app.services.opportunity_replay import OpportunityReplayService
from app.services.telegram_notifier import TelegramNotifier, TelegramNotifierConfig

settings = get_settings()
execution_account_state_provider: ExecutionAccountStateProvider = get_execution_account_state_provider(settings)
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Cross-exchange perpetual market data collector for arbitrage discovery.",
)

PROFILE_POLICY_BLOCKERS = {
    "extended_size_up_not_enabled_in_execution_policy",
    "live_target_leverage_too_high",
    "live_max_allowed_leverage_too_high",
    "live_liquidation_buffer_requirement_not_strict_enough",
}
EXECUTION_CAPACITY_BLOCKERS = {
    "insufficient_live_total_capacity_for_extended_size_up",
    "insufficient_live_symbol_capacity_for_extended_size_up",
    "insufficient_live_long_exchange_capacity_for_extended_size_up",
    "insufficient_live_short_exchange_capacity_for_extended_size_up",
}
OPPORTUNITY_EXECUTION_BLOCKERS = {
    "not_in_size_up_mode",
    "extended_size_up_risk_not_eligible",
}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "env": settings.app_env}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    symbols: str | None = Query(default=None),
    min_edge_bps: float = Query(default=0.0),
    execution_mode: str | None = Query(default=None),
    refresh_seconds: int = Query(default=15, ge=5, le=300),
) -> str:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    opportunities = await _collect_current_opportunities(requested_symbols)
    filtered = [
        item
        for item in opportunities
        if item.net_edge_bps >= min_edge_bps
        and (execution_mode is None or item.execution_mode == execution_mode)
    ]
    options = ["paper", "small_probe", "normal", "size_up", "extended_size_up"]
    rows = "".join(
        (
            "<tr>"
            f"<td>{escape(item.symbol)}</td>"
            f"<td>{escape(item.long_exchange)}</td>"
            f"<td>{escape(item.short_exchange)}</td>"
            f"<td>{item.price_spread_bps:.2f}</td>"
            f"<td>{(item.funding_spread_bps or 0.0):.2f}</td>"
            f"<td>{item.net_edge_bps:.2f}</td>"
            f"<td>{escape(item.opportunity_grade)}</td>"
            f"<td>{escape(item.execution_mode)}</td>"
            f"<td>{item.final_position_pct:.2%}</td>"
            f"<td>{escape(item.data_quality_status)} | {escape(', '.join(item.risk_flags[:2]) or '-')}</td>"
            f"<td>{item.cluster_id or '-'}</td>"
            "</tr>"
        )
        for item in filtered
    )
    select_options = "".join(
        f"<option value='{mode}' {'selected' if mode == execution_mode else ''}>{mode}</option>" for mode in options
    )
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta http-equiv="refresh" content="{refresh_seconds}">
  <title>Crypto Arb Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; color: #222; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background: #f7f7f7; }}
    .filters {{ margin-bottom: 12px; display: flex; gap: 8px; align-items: center; }}
    input, select {{ padding: 4px; }}
  </style>
</head>
<body>
  <h1>Live Opportunities Dashboard</h1>
  <form method="get" class="filters">
    <label>symbol <input name="symbols" value="{escape(','.join(requested_symbols))}" /></label>
    <label>min edge bps <input name="min_edge_bps" type="number" step="0.1" value="{min_edge_bps}" /></label>
    <label>execution mode
      <select name="execution_mode">
        <option value="">all</option>
        {select_options}
      </select>
    </label>
    <label>refresh sec <input name="refresh_seconds" type="number" min="5" max="300" value="{refresh_seconds}" /></label>
    <button type="submit">Apply</button>
  </form>
  <table>
    <thead>
      <tr>
        <th>symbol</th><th>long_exchange</th><th>short_exchange</th><th>price_spread_bps</th>
        <th>funding_spread_bps</th><th>estimated_net_edge_bps</th><th>opportunity_grade</th>
        <th>execution_mode</th><th>final_position_pct</th><th>risk_flags</th><th>replay_summary</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
"""


@app.post("/api/v1/alerts/telegram/opportunities")
async def alert_telegram_opportunities(
    symbols: str | None = Query(default=None),
    min_net_edge_bps: float = Query(default=15.0),
    top_n: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    opportunities = await _collect_current_opportunities(requested_symbols)
    notifier = TelegramNotifier(
        TelegramNotifierConfig(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
        )
    )
    if not notifier.is_configured:
        raise HTTPException(status_code=400, detail="telegram_not_configured")

    evaluated_count = len(opportunities)
    sent_routes: list[str] = []
    skipped: list[dict[str, str]] = []
    deduped: list[Opportunity] = []
    seen_routes: set[str] = set()
    for item in opportunities:
        route = f"{item.symbol}:{item.long_exchange}->{item.short_exchange}"
        if route in seen_routes:
            skipped.append({"route": route, "reason": "duplicate_route"})
            continue
        seen_routes.add(route)
        deduped.append(item)

    filtered: list[Opportunity] = []
    for item in deduped:
        route = f"{item.symbol}:{item.long_exchange}->{item.short_exchange}"
        if item.execution_mode in {"paper", "small_probe"}:
            skipped.append({"route": route, "reason": "non_live_execution_mode"})
            continue
        if item.net_edge_bps < min_net_edge_bps:
            skipped.append({"route": route, "reason": "below_min_net_edge"})
            continue
        if item.data_quality_status not in {"healthy", "degraded"}:
            skipped.append({"route": route, "reason": "poor_data_quality"})
            continue
        filtered.append(item)

    for item in filtered[:top_n]:
        route = f"{item.symbol}:{item.long_exchange}->{item.short_exchange}"
        await notifier.send_text(TelegramNotifier.format_opportunity_alert(item.model_dump()))
        sent_routes.append(route)

    return {
        "evaluated": evaluated_count,
        "sent": len(sent_routes),
        "sent_routes": sent_routes,
        "skipped": skipped,
    }


@app.get("/api/v1/meta")
async def meta() -> dict[str, object]:
    resolved_execution_policy = resolve_execution_policy_profile(settings)
    execution_account_state_provider_name = resolve_execution_account_state_provider_name(settings)
    execution_account_state_fixture_scenario = str(
        getattr(settings, "execution_account_state_fixture_scenario", "roomy")
    ).strip().lower()
    execution_account_state_resolved: dict[str, float] | None = None
    if execution_account_state_provider_name == "fixed_fixture":
        execution_account_state_fixture_scenario = resolve_execution_account_state_fixture_scenario(settings)
        execution_account_state_resolved = resolve_fixed_fixture_remaining_caps(settings)
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
        "execution_account_state_provider": execution_account_state_provider_name,
        "execution_account_state_fixture_scenario": execution_account_state_fixture_scenario,
        "execution_account_state_resolved": execution_account_state_resolved,
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
    try:
        opportunities_response = await _build_opportunities_response(requested_symbols)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = OpportunitiesResponse(
        requested_symbols=opportunities_response.requested_symbols,
        opportunities=opportunities_response.opportunities,
        snapshot_errors=opportunities_response.snapshot_errors,
    )
    return response.model_dump()


@app.get("/api/v1/replay-preview")
async def get_replay_preview(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    ),
    limit: int = Query(default=5, ge=1, le=100),
    holding_mode: Literal["to_next_funding", "fixed_minutes"] = Query(default="to_next_funding"),
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


@app.get("/api/v1/replay-profile-compare")
async def get_replay_profile_compare(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    ),
    limit: int = Query(default=5, ge=1, le=100),
    profiles: str | None = Query(
        default=None,
        description="Comma separated execution policy profiles",
    ),
    holding_mode: Literal["to_next_funding", "fixed_minutes"] = Query(default="to_next_funding"),
    holding_minutes: int | None = Query(default=None, ge=0),
    slippage_bps_per_leg: float = Query(default=1.0, ge=0.0),
    extra_exit_slippage_bps_per_leg: float = Query(default=0.5, ge=0.0),
    latency_decay_bps: float = Query(default=0.2, ge=0.0),
    borrow_or_misc_cost_bps: float = Query(default=0.0, ge=0.0),
    account_remaining_total_cap_pct: float | None = None,
    account_remaining_symbol_cap_pct: float | None = None,
    account_remaining_long_exchange_cap_pct: float | None = None,
    account_remaining_short_exchange_cap_pct: float | None = None,
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
        comparison_profiles = _parse_execution_profiles(profiles)
        market_data = await market_data_service.fetch_snapshots(requested_symbols)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    quality_result = quality_gate.evaluate(market_data.snapshots)
    accepted_snapshots = quality_result.accepted_snapshots
    opportunities = scanner.build_opportunities(accepted_snapshots)
    snapshot_lookup = _snapshot_lookup(accepted_snapshots)
    items: list[ReplayProfileCompareItem] = []
    for opportunity in opportunities[:limit]:
        long_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.long_exchange.lower()))
        short_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.short_exchange.lower()))
        if long_snapshot is None or short_snapshot is None:
            continue

        replay = replay_service.replay(opportunity, long_snapshot, short_snapshot, assumptions)
        debug_account_state = _build_debug_execution_account_state_for_opportunity(
            opportunity=opportunity,
            account_remaining_total_cap_pct=account_remaining_total_cap_pct,
            account_remaining_symbol_cap_pct=account_remaining_symbol_cap_pct,
            account_remaining_long_exchange_cap_pct=account_remaining_long_exchange_cap_pct,
            account_remaining_short_exchange_cap_pct=account_remaining_short_exchange_cap_pct,
        )
        profile_results: list[ReplayProfileComparisonResult] = []
        for profile_name in comparison_profiles:
            resolved_execution_policy = resolve_execution_policy_profile_name(settings, profile_name)
            decision = ExecutionSizingPolicyEvaluator.evaluate(
                opportunity=opportunity,
                account_inputs=build_execution_account_inputs_for_profile(
                    settings,
                    opportunity,
                    profile_name,
                    account_state=debug_account_state,
                ),
            )
            profile_results.append(
                ReplayProfileComparisonResult(
                    profile_name=profile_name,
                    resolved_execution_extended_size_up_enabled=resolved_execution_policy.extended_size_up_enabled,
                    resolved_execution_target_leverage=resolved_execution_policy.live_target_leverage,
                    resolved_execution_max_allowed_leverage=resolved_execution_policy.live_max_allowed_leverage,
                    resolved_execution_required_liquidation_buffer_pct=(
                        resolved_execution_policy.live_required_liquidation_buffer_pct
                    ),
                    extended_size_up_execution_ready=decision.extended_size_up_execution_ready,
                    extended_size_up_execution_blockers=decision.extended_size_up_execution_blockers,
                    why_not_explainability=_build_profile_why_not_explainability(opportunity, decision),
                    execution_max_single_cap_pct=decision.execution_max_single_cap_pct,
                    execution_cap_reasons=decision.execution_cap_reasons,
                    replay=replay,
                )
            )

        items.append(
            ReplayProfileCompareItem(
                cluster_id=opportunity.cluster_id,
                route_rank=opportunity.route_rank,
                symbol=opportunity.symbol,
                long_exchange=opportunity.long_exchange,
                short_exchange=opportunity.short_exchange,
                execution_mode=opportunity.execution_mode,
                opportunity_grade=opportunity.opportunity_grade,
                normal_blockers=opportunity.normal_blockers,
                normal_promotion_reasons=opportunity.normal_promotion_reasons,
                size_up_blockers=opportunity.size_up_blockers,
                size_up_promotion_reasons=opportunity.size_up_promotion_reasons,
                extended_size_up_risk_eligible=opportunity.extended_size_up_risk_eligible,
                extended_size_up_risk_blockers=opportunity.extended_size_up_risk_blockers,
                profile_results=profile_results,
            )
        )

    response = ReplayProfileCompareResponse(
        requested_symbols=market_data.requested_symbols,
        replay_assumptions=assumptions,
        compared_profiles=comparison_profiles,
        compare_count=len(items),
        account_state_applied=_debug_account_state_requested(
            account_remaining_total_cap_pct=account_remaining_total_cap_pct,
            account_remaining_symbol_cap_pct=account_remaining_symbol_cap_pct,
            account_remaining_long_exchange_cap_pct=account_remaining_long_exchange_cap_pct,
            account_remaining_short_exchange_cap_pct=account_remaining_short_exchange_cap_pct,
        ),
        items=items,
        snapshot_errors=market_data.errors,
    )
    return response.model_dump()


def _hydrate_execution_sizing_outputs(
    opportunities: list[Opportunity],
    *,
    active_execution_policy_profile: str,
    account_state_provider: ExecutionAccountStateProvider | None = None,
) -> list[Opportunity]:
    resolved_execution_policy = resolve_execution_policy_profile(settings)
    provider = account_state_provider or execution_account_state_provider
    hydrated = []
    for opportunity in opportunities:
        account_state = provider.get_account_state(opportunity)
        decision = ExecutionSizingPolicyEvaluator.evaluate(
            opportunity=opportunity,
            account_inputs=build_execution_account_inputs(settings, opportunity, account_state=account_state),
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


async def _build_opportunities_response(requested_symbols: list[str]) -> OpportunitiesResponse:
    market_data_service = MarketDataService(settings)
    scanner = ArbitrageScannerService()
    quality_gate = MarketDataQualityGate()

    market_data = await market_data_service.fetch_snapshots(requested_symbols)
    quality_result = quality_gate.evaluate(market_data.snapshots)
    accepted_snapshots = quality_result.accepted_snapshots
    opportunities = _hydrate_execution_sizing_outputs(
        scanner.build_opportunities(accepted_snapshots),
        active_execution_policy_profile=settings.execution_policy_profile,
    )
    return OpportunitiesResponse(
        requested_symbols=market_data.requested_symbols,
        opportunities=opportunities,
        snapshot_errors=market_data.errors,
    )


async def _collect_current_opportunities(requested_symbols: list[str]) -> list[Opportunity]:
    response = await _build_opportunities_response(requested_symbols)
    return response.opportunities


def _snapshot_lookup(snapshots: list[MarketSnapshot]) -> dict[tuple[str, str], MarketSnapshot]:
    lookup: dict[tuple[str, str], MarketSnapshot] = {}
    for snapshot in snapshots:
        lookup[(snapshot.base_symbol.upper(), snapshot.exchange.lower())] = snapshot
    return lookup


def _parse_execution_profiles(profiles: str | None) -> list[str]:
    default_profiles = ["dev_default", "paper_conservative", "live_conservative"]
    if not profiles:
        return default_profiles

    parsed = [item.strip().lower() for item in profiles.split(",") if item.strip()]
    deduped = list(dict.fromkeys(parsed))
    if not deduped:
        raise ValueError("profiles must include at least one execution policy profile")

    for profile_name in deduped:
        resolve_execution_policy_profile_name(settings, profile_name)
    return deduped


def _debug_account_state_requested(
    *,
    account_remaining_total_cap_pct: float | None,
    account_remaining_symbol_cap_pct: float | None,
    account_remaining_long_exchange_cap_pct: float | None,
    account_remaining_short_exchange_cap_pct: float | None,
) -> bool:
    return (
        account_remaining_total_cap_pct is not None
        or account_remaining_symbol_cap_pct is not None
        or account_remaining_long_exchange_cap_pct is not None
        or account_remaining_short_exchange_cap_pct is not None
    )


def _build_debug_execution_account_state_for_opportunity(
    *,
    opportunity: Opportunity,
    account_remaining_total_cap_pct: float | None,
    account_remaining_symbol_cap_pct: float | None,
    account_remaining_long_exchange_cap_pct: float | None,
    account_remaining_short_exchange_cap_pct: float | None,
) -> ExecutionAccountState | None:
    if not _debug_account_state_requested(
        account_remaining_total_cap_pct=account_remaining_total_cap_pct,
        account_remaining_symbol_cap_pct=account_remaining_symbol_cap_pct,
        account_remaining_long_exchange_cap_pct=account_remaining_long_exchange_cap_pct,
        account_remaining_short_exchange_cap_pct=account_remaining_short_exchange_cap_pct,
    ):
        return None

    return ExecutionAccountState(
        remaining_total_cap_pct=account_remaining_total_cap_pct,
        remaining_symbol_cap_pct_by_symbol=(
            {opportunity.symbol: account_remaining_symbol_cap_pct}
            if account_remaining_symbol_cap_pct is not None
            else {}
        ),
        remaining_long_exchange_cap_pct_by_exchange=(
            {opportunity.long_exchange: account_remaining_long_exchange_cap_pct}
            if account_remaining_long_exchange_cap_pct is not None
            else {}
        ),
        remaining_short_exchange_cap_pct_by_exchange=(
            {opportunity.short_exchange: account_remaining_short_exchange_cap_pct}
            if account_remaining_short_exchange_cap_pct is not None
            else {}
        ),
    )


def _build_profile_why_not_explainability(
    opportunity: Opportunity,
    decision: ExecutionSizingDecision,
) -> ProfileWhyNotExplainability:
    decision_blockers = decision.extended_size_up_execution_blockers
    opportunity_blockers = list(
        dict.fromkeys(
            [
                blocker
                for blocker in decision_blockers
                if blocker in OPPORTUNITY_EXECUTION_BLOCKERS
            ]
            + opportunity.size_up_blockers
            + opportunity.extended_size_up_risk_blockers
        )
    )
    profile_policy_blockers = [
        blocker for blocker in decision_blockers if blocker in PROFILE_POLICY_BLOCKERS
    ]
    execution_capacity_blockers = [
        blocker for blocker in decision_blockers if blocker in EXECUTION_CAPACITY_BLOCKERS
    ]

    return ProfileWhyNotExplainability(
        opportunity_blockers=opportunity_blockers,
        profile_policy_blockers=profile_policy_blockers,
        execution_capacity_blockers=execution_capacity_blockers,
    )
