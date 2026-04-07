from html import escape
from dataclasses import dataclass
import hashlib
import time
from datetime import datetime, timezone
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
from app.services.opportunity_observer import OpportunityObservationContext, OpportunityObserverService
from app.services.opportunity_replay import OpportunityReplayService
from app.storage.observations import ObservationStore
from app.services.telegram_notifier import TelegramNotifier, TelegramNotifierConfig
from app.services.alert_memory import AlertCandidate, AlertMemoryService

settings = get_settings()
execution_account_state_provider: ExecutionAccountStateProvider = get_execution_account_state_provider(settings)
observation_store = ObservationStore(settings.observations_db_path)
opportunity_observer = OpportunityObserverService()
alert_memory = AlertMemoryService()
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


@dataclass(frozen=True)
class DashboardRow:
    opportunity: Opportunity
    why_not_tradable: str
    replay_net_after_cost_bps: float | None
    replay_confidence_label: str | None
    replay_passes_min_trade_gate: bool | None
    history_hint: str


@dataclass(frozen=True)
class _ScanContext:
    requested_symbols: list[str]
    opportunities: list[Opportunity]
    snapshot_errors: list[object]
    accepted_snapshots: list[MarketSnapshot]


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
    dashboard_rows = await _build_dashboard_rows(requested_symbols)
    recent_observations = observation_store.latest(limit=20)
    recent_alert_events = observation_store.latest_alert_events(limit=20)
    filtered = [
        item
        for item in dashboard_rows
        if item.opportunity.net_edge_bps >= min_edge_bps
        and (execution_mode is None or item.opportunity.execution_mode == execution_mode)
    ]
    options = ["paper", "small_probe", "normal", "size_up", "extended_size_up"]
    rows = "".join(_render_dashboard_row(item) for item in filtered)
    recent_observations_rows = "".join(_render_recent_observation_row(item) for item in recent_observations)
    recent_alert_rows = "".join(_render_recent_alert_row(item) for item in recent_alert_events)
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
    h2 {{ margin-top: 26px; }}
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
        <th>execution_mode</th><th>final_position_pct</th><th>why_not_tradable</th><th>risk_flags</th>
        <th>replay_net_after_cost_bps</th><th>replay_confidence_label</th><th>replay_passes_min_trade_gate</th><th>replay_summary</th><th>history_hint</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <h2>Recent Observations</h2>
  <table>
    <thead>
      <tr>
        <th>observed_at</th><th>symbol</th><th>long_exchange</th><th>short_exchange</th><th>estimated_net_edge_bps</th>
        <th>execution_mode</th><th>why_not_tradable</th><th>replay_net_after_cost_bps</th><th>replay_confidence_label</th>
      </tr>
    </thead>
    <tbody>{recent_observations_rows or "<tr><td colspan='9'>No observations recorded yet.</td></tr>"}</tbody>
  </table>
  <h2>Recent Alert Events</h2>
  <table>
    <thead>
      <tr>
        <th>sent_at</th><th>symbol</th><th>long_exchange</th><th>short_exchange</th><th>execution_mode</th>
        <th>final_position_pct</th><th>replay_net_after_cost_bps</th>
      </tr>
    </thead>
    <tbody>{recent_alert_rows or "<tr><td colspan='7'>No alert events sent yet.</td></tr>"}</tbody>
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
    scan_context = await _build_scan_context(requested_symbols)
    opportunities = scan_context.opportunities
    contexts = opportunity_observer.build_observation_contexts(opportunities, scan_context.accepted_snapshots)
    notifier = TelegramNotifier(
        TelegramNotifierConfig(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
        )
    )
    if not notifier.is_configured:
        raise HTTPException(status_code=400, detail="telegram_not_configured")

    now_ms = int(time.time() * 1000)
    evaluated_count = len(opportunities)
    eligible_count = 0
    sent_routes: list[str] = []
    skipped_routes: list[dict[str, str]] = []
    skipped_due_to_dedupe_count = 0
    filtered: list[tuple[Opportunity, OpportunityObservationContext]] = []
    seen_identities: set[str] = set()
    for item, context in zip(opportunities, contexts, strict=False):
        dedupe_identity, route_key = alert_memory.dedupe_identity_for(
            symbol=item.symbol,
            long_exchange=item.long_exchange,
            short_exchange=item.short_exchange,
            cluster_id=item.cluster_id,
        )
        if dedupe_identity in seen_identities:
            skipped_routes.append({"route": route_key, "reason": "duplicate_route_in_run"})
            continue
        seen_identities.add(dedupe_identity)
        filtered.append((item, context))

    candidates: list[tuple[Opportunity, OpportunityObservationContext]] = []
    for item, context in filtered:
        route_key = alert_memory.route_key_for(
            symbol=item.symbol,
            long_exchange=item.long_exchange,
            short_exchange=item.short_exchange,
        )
        if _is_low_signal_alert_candidate(item, context):
            skipped_routes.append({"route": route_key, "reason": "low_signal"})
            continue
        if item.net_edge_bps < min_net_edge_bps:
            skipped_routes.append({"route": route_key, "reason": "below_min_net_edge"})
            continue
        if item.data_quality_status not in {"healthy", "degraded"}:
            skipped_routes.append({"route": route_key, "reason": "poor_data_quality"})
            continue
        candidates.append((item, context))

    eligible_count = len(candidates)
    send_failed_count = 0

    for item, context in candidates:
        if len(sent_routes) >= top_n:
            break
        dedupe_identity, route_key = alert_memory.dedupe_identity_for(
            symbol=item.symbol,
            long_exchange=item.long_exchange,
            short_exchange=item.short_exchange,
            cluster_id=item.cluster_id,
        )
        candidate = AlertCandidate(
            dedupe_identity=dedupe_identity,
            cluster_id=item.cluster_id,
            route_key=route_key,
            symbol=item.symbol,
            long_exchange=item.long_exchange,
            short_exchange=item.short_exchange,
            execution_mode=item.execution_mode,
            final_position_pct=item.final_position_pct,
            replay_net_after_cost_bps=context.replay_net_after_cost_bps,
            replay_passes_min_trade_gate=context.replay_passes_min_trade_gate,
        )
        previous_event = observation_store.latest_alert_event(candidate.dedupe_identity)
        decision = alert_memory.evaluate(candidate=candidate, previous_event=previous_event, now_ms=now_ms)
        if not decision.should_send:
            skipped_due_to_dedupe_count += 1
            skipped_routes.append({"route": route_key, "reason": decision.reason})
            continue

        message = TelegramNotifier.format_opportunity_alert(item.model_dump())
        try:
            sent_ok = await notifier.send_text(message)
        except Exception:
            sent_ok = False
        if not sent_ok:
            send_failed_count += 1
            skipped_routes.append({"route": route_key, "reason": "send_failed"})
            continue

        message_hash = hashlib.sha256(message.encode("utf-8")).hexdigest()
        observation_store.insert_alert_event(
            sent_at_ms=now_ms,
            dedupe_identity=candidate.dedupe_identity,
            cluster_id=candidate.cluster_id,
            route_key=candidate.route_key,
            symbol=candidate.symbol,
            long_exchange=candidate.long_exchange,
            short_exchange=candidate.short_exchange,
            execution_mode=candidate.execution_mode,
            final_position_pct=candidate.final_position_pct,
            replay_net_after_cost_bps=candidate.replay_net_after_cost_bps,
            replay_passes_min_trade_gate=candidate.replay_passes_min_trade_gate,
            message_hash=message_hash,
        )
        sent_routes.append(route_key)

    return {
        "evaluated_count": evaluated_count,
        "eligible_count": eligible_count,
        "sent_count": len(sent_routes),
        "send_failed_count": send_failed_count,
        "skipped_count": len(skipped_routes),
        "skipped_due_to_dedupe_count": skipped_due_to_dedupe_count,
        "skipped_due_to_low_signal_count": sum(1 for item in skipped_routes if item["reason"] == "low_signal"),
        "sent_routes": sent_routes,
        "skipped_routes": skipped_routes,
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


@app.post("/api/v1/observe/run")
async def run_observation_collection(
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    scan_context = await _build_scan_context(requested_symbols)
    contexts = opportunity_observer.build_observation_contexts(scan_context.opportunities, scan_context.accepted_snapshots)
    selected = opportunity_observer.select_top_opportunities(contexts)
    observed_at_ms = int(time.time() * 1000)
    records = opportunity_observer.to_observation_records(selected, observed_at_ms)
    observation_store.insert_many(records)
    summary = opportunity_observer.build_summary(evaluated_count=len(scan_context.opportunities), records=records)
    return summary.model_dump()


@app.get("/api/v1/observe/latest")
async def get_latest_observations(limit: int = Query(default=20, ge=1, le=500)) -> dict[str, object]:
    records = observation_store.latest(limit=limit)
    return {"count": len(records), "items": [item.model_dump() for item in records]}


@app.get("/api/v1/observe/history")
async def get_observation_history(
    symbol: str = Query(..., min_length=1, description="Base symbol, e.g. BTC"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, object]:
    records = observation_store.history(symbol=symbol, limit=limit)
    return {"symbol": symbol.upper(), "count": len(records), "items": [item.model_dump() for item in records]}


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
    scan_context = await _build_scan_context(requested_symbols)
    return OpportunitiesResponse(
        requested_symbols=scan_context.requested_symbols,
        opportunities=scan_context.opportunities,
        snapshot_errors=scan_context.snapshot_errors,
    )


async def _build_scan_context(requested_symbols: list[str]) -> _ScanContext:
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
    return _ScanContext(
        requested_symbols=market_data.requested_symbols,
        opportunities=opportunities,
        snapshot_errors=market_data.errors,
        accepted_snapshots=accepted_snapshots,
    )


async def _collect_current_opportunities(requested_symbols: list[str]) -> list[Opportunity]:
    response = await _build_opportunities_response(requested_symbols)
    return response.opportunities


def _is_low_signal_alert_candidate(opportunity: Opportunity, context: OpportunityObservationContext) -> bool:
    replay_net = context.replay_net_after_cost_bps if context.replay_net_after_cost_bps is not None else -999.0
    if opportunity.execution_mode == "paper":
        if opportunity.opportunity_grade != "watchlist":
            return True
        if opportunity.net_edge_bps < 10.0 and replay_net < 6.0:
            return True
        if not context.replay_passes_min_trade_gate and opportunity.opportunity_grade == "watchlist":
            return True
    if opportunity.execution_mode == "small_probe" and opportunity.net_edge_bps < 10.0 and replay_net < 6.0:
        return True
    if opportunity.opportunity_grade == "watchlist" and replay_net < 4.0:
        return True
    return False


async def _build_dashboard_rows(requested_symbols: list[str]) -> list[DashboardRow]:
    scan_context = await _build_scan_context(requested_symbols)
    recent_observations = observation_store.latest(limit=300)
    route_history = _build_route_history_lookup(recent_observations)
    snapshot_lookup = _snapshot_lookup(scan_context.accepted_snapshots)
    assumptions = ReplayAssumptions(
        holding_mode="to_next_funding",
        slippage_bps_per_leg=1.0,
        extra_exit_slippage_bps_per_leg=0.5,
        latency_decay_bps=0.2,
        borrow_or_misc_cost_bps=0.0,
    )
    replay_service = OpportunityReplayService()

    rows: list[DashboardRow] = []
    for opportunity in scan_context.opportunities:
        long_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.long_exchange.lower()))
        short_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.short_exchange.lower()))
        replay = (
            replay_service.replay(opportunity, long_snapshot, short_snapshot, assumptions)
            if long_snapshot is not None and short_snapshot is not None
            else None
        )
        replay_passes_min_trade_gate = _replay_passes_min_trade_gate(opportunity, replay.net_realized_edge_bps) if replay else None
        rows.append(
            DashboardRow(
                opportunity=opportunity,
                why_not_tradable=_why_not_tradable_label(
                    opportunity=opportunity,
                    replay_net_after_cost_bps=(replay.net_realized_edge_bps if replay else None),
                    replay_passes_min_trade_gate=replay_passes_min_trade_gate,
                ),
                replay_net_after_cost_bps=(replay.net_realized_edge_bps if replay else None),
                replay_confidence_label=(replay.replay_confidence_label if replay else None),
                replay_passes_min_trade_gate=replay_passes_min_trade_gate,
                history_hint=_build_history_hint(opportunity, route_history),
            )
        )
    return rows


def _render_dashboard_row(item: DashboardRow) -> str:
    replay_net_after_cost_display = (
        f"{item.replay_net_after_cost_bps:.2f}" if item.replay_net_after_cost_bps is not None else "N/A"
    )
    replay_passes_gate_display = (
        "yes"
        if item.replay_passes_min_trade_gate is True
        else "no" if item.replay_passes_min_trade_gate is False else "N/A"
    )
    return (
        "<tr>"
        f"<td>{escape(item.opportunity.symbol)}</td>"
        f"<td>{escape(item.opportunity.long_exchange)}</td>"
        f"<td>{escape(item.opportunity.short_exchange)}</td>"
        f"<td>{item.opportunity.price_spread_bps:.2f}</td>"
        f"<td>{(item.opportunity.funding_spread_bps or 0.0):.2f}</td>"
        f"<td>{item.opportunity.net_edge_bps:.2f}</td>"
        f"<td>{escape(item.opportunity.opportunity_grade)}</td>"
        f"<td>{escape(item.opportunity.execution_mode)}</td>"
        f"<td>{item.opportunity.final_position_pct:.2%}</td>"
        f"<td>{escape(item.why_not_tradable)}</td>"
        f"<td>{escape(item.opportunity.data_quality_status)} | "
        f"{escape(', '.join(item.opportunity.risk_flags[:2]) or '-')}</td>"
        f"<td>{replay_net_after_cost_display}</td>"
        f"<td>{escape(item.replay_confidence_label or 'N/A')}</td>"
        f"<td>{replay_passes_gate_display}</td>"
        f"<td>{item.opportunity.cluster_id or '-'}</td>"
        f"<td>{escape(item.history_hint)}</td>"
        "</tr>"
    )


def _render_recent_observation_row(record: object) -> str:
    observed_at = _format_ms_timestamp(getattr(record, "observed_at_ms", None))
    estimated_net_edge_bps = getattr(record, "estimated_net_edge_bps", None)
    replay_net = getattr(record, "replay_net_after_cost_bps", None)
    return (
        "<tr>"
        f"<td>{observed_at}</td>"
        f"<td>{escape(getattr(record, 'symbol', '') or '-')}</td>"
        f"<td>{escape(getattr(record, 'long_exchange', '') or '-')}</td>"
        f"<td>{escape(getattr(record, 'short_exchange', '') or '-')}</td>"
        f"<td>{f'{estimated_net_edge_bps:.2f}' if isinstance(estimated_net_edge_bps, int | float) else 'N/A'}</td>"
        f"<td>{escape(getattr(record, 'execution_mode', '') or '-')}</td>"
        f"<td>{escape(getattr(record, 'why_not_tradable', '') or '-')}</td>"
        f"<td>{f'{replay_net:.2f}' if isinstance(replay_net, int | float) else 'N/A'}</td>"
        f"<td>{escape(getattr(record, 'replay_confidence_label', '') or '-')}</td>"
        "</tr>"
    )


def _render_recent_alert_row(event: dict[str, object]) -> str:
    final_position = event.get("final_position_pct")
    replay_net = event.get("replay_net_after_cost_bps")
    return (
        "<tr>"
        f"<td>{_format_ms_timestamp(event.get('sent_at_ms'))}</td>"
        f"<td>{escape(str(event.get('symbol') or '-'))}</td>"
        f"<td>{escape(str(event.get('long_exchange') or '-'))}</td>"
        f"<td>{escape(str(event.get('short_exchange') or '-'))}</td>"
        f"<td>{escape(str(event.get('execution_mode') or '-'))}</td>"
        f"<td>{f'{float(final_position):.2%}' if isinstance(final_position, int | float) else 'N/A'}</td>"
        f"<td>{f'{float(replay_net):.2f}' if isinstance(replay_net, int | float) else 'N/A'}</td>"
        "</tr>"
    )


def _format_ms_timestamp(timestamp_ms: object) -> str:
    if not isinstance(timestamp_ms, int | float):
        return "N/A"
    return datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _route_identity(symbol: str, long_exchange: str, short_exchange: str, cluster_id: str | None) -> str:
    if cluster_id:
        return f"cluster:{cluster_id}"
    return f"route:{symbol.upper()}:{long_exchange.lower()}->{short_exchange.lower()}"


def _route_key_identity(symbol: str, long_exchange: str, short_exchange: str) -> str:
    return f"route:{symbol.upper()}:{long_exchange.lower()}->{short_exchange.lower()}"


def _build_route_history_lookup(records: list[object]) -> dict[str, list[object]]:
    history: dict[str, list[object]] = {}
    for record in records:
        cluster_key = _route_identity(
            symbol=getattr(record, "symbol", ""),
            long_exchange=getattr(record, "long_exchange", ""),
            short_exchange=getattr(record, "short_exchange", ""),
            cluster_id=getattr(record, "cluster_id", None),
        )
        route_key = _route_key_identity(
            symbol=getattr(record, "symbol", ""),
            long_exchange=getattr(record, "long_exchange", ""),
            short_exchange=getattr(record, "short_exchange", ""),
        )
        history.setdefault(cluster_key, []).append(record)
        if route_key != cluster_key:
            history.setdefault(route_key, []).append(record)
    return history


def _build_history_hint(opportunity: Opportunity, route_history: dict[str, list[object]]) -> str:
    key = _route_identity(
        symbol=opportunity.symbol,
        long_exchange=opportunity.long_exchange,
        short_exchange=opportunity.short_exchange,
        cluster_id=opportunity.cluster_id,
    )
    matches = route_history.get(key, [])
    if not matches:
        matches = route_history.get(
            _route_key_identity(opportunity.symbol, opportunity.long_exchange, opportunity.short_exchange),
            [],
        )
    if not matches:
        return "new route"
    latest = matches[0]
    previous_edge = getattr(latest, "estimated_net_edge_bps", None)
    previous_mode = getattr(latest, "execution_mode", None)
    edge_display = f"{previous_edge:.2f} bps" if isinstance(previous_edge, int | float) else "N/A"
    mode_display = previous_mode or "N/A"
    return f"seen {len(matches)}x recently | prev edge {edge_display} | prev mode {mode_display}"


def _replay_passes_min_trade_gate(opportunity: Opportunity, replay_net_after_cost_bps: float | None) -> bool:
    if replay_net_after_cost_bps is None:
        return False
    min_gate_bps = 6.0 if opportunity.execution_mode in {"small_probe", "paper"} else opportunity.normal_required_edge_bps
    return replay_net_after_cost_bps >= min_gate_bps


def _why_not_tradable_label(
    *,
    opportunity: Opportunity,
    replay_net_after_cost_bps: float | None,
    replay_passes_min_trade_gate: bool | None,
) -> str:
    risk_flags = set(opportunity.risk_flags)
    if "mixed_funding_sources" in risk_flags:
        return "mixed funding semantics"
    if "different_funding_periods" in risk_flags:
        return "funding period mismatch"
    if opportunity.data_quality_status != "healthy":
        return "quality gate downgraded opportunity"
    if replay_passes_min_trade_gate is False:
        return "replay edge too weak after costs"
    if opportunity.execution_mode == "paper":
        return "paper-only due to risk flags" if risk_flags else "insufficient confidence for live sizing"
    if opportunity.execution_mode == "small_probe":
        return "small probe only"
    if replay_net_after_cost_bps is not None and replay_net_after_cost_bps > 0:
        return "live candidate"
    return ""


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
