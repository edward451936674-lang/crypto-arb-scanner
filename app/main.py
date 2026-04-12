from html import escape
from dataclasses import dataclass
import hashlib
import time
from datetime import datetime, timezone
from typing import Literal

from fastapi import Body, FastAPI, HTTPException, Query
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
from app.models.execution import ExecutionCandidate
from app.models.observation import ObservationRecord
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
from app.services.final_opportunities import FinalOpportunitiesFilters, list_final_opportunities
from app.services.execution_preparation import build_execution_candidates, to_paper_execution_records
from app.services.research_summary import ResearchSummaryService
from app.venues.registry import list_venue_definitions

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


def _render_dashboard_page(
    *,
    symbols_query: str | None,
    top_n: int,
    only_actionable: bool,
    dedupe_by_route: bool,
    include_test: bool,
    refresh_seconds: int,
    final_opportunities: list[dict[str, object]],
) -> str:
    filtered_opportunities = (
        final_opportunities
        if include_test
        else [item for item in final_opportunities if not _coerce_bool(item.get("is_test"), default=False)]
    )
    rows = "".join(_render_dashboard_opportunity_row(item) for item in filtered_opportunities[:top_n])
    only_actionable_checked = "checked" if only_actionable else ""
    include_test_checked = "checked" if include_test else ""
    if rows:
        empty_state = "No opportunities match the selected filters."
    elif not include_test and final_opportunities:
        empty_state = "No non-test opportunities match the selected filters. Enable include test to view test rows."
    else:
        empty_state = "No opportunities match the selected filters."
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
    .filters {{ margin-bottom: 14px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    input, select {{ padding: 4px; }}
    .badge-test {{ display: inline-block; background: #fff1f0; color: #b42318; border: 1px solid #fecdca; border-radius: 999px; padding: 1px 7px; font-size: 12px; font-weight: 700; }}
    .muted {{ color: #667085; }}
  </style>
</head>
<body>
  <h1>Live Opportunities Dashboard</h1>
  <form method="get" class="filters">
    <label>symbol <input name="symbols" value="{escape(symbols_query or '')}" /></label>
    <label>top n <input name="top_n" type="number" min="1" max="500" value="{top_n}" /></label>
    <label>only actionable <input name="only_actionable" type="checkbox" value="true" {only_actionable_checked} /></label>
    <label>include test <input name="include_test" type="checkbox" value="true" {include_test_checked} /></label>
    <input type="hidden" name="dedupe_by_route" value="true" />
    <input type="hidden" name="refresh_seconds" value="{refresh_seconds}" />
    <button type="submit">Apply</button>
  </form>
  <table>
    <thead>
      <tr>
        <th>rank</th><th>symbol</th><th>long_exchange</th><th>short_exchange</th><th>price_spread_bps</th>
        <th>funding_spread_bps</th><th>risk_adjusted_edge_bps</th><th>replay_net_after_cost_bps</th>
        <th>estimated_net_edge_bps</th><th>execution_mode</th><th>opportunity_type</th><th>route_key</th><th>why_not_tradable</th><th>replay_confidence_label</th><th>replay_passes_min_trade_gate</th><th>final_position_pct</th><th>is_test</th>
      </tr>
    </thead>
    <tbody>{rows or f"<tr><td colspan='17' class='muted'>{escape(empty_state)}</td></tr>"}</tbody>
  </table>
</body>
</html>
"""




def _dashboard_final_opportunities(
    *,
    symbols: str | None,
    top_n: int,
    only_actionable: bool,
    dedupe_by_route: bool,
    include_test: bool,
) -> list[dict[str, object]]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    query_top_n = top_n if include_test else 500
    return list_opportunities(
        symbols=",".join(requested_symbols),
        top_n=query_top_n,
        only_actionable=only_actionable,
        dedupe_by_route=dedupe_by_route,
        min_edge_bps=0.0,
        min_score=0.0,
    )


@app.get("/", response_class=HTMLResponse)
async def root_dashboard(
    symbols: str | None = Query(default=None),
    top_n: int = Query(default=10, ge=1, le=500),
    only_actionable: bool = Query(default=False),
    dedupe_by_route: bool = Query(default=True),
    include_test: bool = Query(default=True),
    refresh_seconds: int = Query(default=15, ge=5, le=300),
) -> str:
    final_opportunities = _dashboard_final_opportunities(
        symbols=symbols,
        top_n=top_n,
        only_actionable=only_actionable,
        dedupe_by_route=dedupe_by_route,
        include_test=include_test,
    )
    return _render_dashboard_page(
        symbols_query=symbols,
        top_n=top_n,
        only_actionable=only_actionable,
        dedupe_by_route=dedupe_by_route,
        include_test=include_test,
        refresh_seconds=refresh_seconds,
        final_opportunities=final_opportunities,
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    symbols: str | None = Query(default=None),
    top_n: int = Query(default=10, ge=1, le=500),
    only_actionable: bool = Query(default=False),
    dedupe_by_route: bool = Query(default=True),
    include_test: bool = Query(default=True),
    refresh_seconds: int = Query(default=15, ge=5, le=300),
) -> str:
    final_opportunities = _dashboard_final_opportunities(
        symbols=symbols,
        top_n=top_n,
        only_actionable=only_actionable,
        dedupe_by_route=dedupe_by_route,
        include_test=include_test,
    )
    return _render_dashboard_page(
        symbols_query=symbols,
        top_n=top_n,
        only_actionable=only_actionable,
        dedupe_by_route=dedupe_by_route,
        include_test=include_test,
        refresh_seconds=refresh_seconds,
        final_opportunities=final_opportunities,
    )


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


@app.get("/api/v1/execution/venue-capabilities")
async def get_execution_venue_capabilities() -> dict[str, object]:
    venues = [item.model_dump(mode="json") for item in list_venue_definitions()]
    return {
        "live_execution_enabled": False,
        "live_execution_status": "not_enabled_in_this_repo",
        "message": "Live execution adapters are intentionally not implemented yet.",
        "venues": venues,
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


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", ""}:
            return False
    return bool(value)


def _coerce_float(value: object, *, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def list_opportunities(
    *,
    symbols: str | None,
    top_n: int,
    only_actionable: bool,
    dedupe_by_route: bool,
    min_edge_bps: float,
    min_score: float,
) -> list[dict[str, object]]:
    return list_final_opportunities(
        store=observation_store,
        filters=FinalOpportunitiesFilters(
            symbols=symbols,
            top_n=top_n,
            only_actionable=only_actionable,
            dedupe_by_route=dedupe_by_route,
            min_edge_bps=min_edge_bps,
            min_score=min_score,
        ),
    )




def list_execution_candidates(
    *,
    symbols: str | None,
    top_n: int,
    only_actionable: bool,
    include_test: bool,
) -> list[dict[str, object]]:
    query_top_n = top_n if include_test else 500
    final_opportunities = list_opportunities(
        symbols=symbols,
        top_n=query_top_n,
        only_actionable=only_actionable,
        dedupe_by_route=True,
        min_edge_bps=0.0,
        min_score=0.0,
    )
    candidates = build_execution_candidates(
        final_opportunities=final_opportunities,
        include_test=include_test,
        top_n=top_n,
    )
    return [item.model_dump() for item in candidates]


def list_paper_executions(
    *,
    top_n: int,
    status: str | None,
    outcome_status: str | None,
    symbols: str | None,
    include_test: bool,
) -> list[dict[str, object]]:
    normalized_symbols = parse_symbols(symbols) if symbols else None
    records = observation_store.latest_paper_executions(
        limit=top_n,
        status=status,
        outcome_status=outcome_status,
        symbols=normalized_symbols,
        include_test=include_test,
    )
    return [item.model_dump() for item in records]


@app.get("/api/v1/execution/candidates")
async def get_execution_candidates(
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
    top_n: int = Query(default=10, ge=1, le=500),
    only_actionable: bool = Query(default=False),
    include_test: bool = Query(default=False),
) -> list[dict[str, object]]:
    resolved_top_n = int(_coerce_float(top_n, default=10.0))
    if resolved_top_n < 1:
        resolved_top_n = 1
    if resolved_top_n > 500:
        resolved_top_n = 500
    return list_execution_candidates(
        symbols=symbols,
        top_n=resolved_top_n,
        only_actionable=_coerce_bool(only_actionable, default=False),
        include_test=_coerce_bool(include_test, default=False),
    )


@app.post("/api/v1/paper-executions/from-candidates")
async def create_paper_executions_from_candidates(
    payload: dict[str, list[str]] | None = Body(default=None),
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
    top_n: int = Query(default=10, ge=1, le=500),
    only_actionable: bool = Query(default=False),
    include_test: bool = Query(default=False),
) -> dict[str, object]:
    route_keys_raw: list[str] = []
    if payload is not None and isinstance(payload.get("route_keys"), list):
        route_keys_raw = [str(item) for item in payload.get("route_keys", [])]
    route_key_set = {item for item in route_keys_raw if item}

    candidates = list_execution_candidates(
        symbols=symbols,
        top_n=top_n,
        only_actionable=only_actionable,
        include_test=include_test,
    )
    selected_candidates = candidates
    if route_key_set:
        selected_candidates = [item for item in candidates if str(item.get("route_key") or "") in route_key_set]

    created_at_ms = int(time.time() * 1000)
    records = to_paper_execution_records([ExecutionCandidate.model_validate(item) for item in selected_candidates], created_at_ms=created_at_ms)
    inserted_count = observation_store.insert_paper_executions(records)

    return {
        "created_at_ms": created_at_ms,
        "candidate_count": len(candidates),
        "stored_count": inserted_count,
        "stored_route_keys": [item.route_key for item in records],
        "stored_symbols": sorted({item.symbol for item in records}),
    }


@app.get("/api/v1/paper-executions")
async def get_paper_executions(
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
    top_n: int = Query(default=100, ge=1, le=500),
    status: Literal["planned", "expired", "still_valid", "invalidated"] | None = Query(default=None),
    outcome_status: Literal["unknown", "flat", "positive", "negative"] | None = Query(default=None),
    include_test: bool = Query(default=False),
) -> dict[str, object]:
    items = list_paper_executions(
        top_n=top_n,
        status=status,
        outcome_status=outcome_status,
        symbols=symbols,
        include_test=_coerce_bool(include_test, default=False),
    )
    return {"count": len(items), "items": items}


def _paper_outcome_status_for_pnl(paper_pnl_bps: float | None) -> Literal["unknown", "flat", "positive", "negative"]:
    if paper_pnl_bps is None:
        return "unknown"
    if abs(paper_pnl_bps) < 0.5:
        return "flat"
    if paper_pnl_bps >= 0.5:
        return "positive"
    return "negative"


def _normalize_route_key(route_key: str | None) -> str:
    return str(route_key or "").strip()


@app.post("/api/v1/paper-executions/mark-to-market")
async def mark_to_market_paper_executions(
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
    top_n: int = Query(default=100, ge=1, le=500),
    status: Literal["planned", "expired", "still_valid", "invalidated"] | None = Query(default=None),
    include_test: bool = Query(default=False),
) -> dict[str, object]:
    records = observation_store.latest_paper_executions(
        limit=top_n,
        status=status,
        symbols=parse_symbols(symbols) if symbols else None,
        include_test=_coerce_bool(include_test, default=False),
    )
    if not records:
        return {"evaluated_count": 0, "outcome_counts": {}}

    final_opportunities = list_opportunities(
        symbols=symbols,
        top_n=5000,
        only_actionable=False,
        dedupe_by_route=True,
        min_edge_bps=0.0,
        min_score=0.0,
    )
    candidates = build_execution_candidates(final_opportunities=final_opportunities, include_test=True, top_n=5000)
    candidates_by_route = {}
    for item in candidates:
        candidate_route_key = _normalize_route_key(item.route_key)
        if candidate_route_key:
            candidates_by_route[candidate_route_key] = item

    now_ms = int(time.time() * 1000)
    outcome_counts: dict[str, int] = {}
    for record in records:
        record_route_key = _normalize_route_key(record.route_key)
        candidate = candidates_by_route.get(record_route_key) if record_route_key else None
        latest_long = None if candidate is None else candidate.entry_reference_price_long
        latest_short = None if candidate is None else candidate.entry_reference_price_short
        entry_long = record.entry_reference_price_long
        entry_short = record.entry_reference_price_short

        paper_pnl_bps: float | None = None
        if (
            entry_long is not None
            and entry_short is not None
            and latest_long is not None
            and latest_short is not None
            and entry_long > 0
            and entry_short > 0
        ):
            long_leg_return_bps = (latest_long / entry_long - 1.0) * 10000.0
            short_leg_return_bps = (1.0 - latest_short / entry_short) * 10000.0
            paper_pnl_bps = long_leg_return_bps + short_leg_return_bps
        paper_pnl_usd = (
            None
            if paper_pnl_bps is None or record.target_notional_usd is None
            else record.target_notional_usd * paper_pnl_bps / 10000.0
        )
        outcome_status = _paper_outcome_status_for_pnl(paper_pnl_bps)
        observation_store.update_paper_execution_outcome(
            paper_execution_id=int(record.id or 0),
            latest_reference_price_long=latest_long,
            latest_reference_price_short=latest_short,
            paper_pnl_bps=paper_pnl_bps,
            paper_pnl_usd=paper_pnl_usd,
            outcome_status=outcome_status,
            outcome_updated_at_ms=now_ms,
        )
        outcome_counts[outcome_status] = outcome_counts.get(outcome_status, 0) + 1

    return {"evaluated_count": len(records), "outcome_counts": outcome_counts}


@app.post("/api/v1/paper-executions/evaluate")
async def evaluate_paper_executions(
    symbols: str | None = Query(default=None, description="Comma separated base symbols, e.g. BTC,ETH,SOL"),
    top_n: int = Query(default=100, ge=1, le=500),
    include_test: bool = Query(default=False),
) -> dict[str, object]:
    planned_records = observation_store.latest_paper_executions(
        limit=top_n,
        status="planned",
        symbols=parse_symbols(symbols) if symbols else None,
        include_test=_coerce_bool(include_test, default=False),
    )
    if not planned_records:
        return {"evaluated_count": 0, "status_counts": {}}

    now_ms = int(time.time() * 1000)
    final_opportunities = list_opportunities(
        symbols=symbols,
        top_n=5000,
        only_actionable=False,
        dedupe_by_route=True,
        min_edge_bps=0.0,
        min_score=0.0,
    )
    candidates = build_execution_candidates(final_opportunities=final_opportunities, include_test=True, top_n=5000)
    candidates_by_route = {item.route_key: item for item in candidates}

    status_counts: dict[str, int] = {}
    for record in planned_records:
        candidate = candidates_by_route.get(record.route_key)
        next_status: Literal["expired", "still_valid", "invalidated"]
        closure_reason: str
        if now_ms > record.expires_at_ms:
            next_status = "expired"
            closure_reason = "expired"
        elif candidate is None:
            next_status = "invalidated"
            closure_reason = "route_missing"
        elif not candidate.is_executable_now:
            next_status = "invalidated"
            closure_reason = "no_longer_executable"
        else:
            next_status = "still_valid"
            closure_reason = "still_valid"

        observation_store.update_paper_execution_lifecycle(
            paper_execution_id=int(record.id or 0),
            status=next_status,
            status_updated_at_ms=now_ms,
            closed_at_ms=now_ms,
            closure_reason=closure_reason,
            latest_observed_edge_bps=None if candidate is None else candidate.expected_edge_bps,
            latest_replay_net_after_cost_bps=None if candidate is None else candidate.replay_net_after_cost_bps,
            latest_risk_adjusted_edge_bps=None if candidate is None else candidate.risk_adjusted_edge_bps,
        )
        status_counts[next_status] = status_counts.get(next_status, 0) + 1

    return {"evaluated_count": len(planned_records), "status_counts": status_counts}


@app.get("/api/v1/opportunities")
async def get_opportunities(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    ),
    top_n: int = Query(default=10, ge=1, le=500),
    only_actionable: bool = Query(default=False),
    dedupe_by_route: bool = Query(default=True),
    min_edge_bps: float = Query(default=0.0),
    min_score: float = Query(default=0.0),
) -> list[dict[str, object]]:
    resolved_top_n = int(_coerce_float(top_n, default=10.0))
    if resolved_top_n < 1:
        resolved_top_n = 1
    if resolved_top_n > 500:
        resolved_top_n = 500
    return list_opportunities(
        symbols=symbols,
        top_n=resolved_top_n,
        only_actionable=_coerce_bool(only_actionable, default=False),
        dedupe_by_route=_coerce_bool(dedupe_by_route, default=True),
        min_edge_bps=_coerce_float(min_edge_bps, default=0.0),
        min_score=_coerce_float(min_score, default=0.0),
    )


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


@app.get("/api/v1/research/routes")
async def get_research_routes(
    limit: int = Query(default=20, ge=1, le=200),
    sort_by: str = Query(default="observation_count"),
) -> dict[str, object]:
    items = ResearchSummaryService(observation_store).route_summaries(limit=limit, sort_by=sort_by)
    return {"count": len(items), "sort_by": sort_by, "items": items}


@app.get("/api/v1/research/symbol")
async def get_research_symbol(
    symbol: str = Query(..., min_length=1, description="Base symbol, e.g. BTC"),
    limit: int = Query(default=50, ge=1, le=500),
    sort_by: str = Query(default="observation_count"),
) -> dict[str, object]:
    items = ResearchSummaryService(observation_store).route_summaries(limit=limit, symbol=symbol, sort_by=sort_by)
    return {"symbol": symbol.upper(), "count": len(items), "sort_by": sort_by, "items": items}


@app.get("/api/v1/research/why-not-breakdown")
async def get_research_why_not_breakdown() -> dict[str, object]:
    return ResearchSummaryService(observation_store).why_not_breakdown()


@app.get("/api/v1/research/replay-calibration")
async def get_research_replay_calibration(top_n: int = Query(default=10, ge=1, le=100)) -> dict[str, object]:
    return ResearchSummaryService(observation_store).replay_calibration(top_n=top_n)


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


def _rank_and_filter_opportunities(
    *,
    scan_context: _ScanContext,
    min_edge_bps: float,
    min_score: float | None,
    symbol: str | None,
    only_actionable: bool,
    dedupe_by_route: bool,
    top_n: int | None,
) -> list[Opportunity]:
    assumptions = ReplayAssumptions(
        holding_mode="to_next_funding",
        slippage_bps_per_leg=1.0,
        extra_exit_slippage_bps_per_leg=0.5,
        latency_decay_bps=0.2,
        borrow_or_misc_cost_bps=0.0,
    )
    replay_service = OpportunityReplayService()
    snapshot_lookup = _snapshot_lookup(scan_context.accepted_snapshots)
    recent_observations = observation_store.latest(limit=500)
    route_history = _build_route_history_lookup(recent_observations)

    enriched: list[tuple[tuple[float, ...], Opportunity]] = []
    for opportunity in scan_context.opportunities:
        if opportunity.net_edge_bps < min_edge_bps:
            continue
        if symbol and opportunity.symbol != symbol:
            continue
        if only_actionable and not _is_actionable_opportunity(opportunity):
            continue

        route_key = alert_memory.route_key_for(
            symbol=opportunity.symbol,
            long_exchange=opportunity.long_exchange,
            short_exchange=opportunity.short_exchange,
        )
        long_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.long_exchange.lower()))
        short_snapshot = snapshot_lookup.get((opportunity.symbol, opportunity.short_exchange.lower()))
        replay_net = None
        if long_snapshot is not None and short_snapshot is not None:
            replay_net = replay_service.replay(opportunity, long_snapshot, short_snapshot, assumptions).net_realized_edge_bps

        observation_count = len(route_history.get(_route_key_identity(opportunity.symbol, opportunity.long_exchange, opportunity.short_exchange), []))
        score = _opportunity_output_score(
            opportunity=opportunity,
            replay_net_after_cost_bps=replay_net,
            observation_count=observation_count,
        )
        if min_score is not None and score < min_score:
            continue
        opportunity_type = "cash_and_carry" if opportunity.hourly_funding_spread_bps is not None else "basis"
        updated = opportunity.model_copy(
            update={
                "route_key": route_key,
                "replay_net_after_cost_bps": replay_net,
                "score": score,
                "opportunity_type": opportunity_type,
            }
        )
        ranking_key = (
            score,
            replay_net if replay_net is not None else float("-inf"),
            updated.net_edge_bps,
            updated.final_position_pct,
            float(observation_count),
            -_execution_mode_rank(updated.execution_mode),
        )
        enriched.append((ranking_key, updated))

    enriched.sort(
        key=lambda item: (
            -item[0][0],
            -item[0][1],
            -item[0][2],
            -item[0][3],
            -item[0][4],
            item[0][5],
            item[1].symbol,
            item[1].long_exchange,
            item[1].short_exchange,
        )
    )
    ranked = [item for _, item in enriched]
    if dedupe_by_route:
        ranked = _dedupe_best_by_route(ranked)
    if top_n is not None:
        ranked = ranked[:top_n]

    return [item.model_copy(update={"rank": index}) for index, item in enumerate(ranked, start=1)]


def _is_actionable_opportunity(opportunity: Opportunity) -> bool:
    if opportunity.execution_mode == "paper":
        return False
    if opportunity.final_position_pct <= 0.0:
        return False
    if opportunity.net_edge_bps < 8.0:
        return False
    return opportunity.is_executable_now


def _execution_mode_rank(execution_mode: str) -> int:
    return {
        "extended_size_up": 5,
        "size_up": 4,
        "normal": 3,
        "small_probe": 2,
        "paper": 1,
    }.get(execution_mode, 0)


def _opportunity_output_score(
    *,
    opportunity: Opportunity,
    replay_net_after_cost_bps: float | None,
    observation_count: int,
) -> float:
    replay_component = replay_net_after_cost_bps if replay_net_after_cost_bps is not None else opportunity.net_edge_bps
    persistence_bonus = min(observation_count, 20) * 0.15
    execution_bonus = _execution_mode_rank(opportunity.execution_mode) * 0.75
    position_bonus = opportunity.final_position_pct * 100
    return replay_component * 0.45 + opportunity.net_edge_bps * 0.35 + execution_bonus + position_bonus + persistence_bonus


def _dedupe_best_by_route(opportunities: list[Opportunity]) -> list[Opportunity]:
    deduped: list[Opportunity] = []
    seen_routes: set[str] = set()
    for item in opportunities:
        route_key = item.route_key or alert_memory.route_key_for(
            symbol=item.symbol,
            long_exchange=item.long_exchange,
            short_exchange=item.short_exchange,
        )
        if route_key in seen_routes:
            continue
        seen_routes.add(route_key)
        deduped.append(item)
    return deduped


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


def _render_dashboard_opportunity_row(item: dict[str, object]) -> str:
    def _fmt_bps(value: object) -> str:
        coerced = _coerce_float(value, default=float("nan"))
        return f"{coerced:.2f}" if coerced == coerced else "—"

    is_test = _coerce_bool(item.get("is_test"), default=False)
    test_display = "<span class='badge-test'>TEST</span>" if is_test else ""
    replay_passes_gate_display = (
        "yes"
        if item.get("replay_passes_min_trade_gate") is True
        else "no" if item.get("replay_passes_min_trade_gate") is False else "-"
    )
    final_position = item.get("final_position_pct")
    final_position_display = f"{float(final_position):.2%}" if isinstance(final_position, int | float) else "-"
    return (
        "<tr>"
        f"<td>{int(item.get('rank', 0))}</td>"
        f"<td>{escape(str(item.get('symbol', '-')))}</td>"
        f"<td>{escape(str(item.get('long_exchange', '-')))}</td>"
        f"<td>{escape(str(item.get('short_exchange', '-')))}</td>"
        f"<td>{_fmt_bps(item.get('price_spread_bps'))}</td>"
        f"<td>{_fmt_bps(item.get('funding_spread_bps'))}</td>"
        f"<td>{_fmt_bps(item.get('risk_adjusted_edge_bps'))}</td>"
        f"<td>{_fmt_bps(item.get('replay_net_after_cost_bps'))}</td>"
        f"<td>{_fmt_bps(item.get('estimated_net_edge_bps'))}</td>"
        f"<td>{escape(str(item.get('execution_mode') or '-'))}</td>"
        f"<td>{escape(str(item.get('opportunity_type') or 'unknown'))}</td>"
        f"<td>{escape(str(item.get('route_key', '-')))}</td>"
        f"<td>{escape(str(item.get('why_not_tradable') or '-'))}</td>"
        f"<td>{escape(str(item.get('replay_confidence_label') or '-'))}</td>"
        f"<td>{replay_passes_gate_display}</td>"
        f"<td>{final_position_display}</td>"
        f"<td>{test_display}</td>"
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
