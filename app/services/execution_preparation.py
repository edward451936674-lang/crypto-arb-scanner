from __future__ import annotations

import time

from app.models.execution import ExecutionCandidate, ExecutionPlan, PaperExecutionRecord


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _normalize_risk_flags(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def _execution_readiness(item: dict[str, object]) -> tuple[bool, str | None]:
    reasons: list[str] = []
    execution_mode = str(item.get("execution_mode") or "").lower()
    if execution_mode == "paper":
        reasons.append("execution_mode_paper")

    replay_passes = item.get("replay_passes_min_trade_gate")
    if replay_passes is not True:
        reasons.append("replay_min_trade_gate_not_passed")

    final_position_pct = _coerce_float(item.get("final_position_pct"))
    if final_position_pct is None or final_position_pct <= 0.0:
        reasons.append("no_target_position")

    why_not_tradable = item.get("why_not_tradable")
    if why_not_tradable is not None and str(why_not_tradable).strip() != "":
        reasons.append(str(why_not_tradable))

    risk_flags = _normalize_risk_flags(item.get("risk_flags"))
    if risk_flags:
        reasons.append("risk_flags_present")

    if reasons:
        return False, ";".join(reasons)
    return True, None


def build_execution_candidates(
    *,
    final_opportunities: list[dict[str, object]],
    include_test: bool,
    top_n: int,
) -> list[ExecutionCandidate]:
    generated_at_ms = int(time.time() * 1000)
    candidates: list[ExecutionCandidate] = []
    for item in final_opportunities:
        is_test = _coerce_bool(item.get("is_test"), default=False)
        if not include_test and is_test:
            continue

        risk_flags = _normalize_risk_flags(item.get("risk_flags"))
        is_executable_now, why_not_executable = _execution_readiness(item)
        plan = ExecutionPlan(
            target_position_pct=_coerce_float(item.get("final_position_pct")),
            target_notional_usd=_coerce_float(item.get("target_notional_usd")),
            max_slippage_bps=_coerce_float(item.get("max_slippage_bps")),
            max_order_age_ms=_coerce_int(item.get("max_order_age_ms")),
        )
        candidates.append(
            ExecutionCandidate(
                symbol=str(item.get("symbol") or "").upper(),
                long_exchange=str(item.get("long_exchange") or ""),
                short_exchange=str(item.get("short_exchange") or ""),
                route_key=str(item.get("route_key") or ""),
                opportunity_type=(None if item.get("opportunity_type") is None else str(item.get("opportunity_type"))),
                execution_mode=(None if item.get("execution_mode") is None else str(item.get("execution_mode"))),
                expected_edge_bps=_coerce_float(item.get("estimated_net_edge_bps")),
                replay_net_after_cost_bps=_coerce_float(item.get("replay_net_after_cost_bps")),
                risk_adjusted_edge_bps=_coerce_float(item.get("risk_adjusted_edge_bps")),
                target_position_pct=plan.target_position_pct,
                target_notional_usd=plan.target_notional_usd,
                max_slippage_bps=plan.max_slippage_bps,
                max_order_age_ms=plan.max_order_age_ms,
                is_executable_now=is_executable_now,
                why_not_executable=why_not_executable,
                replay_confidence_label=(
                    None if item.get("replay_confidence_label") is None else str(item.get("replay_confidence_label"))
                ),
                replay_passes_min_trade_gate=(
                    None if item.get("replay_passes_min_trade_gate") is None else _coerce_bool(item.get("replay_passes_min_trade_gate"))
                ),
                risk_flags=risk_flags,
                generated_at_ms=generated_at_ms,
                is_test=is_test,
            )
        )

    candidates.sort(
        key=lambda item: (
            item.is_executable_now,
            float(item.risk_adjusted_edge_bps or 0.0),
            float(item.replay_net_after_cost_bps or 0.0),
            float(item.expected_edge_bps or 0.0),
            item.route_key,
        ),
        reverse=True,
    )
    selected = candidates[:top_n]
    return selected


def to_paper_execution_records(candidates: list[ExecutionCandidate], *, created_at_ms: int) -> list[PaperExecutionRecord]:
    records: list[PaperExecutionRecord] = []
    for candidate in candidates:
        payload = candidate.model_dump()
        records.append(
            PaperExecutionRecord(
                created_at_ms=created_at_ms,
                symbol=candidate.symbol,
                long_exchange=candidate.long_exchange,
                short_exchange=candidate.short_exchange,
                route_key=candidate.route_key,
                opportunity_type=candidate.opportunity_type,
                execution_mode=candidate.execution_mode,
                target_position_pct=candidate.target_position_pct,
                target_notional_usd=candidate.target_notional_usd,
                expected_edge_bps=candidate.expected_edge_bps,
                replay_net_after_cost_bps=candidate.replay_net_after_cost_bps,
                risk_adjusted_edge_bps=candidate.risk_adjusted_edge_bps,
                is_executable_now=candidate.is_executable_now,
                why_not_executable=candidate.why_not_executable,
                replay_confidence_label=candidate.replay_confidence_label,
                replay_passes_min_trade_gate=candidate.replay_passes_min_trade_gate,
                risk_flags=candidate.risk_flags,
                raw_execution_json=payload,
            )
        )
    return records
