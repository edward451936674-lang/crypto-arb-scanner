from __future__ import annotations

from dataclasses import dataclass
import json

from app.core.symbols import parse_symbols
from app.models.observation import ObservationRecord
from app.storage.observations import ObservationStore


@dataclass(frozen=True)
class FinalOpportunitiesFilters:
    symbols: str | None
    top_n: int
    only_actionable: bool
    dedupe_by_route: bool
    min_edge_bps: float
    min_score: float


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


def _safe_raw_opportunity_json(raw_value: object) -> dict[str, object]:
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _value_from_record_then_raw(
    record: ObservationRecord,
    raw: dict[str, object],
    *,
    record_field: str,
    raw_keys: list[str],
) -> object | None:
    record_value = getattr(record, record_field, None)
    if record_value is not None:
        return record_value
    for key in raw_keys:
        value = raw.get(key)
        if value is not None:
            return value
    return None


def list_final_opportunities(
    *,
    store: ObservationStore,
    filters: FinalOpportunitiesFilters,
) -> list[dict[str, object]]:
    requested_symbols = parse_symbols(filters.symbols) if filters.symbols else None
    requested_symbol_set = {symbol.upper() for symbol in requested_symbols} if requested_symbols else None

    records = store.latest(limit=5000)
    opportunities: list[dict[str, object]] = []
    for record in records:
        symbol_key = record.symbol.upper()
        if requested_symbol_set is not None and symbol_key not in requested_symbol_set:
            continue

        raw = _safe_raw_opportunity_json(record.raw_opportunity_json)
        execution_mode = str(raw.get("execution_mode", record.execution_mode or "")).lower()
        funding_confidence_label = str(raw.get("funding_confidence_label", "")).lower()
        conviction_label = str(raw.get("conviction_label", "")).lower()
        if filters.only_actionable and (
            execution_mode in {"paper", "small_probe"}
            or funding_confidence_label == "low"
            or conviction_label == "low"
        ):
            continue

        long_exchange = str(raw.get("long_exchange", record.long_exchange))
        short_exchange = str(raw.get("short_exchange", record.short_exchange))
        symbol = str(raw.get("symbol", record.symbol)).upper()
        route_key = str(raw.get("route_key") or f"{symbol}:{long_exchange.lower()}->{short_exchange.lower()}")

        price_spread_bps = _coerce_float(
            _value_from_record_then_raw(record, raw, record_field="price_spread_bps", raw_keys=["price_spread_bps"])
        )
        funding_spread_bps = _coerce_float(
            _value_from_record_then_raw(record, raw, record_field="funding_spread_bps", raw_keys=["funding_spread_bps"])
        )
        risk_adjusted_edge_bps = _coerce_float(
            _value_from_record_then_raw(record, raw, record_field="risk_adjusted_edge_bps", raw_keys=["risk_adjusted_edge_bps"])
        )
        replay_net_after_cost_bps = _coerce_float(
            _value_from_record_then_raw(
                record,
                raw,
                record_field="replay_net_after_cost_bps",
                raw_keys=["replay_net_after_cost_bps"],
            )
        )
        estimated_net_edge_bps = _coerce_float(
            _value_from_record_then_raw(
                record,
                raw,
                record_field="estimated_net_edge_bps",
                raw_keys=["estimated_net_edge_bps", "net_edge_bps"],
            )
        )
        if (estimated_net_edge_bps or 0.0) < filters.min_edge_bps or (risk_adjusted_edge_bps or 0.0) < filters.min_score:
            continue

        opportunities.append(
            {
                "symbol": symbol,
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "price_spread_bps": price_spread_bps,
                "long_price": _coerce_float(
                    _value_from_record_then_raw(
                        record,
                        raw,
                        record_field="long_price",
                        raw_keys=["long_price", "long_mark_price", "long_reference_price"],
                    )
                ),
                "short_price": _coerce_float(
                    _value_from_record_then_raw(
                        record,
                        raw,
                        record_field="short_price",
                        raw_keys=["short_price", "short_mark_price", "short_reference_price"],
                    )
                ),
                "funding_spread_bps": funding_spread_bps,
                "risk_adjusted_edge_bps": risk_adjusted_edge_bps,
                "replay_net_after_cost_bps": replay_net_after_cost_bps,
                "estimated_net_edge_bps": estimated_net_edge_bps,
                "route_key": route_key,
                "opportunity_type": (
                    raw.get("opportunity_type")
                    if raw.get("opportunity_type") is not None
                    else _value_from_record_then_raw(
                        record,
                        raw,
                        record_field="opportunity_grade",
                        raw_keys=["opportunity_grade"],
                    )
                ),
                "execution_mode": _value_from_record_then_raw(
                    record,
                    raw,
                    record_field="execution_mode",
                    raw_keys=["execution_mode"],
                ),
                "final_position_pct": _coerce_float(
                    _value_from_record_then_raw(
                        record,
                        raw,
                        record_field="final_position_pct",
                        raw_keys=["final_position_pct"],
                    )
                ),
                "target_notional_usd": _coerce_float(raw.get("target_notional_usd")),
                "max_slippage_bps": _coerce_float(raw.get("max_slippage_bps")),
                "max_order_age_ms": _coerce_int(raw.get("max_order_age_ms")),
                "why_not_tradable": _value_from_record_then_raw(
                    record,
                    raw,
                    record_field="why_not_tradable",
                    raw_keys=["why_not_tradable"],
                ),
                "replay_confidence_label": _value_from_record_then_raw(
                    record,
                    raw,
                    record_field="replay_confidence_label",
                    raw_keys=["replay_confidence_label"],
                ),
                "replay_passes_min_trade_gate": (
                    None
                    if _value_from_record_then_raw(
                        record,
                        raw,
                        record_field="replay_passes_min_trade_gate",
                        raw_keys=["replay_passes_min_trade_gate"],
                    )
                    is None
                    else _coerce_bool(
                        _value_from_record_then_raw(
                            record,
                            raw,
                            record_field="replay_passes_min_trade_gate",
                            raw_keys=["replay_passes_min_trade_gate"],
                        )
                    )
                ),
                "risk_flags": _value_from_record_then_raw(record, raw, record_field="risk_flags", raw_keys=["risk_flags"]),
                "replay_summary": _value_from_record_then_raw(
                    record,
                    raw,
                    record_field="replay_summary",
                    raw_keys=["replay_summary"],
                ),
                "is_test": _coerce_bool(raw.get("test"), default=False),
            }
        )

    def _sort_tuple(item: dict[str, object]) -> tuple[float, float, float, str, str, str, str]:
        return (
            float(item.get("risk_adjusted_edge_bps") or 0.0),
            float(item.get("replay_net_after_cost_bps") or 0.0),
            float(item.get("estimated_net_edge_bps") or 0.0),
            str(item["route_key"]),
            str(item["symbol"]),
            str(item["long_exchange"]),
            str(item["short_exchange"]),
        )

    if filters.dedupe_by_route:
        best_by_route: dict[str, dict[str, object]] = {}
        for item in opportunities:
            item_route_key = str(item["route_key"])
            existing = best_by_route.get(item_route_key)
            if existing is None or _sort_tuple(item) > _sort_tuple(existing):
                best_by_route[item_route_key] = item
        opportunities = list(best_by_route.values())

    opportunities.sort(key=_sort_tuple, reverse=True)
    selected = opportunities[: filters.top_n]
    for index, item in enumerate(selected, start=1):
        item["rank"] = index
    return selected
