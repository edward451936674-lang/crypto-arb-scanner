import json
from collections import defaultdict
from dataclasses import dataclass

from app.storage.observations import ObservationStore


@dataclass(frozen=True)
class RouteSummary:
    symbol: str
    long_exchange: str
    short_exchange: str
    route_key: str
    observation_count: int
    alert_count: int
    avg_estimated_net_edge_bps: float | None
    avg_replay_net_after_cost_bps: float | None
    small_probe_count: int
    paper_count: int
    first_seen_at_ms: int | None
    last_seen_at_ms: int | None
    persistence_window_ms: int | None
    last_execution_mode: str | None


class ResearchSummaryService:
    def __init__(self, store: ObservationStore) -> None:
        self._store = store

    def route_summaries(self, *, limit: int = 20, symbol: str | None = None, sort_by: str = "observation_count") -> list[dict[str, object]]:
        if sort_by not in {"observation_count", "avg_estimated_net_edge_bps"}:
            sort_by = "observation_count"
        summaries = [item.__dict__ for item in self._fetch_route_summaries(symbol=symbol)]
        if sort_by == "avg_estimated_net_edge_bps":
            summaries.sort(
                key=lambda item: (
                    -(item["avg_estimated_net_edge_bps"] or float("-inf")),
                    -item["observation_count"],
                    item["route_key"],
                )
            )
        else:
            summaries.sort(
                key=lambda item: (
                    -item["observation_count"],
                    -(item["avg_estimated_net_edge_bps"] or float("-inf")),
                    item["route_key"],
                )
            )
        return summaries[:limit]

    def why_not_breakdown(self) -> dict[str, object]:
        with self._store._connect() as conn:
            rows = conn.execute(
                """
                SELECT risk_flags, estimated_net_edge_bps
                FROM observations
                ORDER BY observed_at_ms ASC, id ASC
                """
            ).fetchall()

        stats: dict[str, dict[str, float | int]] = defaultdict(lambda: {"count": 0, "edge_sum": 0.0, "edge_count": 0})
        for row in rows:
            risk_flags = self._parse_risk_flags(row["risk_flags"])
            estimated_edge = row["estimated_net_edge_bps"]
            for flag in risk_flags:
                stats[flag]["count"] += 1
                if estimated_edge is not None:
                    stats[flag]["edge_sum"] += float(estimated_edge)
                    stats[flag]["edge_count"] += 1

        items: list[dict[str, object]] = []
        for flag in sorted(stats):
            count = int(stats[flag]["count"])
            edge_count = int(stats[flag]["edge_count"])
            avg_edge = None if edge_count == 0 else float(stats[flag]["edge_sum"]) / edge_count
            items.append(
                {
                    "risk_flag": flag,
                    "count": count,
                    "avg_estimated_net_edge_bps": avg_edge,
                }
            )

        return {"count": len(items), "items": items}

    def replay_calibration(self, *, top_n: int = 10) -> dict[str, object]:
        routes = self._fetch_route_summaries(symbol=None)
        estimated_values = [item.avg_estimated_net_edge_bps for item in routes if item.avg_estimated_net_edge_bps is not None]
        replay_values = [item.avg_replay_net_after_cost_bps for item in routes if item.avg_replay_net_after_cost_bps is not None]

        comparable: list[dict[str, object]] = []
        for item in routes:
            if item.avg_estimated_net_edge_bps is None or item.avg_replay_net_after_cost_bps is None:
                continue
            diff = item.avg_estimated_net_edge_bps - item.avg_replay_net_after_cost_bps
            comparable.append(
                {
                    "route_key": item.route_key,
                    "symbol": item.symbol,
                    "long_exchange": item.long_exchange,
                    "short_exchange": item.short_exchange,
                    "observation_count": item.observation_count,
                    "avg_estimated_net_edge_bps": item.avg_estimated_net_edge_bps,
                    "avg_replay_net_after_cost_bps": item.avg_replay_net_after_cost_bps,
                    "avg_overestimation_bps": diff,
                }
            )

        comparable.sort(
            key=lambda item: (
                -item["avg_overestimation_bps"],
                -item["observation_count"],
                item["route_key"],
            )
        )

        overall_estimated = None if not estimated_values else sum(estimated_values) / len(estimated_values)
        overall_replay = None if not replay_values else sum(replay_values) / len(replay_values)
        overall_diff = None
        if overall_estimated is not None and overall_replay is not None:
            overall_diff = overall_estimated - overall_replay

        return {
            "route_count": len(routes),
            "comparable_route_count": len(comparable),
            "avg_estimated_net_edge_bps": overall_estimated,
            "avg_replay_net_after_cost_bps": overall_replay,
            "avg_overestimation_bps": overall_diff,
            "top_overestimated_routes": comparable[:top_n],
        }

    def _fetch_route_summaries(self, *, symbol: str | None) -> list[RouteSummary]:
        params: list[object] = []
        filter_clause = ""
        if symbol:
            filter_clause = "WHERE o.symbol = ?"
            params.append(symbol.upper())

        query = f"""
            SELECT
                o.symbol,
                o.long_exchange,
                o.short_exchange,
                COUNT(*) AS observation_count,
                AVG(o.estimated_net_edge_bps) AS avg_estimated_net_edge_bps,
                AVG(o.replay_net_after_cost_bps) AS avg_replay_net_after_cost_bps,
                SUM(CASE WHEN o.execution_mode = 'small_probe' THEN 1 ELSE 0 END) AS small_probe_count,
                SUM(CASE WHEN o.execution_mode = 'paper' THEN 1 ELSE 0 END) AS paper_count,
                MIN(o.observed_at_ms) AS first_seen_at_ms,
                MAX(o.observed_at_ms) AS last_seen_at_ms,
                (
                    SELECT x.execution_mode
                    FROM observations x
                    WHERE x.symbol = o.symbol
                      AND x.long_exchange = o.long_exchange
                      AND x.short_exchange = o.short_exchange
                    ORDER BY x.observed_at_ms DESC, x.id DESC
                    LIMIT 1
                ) AS last_execution_mode,
                COALESCE(a.alert_count, 0) AS alert_count
            FROM observations o
            LEFT JOIN (
                SELECT symbol, long_exchange, short_exchange, COUNT(*) AS alert_count
                FROM alert_events
                GROUP BY symbol, long_exchange, short_exchange
            ) a
              ON a.symbol = o.symbol
             AND a.long_exchange = o.long_exchange
             AND a.short_exchange = o.short_exchange
            {filter_clause}
            GROUP BY o.symbol, o.long_exchange, o.short_exchange
        """

        with self._store._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        summaries: list[RouteSummary] = []
        for row in rows:
            route_key = f"{row['symbol']}:{str(row['long_exchange']).lower()}->{str(row['short_exchange']).lower()}"
            first_seen = row["first_seen_at_ms"]
            last_seen = row["last_seen_at_ms"]
            persistence_window = None
            if first_seen is not None and last_seen is not None:
                persistence_window = int(last_seen) - int(first_seen)
            summaries.append(
                RouteSummary(
                    symbol=row["symbol"],
                    long_exchange=row["long_exchange"],
                    short_exchange=row["short_exchange"],
                    route_key=route_key,
                    observation_count=int(row["observation_count"]),
                    alert_count=int(row["alert_count"]),
                    avg_estimated_net_edge_bps=row["avg_estimated_net_edge_bps"],
                    avg_replay_net_after_cost_bps=row["avg_replay_net_after_cost_bps"],
                    small_probe_count=int(row["small_probe_count"]),
                    paper_count=int(row["paper_count"]),
                    first_seen_at_ms=first_seen,
                    last_seen_at_ms=last_seen,
                    persistence_window_ms=persistence_window,
                    last_execution_mode=row["last_execution_mode"],
                )
            )
        return summaries

    @staticmethod
    def _parse_risk_flags(raw_value: object) -> list[str]:
        if not isinstance(raw_value, str) or not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [str(item) for item in parsed if isinstance(item, str) and item]
