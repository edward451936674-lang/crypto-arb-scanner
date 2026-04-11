import json
import sqlite3
from pathlib import Path

from app.models.execution import PaperExecutionRecord
from app.models.observation import ObservationRecord


class ObservationStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self) -> None:
        db_parent = Path(self.db_path).parent
        db_parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at_ms INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    long_exchange TEXT NOT NULL,
                    short_exchange TEXT NOT NULL,
                    estimated_net_edge_bps REAL,
                    opportunity_grade TEXT,
                    execution_mode TEXT,
                    final_position_pct REAL,
                    why_not_tradable TEXT,
                    replay_net_after_cost_bps REAL,
                    replay_confidence_label TEXT,
                    replay_passes_min_trade_gate INTEGER,
                    risk_flags TEXT,
                    replay_summary TEXT,
                    raw_opportunity_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at_ms INTEGER NOT NULL,
                    dedupe_identity TEXT NOT NULL,
                    cluster_id TEXT,
                    route_key TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    long_exchange TEXT NOT NULL,
                    short_exchange TEXT NOT NULL,
                    execution_mode TEXT,
                    final_position_pct REAL,
                    replay_net_after_cost_bps REAL,
                    replay_passes_min_trade_gate INTEGER,
                    message_hash TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_events_identity_sent_at
                ON alert_events(dedupe_identity, sent_at_ms DESC, id DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    long_exchange TEXT NOT NULL,
                    short_exchange TEXT NOT NULL,
                    route_key TEXT NOT NULL,
                    opportunity_type TEXT,
                    execution_mode TEXT,
                    target_position_pct REAL,
                    target_notional_usd REAL,
                    expected_edge_bps REAL,
                    replay_net_after_cost_bps REAL,
                    risk_adjusted_edge_bps REAL,
                    is_executable_now INTEGER NOT NULL,
                    why_not_executable TEXT,
                    replay_confidence_label TEXT,
                    replay_passes_min_trade_gate INTEGER,
                    risk_flags TEXT,
                    raw_execution_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_paper_executions_created_at
                ON paper_executions(created_at_ms DESC, id DESC)
                """
            )

    def insert_many(self, observations: list[ObservationRecord]) -> int:
        if not observations:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO observations (
                    observed_at_ms,
                    symbol,
                    cluster_id,
                    long_exchange,
                    short_exchange,
                    estimated_net_edge_bps,
                    opportunity_grade,
                    execution_mode,
                    final_position_pct,
                    why_not_tradable,
                    replay_net_after_cost_bps,
                    replay_confidence_label,
                    replay_passes_min_trade_gate,
                    risk_flags,
                    replay_summary,
                    raw_opportunity_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.observed_at_ms,
                        item.symbol,
                        item.cluster_id,
                        item.long_exchange,
                        item.short_exchange,
                        item.estimated_net_edge_bps,
                        item.opportunity_grade,
                        item.execution_mode,
                        item.final_position_pct,
                        item.why_not_tradable,
                        item.replay_net_after_cost_bps,
                        item.replay_confidence_label,
                        None if item.replay_passes_min_trade_gate is None else int(item.replay_passes_min_trade_gate),
                        json.dumps(item.risk_flags),
                        item.replay_summary,
                        json.dumps(item.raw_opportunity_json),
                    )
                    for item in observations
                ],
            )
        return len(observations)

    def latest(self, limit: int = 20) -> list[ObservationRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM observations
                ORDER BY observed_at_ms DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def history(self, symbol: str, limit: int = 100) -> list[ObservationRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM observations
                WHERE symbol = ?
                ORDER BY observed_at_ms DESC, id DESC
                LIMIT ?
                """,
                (symbol.upper(), limit),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def latest_alert_event(self, dedupe_identity: str) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM alert_events
                WHERE dedupe_identity = ?
                ORDER BY sent_at_ms DESC, id DESC
                LIMIT 1
                """,
                (dedupe_identity,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_alert_event(row)


    def latest_alert_events(self, limit: int = 20) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM alert_events
                ORDER BY sent_at_ms DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_alert_event(row) for row in rows]

    def insert_paper_executions(self, records: list[PaperExecutionRecord]) -> int:
        if not records:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO paper_executions (
                    created_at_ms,
                    symbol,
                    long_exchange,
                    short_exchange,
                    route_key,
                    opportunity_type,
                    execution_mode,
                    target_position_pct,
                    target_notional_usd,
                    expected_edge_bps,
                    replay_net_after_cost_bps,
                    risk_adjusted_edge_bps,
                    is_executable_now,
                    why_not_executable,
                    replay_confidence_label,
                    replay_passes_min_trade_gate,
                    risk_flags,
                    raw_execution_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.created_at_ms,
                        item.symbol,
                        item.long_exchange,
                        item.short_exchange,
                        item.route_key,
                        item.opportunity_type,
                        item.execution_mode,
                        item.target_position_pct,
                        item.target_notional_usd,
                        item.expected_edge_bps,
                        item.replay_net_after_cost_bps,
                        item.risk_adjusted_edge_bps,
                        int(item.is_executable_now),
                        item.why_not_executable,
                        item.replay_confidence_label,
                        None if item.replay_passes_min_trade_gate is None else int(item.replay_passes_min_trade_gate),
                        json.dumps(item.risk_flags),
                        json.dumps(item.raw_execution_json),
                    )
                    for item in records
                ],
            )
        return len(records)

    def latest_paper_executions(self, limit: int = 100) -> list[PaperExecutionRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM paper_executions
                ORDER BY created_at_ms DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_paper_execution(row) for row in rows]

    def _row_to_alert_event(self, row: sqlite3.Row) -> dict[str, object]:
        raw_gate = row["replay_passes_min_trade_gate"]
        return {
            "id": row["id"],
            "sent_at_ms": row["sent_at_ms"],
            "dedupe_identity": row["dedupe_identity"],
            "cluster_id": row["cluster_id"],
            "route_key": row["route_key"],
            "symbol": row["symbol"],
            "long_exchange": row["long_exchange"],
            "short_exchange": row["short_exchange"],
            "execution_mode": row["execution_mode"],
            "final_position_pct": row["final_position_pct"],
            "replay_net_after_cost_bps": row["replay_net_after_cost_bps"],
            "replay_passes_min_trade_gate": None if raw_gate is None else bool(raw_gate),
            "message_hash": row["message_hash"],
        }
    def insert_alert_event(
        self,
        *,
        sent_at_ms: int,
        dedupe_identity: str,
        cluster_id: str | None,
        route_key: str,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        execution_mode: str | None,
        final_position_pct: float | None,
        replay_net_after_cost_bps: float | None,
        replay_passes_min_trade_gate: bool | None,
        message_hash: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO alert_events (
                    sent_at_ms,
                    dedupe_identity,
                    cluster_id,
                    route_key,
                    symbol,
                    long_exchange,
                    short_exchange,
                    execution_mode,
                    final_position_pct,
                    replay_net_after_cost_bps,
                    replay_passes_min_trade_gate,
                    message_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sent_at_ms,
                    dedupe_identity,
                    cluster_id,
                    route_key,
                    symbol,
                    long_exchange,
                    short_exchange,
                    execution_mode,
                    final_position_pct,
                    replay_net_after_cost_bps,
                    None if replay_passes_min_trade_gate is None else int(replay_passes_min_trade_gate),
                    message_hash,
                ),
            )

    def _row_to_paper_execution(self, row: sqlite3.Row) -> PaperExecutionRecord:
        raw_gate = row["replay_passes_min_trade_gate"]
        return PaperExecutionRecord(
            id=row["id"],
            created_at_ms=row["created_at_ms"],
            symbol=row["symbol"],
            long_exchange=row["long_exchange"],
            short_exchange=row["short_exchange"],
            route_key=row["route_key"],
            opportunity_type=row["opportunity_type"],
            execution_mode=row["execution_mode"],
            target_position_pct=row["target_position_pct"],
            target_notional_usd=row["target_notional_usd"],
            expected_edge_bps=row["expected_edge_bps"],
            replay_net_after_cost_bps=row["replay_net_after_cost_bps"],
            risk_adjusted_edge_bps=row["risk_adjusted_edge_bps"],
            is_executable_now=bool(row["is_executable_now"]),
            why_not_executable=row["why_not_executable"],
            replay_confidence_label=row["replay_confidence_label"],
            replay_passes_min_trade_gate=None if raw_gate is None else bool(raw_gate),
            risk_flags=json.loads(row["risk_flags"] or "[]"),
            raw_execution_json=json.loads(row["raw_execution_json"] or "{}"),
        )

    def _row_to_record(self, row: sqlite3.Row) -> ObservationRecord:
        raw_passes = row["replay_passes_min_trade_gate"]
        return ObservationRecord(
            id=row["id"],
            observed_at_ms=row["observed_at_ms"],
            symbol=row["symbol"],
            cluster_id=row["cluster_id"],
            long_exchange=row["long_exchange"],
            short_exchange=row["short_exchange"],
            estimated_net_edge_bps=row["estimated_net_edge_bps"],
            opportunity_grade=row["opportunity_grade"],
            execution_mode=row["execution_mode"],
            final_position_pct=row["final_position_pct"],
            why_not_tradable=row["why_not_tradable"],
            replay_net_after_cost_bps=row["replay_net_after_cost_bps"],
            replay_confidence_label=row["replay_confidence_label"],
            replay_passes_min_trade_gate=None if raw_passes is None else bool(raw_passes),
            risk_flags=json.loads(row["risk_flags"] or "[]"),
            replay_summary=row["replay_summary"],
            raw_opportunity_json=json.loads(row["raw_opportunity_json"] or "{}"),
        )
