import sqlite3

from app.storage.observations import ObservationStore


EXPECTED_OBSERVATIONS_COLUMNS = [
    "id",
    "observed_at_ms",
    "symbol",
    "cluster_id",
    "long_exchange",
    "short_exchange",
    "estimated_net_edge_bps",
    "opportunity_grade",
    "execution_mode",
    "final_position_pct",
    "why_not_tradable",
    "replay_net_after_cost_bps",
    "replay_confidence_label",
    "replay_passes_min_trade_gate",
    "risk_flags",
    "replay_summary",
    "raw_opportunity_json",
]


def test_observations_schema_is_unchanged(tmp_path) -> None:
    db_path = tmp_path / "observations.sqlite3"
    ObservationStore(str(db_path))

    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()]

    assert columns == EXPECTED_OBSERVATIONS_COLUMNS
