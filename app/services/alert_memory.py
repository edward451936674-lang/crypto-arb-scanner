from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlertCandidate:
    dedupe_identity: str
    cluster_id: str | None
    route_key: str
    symbol: str
    long_exchange: str
    short_exchange: str
    execution_mode: str | None
    final_position_pct: float | None
    replay_net_after_cost_bps: float | None
    replay_passes_min_trade_gate: bool | None


@dataclass(frozen=True)
class AlertDecision:
    should_send: bool
    reason: str


class AlertMemoryService:
    def __init__(
        self,
        *,
        cooldown_minutes: int = 10,
        replay_improvement_bps: float = 3.0,
        final_position_improvement_pct: float = 0.01,
    ) -> None:
        self._cooldown_ms = cooldown_minutes * 60 * 1000
        self._replay_improvement_bps = replay_improvement_bps
        self._final_position_improvement_pct = final_position_improvement_pct

    @staticmethod
    def route_key_for(
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
    ) -> str:
        return f"{symbol.upper()}:{long_exchange.lower()}->{short_exchange.lower()}"

    @classmethod
    def dedupe_identity_for(
        cls,
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        cluster_id: str | None,
    ) -> tuple[str, str]:
        route_key = cls.route_key_for(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
        )
        if cluster_id:
            return f"cluster:{cluster_id}", route_key
        return f"route:{route_key}", route_key

    def evaluate(
        self,
        *,
        candidate: AlertCandidate,
        previous_event: dict[str, object] | None,
        now_ms: int,
    ) -> AlertDecision:
        if previous_event is None:
            return AlertDecision(should_send=True, reason="no_prior_alert")

        previous_sent_at_ms = int(previous_event["sent_at_ms"])
        if now_ms - previous_sent_at_ms > self._cooldown_ms:
            return AlertDecision(should_send=True, reason="cooldown_expired")

        if self._meaningful_improvement(candidate, previous_event):
            return AlertDecision(should_send=True, reason="meaningful_improvement")
        return AlertDecision(should_send=False, reason="dedupe_within_cooldown")

    def _meaningful_improvement(self, candidate: AlertCandidate, previous_event: dict[str, object]) -> bool:
        if self._execution_rank(candidate.execution_mode) > self._execution_rank(previous_event.get("execution_mode")):
            return True

        if (
            candidate.replay_passes_min_trade_gate is True
            and previous_event.get("replay_passes_min_trade_gate") is False
        ):
            return True

        previous_replay_net = self._to_float(previous_event.get("replay_net_after_cost_bps"))
        current_replay_net = self._to_float(candidate.replay_net_after_cost_bps)
        if (
            previous_replay_net is not None
            and current_replay_net is not None
            and current_replay_net - previous_replay_net >= self._replay_improvement_bps
        ):
            return True

        previous_final_position_pct = self._to_float(previous_event.get("final_position_pct"))
        current_final_position_pct = self._to_float(candidate.final_position_pct)
        if (
            previous_final_position_pct is not None
            and current_final_position_pct is not None
            and current_final_position_pct - previous_final_position_pct >= self._final_position_improvement_pct
        ):
            return True

        return False

    @staticmethod
    def _execution_rank(execution_mode: object) -> int:
        ranks = {
            "paper": 1,
            "small_probe": 2,
            "normal": 3,
            "size_up": 4,
            "extended_size_up": 5,
        }
        if not isinstance(execution_mode, str):
            return 0
        return ranks.get(execution_mode, 0)

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        return float(value)
