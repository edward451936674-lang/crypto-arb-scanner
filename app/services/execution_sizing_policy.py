from dataclasses import dataclass, field

from app.models.market import Opportunity


@dataclass(frozen=True)
class ExecutionAccountInputs:
    extended_size_up_enabled: bool
    live_target_leverage: float
    live_max_allowed_leverage: float
    live_required_liquidation_buffer_pct: float
    live_remaining_total_cap_pct: float
    live_remaining_symbol_cap_pct: float
    live_remaining_long_exchange_cap_pct: float
    live_remaining_short_exchange_cap_pct: float


@dataclass(frozen=True)
class ExecutionSizingDecision:
    extended_size_up_execution_ready: bool
    extended_size_up_execution_blockers: list[str] = field(default_factory=list)
    execution_max_single_cap_pct: float = 0.0
    execution_cap_reasons: list[str] = field(default_factory=list)


class ExecutionSizingPolicyEvaluator:
    @staticmethod
    def evaluate(opportunity: Opportunity, account_inputs: ExecutionAccountInputs) -> ExecutionSizingDecision:
        blockers: list[str] = []
        if not opportunity.extended_size_up_risk_eligible:
            blockers.append("extended_size_up_risk_not_eligible")
        if opportunity.execution_mode != "size_up":
            blockers.append("not_in_size_up_mode")
        if not account_inputs.extended_size_up_enabled:
            blockers.append("extended_size_up_not_enabled_in_execution_policy")
        if account_inputs.live_target_leverage > 2.0:
            blockers.append("live_target_leverage_too_high")
        if account_inputs.live_max_allowed_leverage > 2.0:
            blockers.append("live_max_allowed_leverage_too_high")
        if account_inputs.live_required_liquidation_buffer_pct < 28.0:
            blockers.append("live_liquidation_buffer_requirement_not_strict_enough")

        capacities = {
            "total": max(0.0, account_inputs.live_remaining_total_cap_pct),
            "symbol": max(0.0, account_inputs.live_remaining_symbol_cap_pct),
            "long_exchange": max(0.0, account_inputs.live_remaining_long_exchange_cap_pct),
            "short_exchange": max(0.0, account_inputs.live_remaining_short_exchange_cap_pct),
        }

        if len(blockers) == 0:
            if capacities["total"] < 0.08:
                blockers.append("insufficient_live_total_capacity_for_extended_size_up")
            if capacities["symbol"] < 0.08:
                blockers.append("insufficient_live_symbol_capacity_for_extended_size_up")
            if capacities["long_exchange"] < 0.08:
                blockers.append("insufficient_live_long_exchange_capacity_for_extended_size_up")
            if capacities["short_exchange"] < 0.08:
                blockers.append("insufficient_live_short_exchange_capacity_for_extended_size_up")

        extended_ready = len(blockers) == 0
        base_cap = 0.08 if extended_ready else 0.05
        execution_max_single_cap_pct = min(
            base_cap,
            capacities["total"],
            capacities["symbol"],
            capacities["long_exchange"],
            capacities["short_exchange"],
        )

        cap_reasons: list[str] = []
        if execution_max_single_cap_pct == base_cap:
            cap_reasons.append("capped_by_execution_base_cap")
        if execution_max_single_cap_pct == capacities["total"]:
            cap_reasons.append("capped_by_live_remaining_total")
        if execution_max_single_cap_pct == capacities["symbol"]:
            cap_reasons.append("capped_by_live_remaining_symbol")
        if execution_max_single_cap_pct == capacities["long_exchange"]:
            cap_reasons.append("capped_by_live_remaining_long_exchange")
        if execution_max_single_cap_pct == capacities["short_exchange"]:
            cap_reasons.append("capped_by_live_remaining_short_exchange")

        return ExecutionSizingDecision(
            extended_size_up_execution_ready=extended_ready,
            extended_size_up_execution_blockers=list(dict.fromkeys(blockers)),
            execution_max_single_cap_pct=execution_max_single_cap_pct,
            execution_cap_reasons=list(dict.fromkeys(cap_reasons)),
        )
