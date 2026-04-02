from dataclasses import dataclass, field

from app.core.config import Settings
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


@dataclass(frozen=True)
class ExecutionPolicyProfile:
    extended_size_up_enabled: bool
    live_target_leverage: float
    live_max_allowed_leverage: float
    live_required_liquidation_buffer_pct: float
    live_remaining_total_cap_pct: float
    live_remaining_symbol_cap_pct: float
    live_remaining_long_exchange_cap_pct: float
    live_remaining_short_exchange_cap_pct: float


@dataclass(frozen=True)
class ExecutionAccountState:
    remaining_total_cap_pct: float | None = None
    remaining_symbol_cap_pct_by_symbol: dict[str, float] = field(default_factory=dict)
    remaining_long_exchange_cap_pct_by_exchange: dict[str, float] = field(default_factory=dict)
    remaining_short_exchange_cap_pct_by_exchange: dict[str, float] = field(default_factory=dict)


PAPER_CONSERVATIVE_PROFILE = ExecutionPolicyProfile(
    extended_size_up_enabled=False,
    live_target_leverage=1.0,
    live_max_allowed_leverage=1.5,
    live_required_liquidation_buffer_pct=30.0,
    live_remaining_total_cap_pct=0.05,
    live_remaining_symbol_cap_pct=0.05,
    live_remaining_long_exchange_cap_pct=0.05,
    live_remaining_short_exchange_cap_pct=0.05,
)

LIVE_CONSERVATIVE_PROFILE = ExecutionPolicyProfile(
    extended_size_up_enabled=False,
    live_target_leverage=1.0,
    live_max_allowed_leverage=1.5,
    live_required_liquidation_buffer_pct=35.0,
    live_remaining_total_cap_pct=0.05,
    live_remaining_symbol_cap_pct=0.03,
    live_remaining_long_exchange_cap_pct=0.05,
    live_remaining_short_exchange_cap_pct=0.05,
)


def resolve_execution_policy_profile_name(settings: Settings, profile_name: str) -> ExecutionPolicyProfile:
    if profile_name == "dev_default":
        return ExecutionPolicyProfile(
            extended_size_up_enabled=settings.execution_extended_size_up_enabled,
            live_target_leverage=settings.execution_live_target_leverage,
            live_max_allowed_leverage=settings.execution_live_max_allowed_leverage,
            live_required_liquidation_buffer_pct=settings.execution_live_required_liquidation_buffer_pct,
            live_remaining_total_cap_pct=settings.execution_live_remaining_total_cap_pct,
            live_remaining_symbol_cap_pct=settings.execution_live_remaining_symbol_cap_pct,
            live_remaining_long_exchange_cap_pct=settings.execution_live_remaining_long_exchange_cap_pct,
            live_remaining_short_exchange_cap_pct=settings.execution_live_remaining_short_exchange_cap_pct,
        )

    named_profiles = {
        "paper_conservative": PAPER_CONSERVATIVE_PROFILE,
        "live_conservative": LIVE_CONSERVATIVE_PROFILE,
    }
    if profile_name in named_profiles:
        return named_profiles[profile_name]

    raise ValueError(f"Unknown execution policy profile: {profile_name}")


def resolve_execution_policy_profile(settings: Settings) -> ExecutionPolicyProfile:
    return resolve_execution_policy_profile_name(settings, settings.execution_policy_profile)


def _cap_with_default(value: float, default_value: float) -> float:
    return value if value > 0.0 else default_value


def _resolve_cap_with_override_and_baseline(
    override_value: float | None,
    baseline_value: float,
    profile_default: float,
) -> float:
    if override_value is not None:
        return max(0.0, override_value)
    return _cap_with_default(baseline_value, profile_default)


def build_execution_account_inputs(
    settings: Settings,
    opportunity: Opportunity,
    account_state: ExecutionAccountState | None = None,
) -> ExecutionAccountInputs:
    return build_execution_account_inputs_for_profile(
        settings,
        opportunity,
        settings.execution_policy_profile,
        account_state,
    )


def build_execution_account_inputs_for_profile(
    settings: Settings,
    opportunity: Opportunity,
    profile_name: str,
    account_state: ExecutionAccountState | None = None,
) -> ExecutionAccountInputs:
    profile = resolve_execution_policy_profile_name(settings, profile_name)
    baseline_total_remaining_cap_pct = opportunity.remaining_total_cap_pct
    baseline_symbol_remaining_cap_pct = opportunity.remaining_symbol_cap_pct
    baseline_long_exchange_remaining_cap_pct = opportunity.remaining_long_exchange_cap_pct
    baseline_short_exchange_remaining_cap_pct = opportunity.remaining_short_exchange_cap_pct

    if account_state is not None:
        symbol_override = account_state.remaining_symbol_cap_pct_by_symbol.get(opportunity.symbol)
        long_exchange_override = account_state.remaining_long_exchange_cap_pct_by_exchange.get(opportunity.long_exchange)
        short_exchange_override = account_state.remaining_short_exchange_cap_pct_by_exchange.get(opportunity.short_exchange)
        total_remaining_cap_pct = _resolve_cap_with_override_and_baseline(
            account_state.remaining_total_cap_pct,
            baseline_total_remaining_cap_pct,
            profile.live_remaining_total_cap_pct,
        )
        symbol_remaining_cap_pct = _resolve_cap_with_override_and_baseline(
            symbol_override,
            baseline_symbol_remaining_cap_pct,
            profile.live_remaining_symbol_cap_pct,
        )
        long_exchange_remaining_cap_pct = _resolve_cap_with_override_and_baseline(
            long_exchange_override,
            baseline_long_exchange_remaining_cap_pct,
            profile.live_remaining_long_exchange_cap_pct,
        )
        short_exchange_remaining_cap_pct = _resolve_cap_with_override_and_baseline(
            short_exchange_override,
            baseline_short_exchange_remaining_cap_pct,
            profile.live_remaining_short_exchange_cap_pct,
        )
    else:
        total_remaining_cap_pct = _cap_with_default(baseline_total_remaining_cap_pct, profile.live_remaining_total_cap_pct)
        symbol_remaining_cap_pct = _cap_with_default(
            baseline_symbol_remaining_cap_pct,
            profile.live_remaining_symbol_cap_pct,
        )
        long_exchange_remaining_cap_pct = _cap_with_default(
            baseline_long_exchange_remaining_cap_pct,
            profile.live_remaining_long_exchange_cap_pct,
        )
        short_exchange_remaining_cap_pct = _cap_with_default(
            baseline_short_exchange_remaining_cap_pct,
            profile.live_remaining_short_exchange_cap_pct,
        )

    return ExecutionAccountInputs(
        extended_size_up_enabled=profile.extended_size_up_enabled,
        live_target_leverage=profile.live_target_leverage,
        live_max_allowed_leverage=profile.live_max_allowed_leverage,
        live_required_liquidation_buffer_pct=profile.live_required_liquidation_buffer_pct,
        live_remaining_total_cap_pct=total_remaining_cap_pct,
        live_remaining_symbol_cap_pct=symbol_remaining_cap_pct,
        live_remaining_long_exchange_cap_pct=long_exchange_remaining_cap_pct,
        live_remaining_short_exchange_cap_pct=short_exchange_remaining_cap_pct,
    )


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
