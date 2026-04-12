from __future__ import annotations

from app.models.execution import ExecutionCandidate, QuantityResolutionResult


TARGET_NOTIONAL_AND_REFERENCE_PRICE = "target_notional_and_reference_price"
TARGET_POSITION_PCT_ONLY = "target_position_pct_only"
UNAVAILABLE = "unavailable"


class ExecutionQuantityResolver:
    """Conservative resolver for executable quantity previews.

    This resolver is intentionally non-live and only fills quantity when
    planning fields provide enough deterministic information.
    """

    def resolve(self, candidate: ExecutionCandidate) -> QuantityResolutionResult:
        warnings: list[str] = []
        target_notional = candidate.target_notional_usd

        if target_notional is None:
            if candidate.target_position_pct is not None:
                return QuantityResolutionResult(
                    quantity_resolution_status="unavailable",
                    quantity_resolution_source=TARGET_POSITION_PCT_ONLY,
                    warnings=["target_position_pct_requires_account_context"],
                    notes="preview_only_quantity_not_fabricated_from_target_position_pct",
                )
            return QuantityResolutionResult(
                quantity_resolution_status="unavailable",
                quantity_resolution_source=UNAVAILABLE,
                warnings=["target_notional_usd_missing"],
                notes="preview_only_quantity_unavailable",
            )

        long_quantity = self._resolve_leg_quantity(
            target_notional,
            candidate.entry_reference_price_long,
            warnings,
            leg="long",
        )
        short_quantity = self._resolve_leg_quantity(
            target_notional,
            candidate.entry_reference_price_short,
            warnings,
            leg="short",
        )

        if long_quantity is not None and short_quantity is not None:
            status = "resolved"
        elif long_quantity is not None or short_quantity is not None:
            status = "partial"
        else:
            status = "unavailable"

        return QuantityResolutionResult(
            resolved_quantity_long=long_quantity,
            resolved_quantity_short=short_quantity,
            quantity_resolution_status=status,
            quantity_resolution_source=TARGET_NOTIONAL_AND_REFERENCE_PRICE,
            warnings=warnings,
            notes="preview_only_quantity_resolution_v1",
        )

    @staticmethod
    def _resolve_leg_quantity(
        target_notional_usd: float,
        reference_price: float | None,
        warnings: list[str],
        *,
        leg: str,
    ) -> float | None:
        if reference_price is None:
            warnings.append(f"{leg}_reference_price_missing")
            return None
        if reference_price <= 0:
            warnings.append(f"{leg}_reference_price_non_positive")
            return None
        return target_notional_usd / reference_price


quantity_resolver = ExecutionQuantityResolver()
