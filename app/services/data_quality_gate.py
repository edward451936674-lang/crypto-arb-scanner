from __future__ import annotations

import statistics
import time

from app.models.data_quality import DataQualityGateResult, SnapshotQualityReport
from app.models.market import MarketSnapshot
from app.services.data_quality_rules import (
    CROSS_EXCHANGE_PRICE_BLOCK,
    CROSS_EXCHANGE_PRICE_SUSPICIOUS,
    CROSS_EXCHANGE_PRICE_WARNING,
    EXPECTED_FUNDING_PERIOD_HOURS,
    FUNDING_WINDOW_TOLERANCE_HOURS,
    LAST_MARK_BASIS_SUSPICIOUS,
    LAST_MARK_BASIS_WARNING,
    MARK_INDEX_BASIS_SUSPICIOUS,
    MARK_INDEX_BASIS_WARNING,
    SNAPSHOT_STALE_BLOCK_MS,
    SNAPSHOT_STALE_WARNING_MS,
)

FLOAT_TOLERANCE = 1e-9


class MarketDataQualityGate:
    def __init__(self, now_ms: int | None = None) -> None:
        self._fixed_now_ms = now_ms

    def evaluate(self, snapshots: list[MarketSnapshot]) -> DataQualityGateResult:
        now_ms = self._now_ms()
        cross_exchange_flags = self._cross_exchange_flags(snapshots)

        reports: list[SnapshotQualityReport] = []
        accepted: list[MarketSnapshot] = []
        rejected: list[MarketSnapshot] = []

        for snapshot in snapshots:
            report = self._evaluate_snapshot(snapshot, now_ms, cross_exchange_flags.get(id(snapshot), []))
            reports.append(report)
            snapshot_with_quality = snapshot.model_copy(
                update={
                    "data_quality_status": report.quality_status,
                    "data_quality_score": report.quality_score,
                    "data_quality_flags": report.quality_flags,
                }
            )
            if report.quality_status in {"healthy", "degraded"}:
                accepted.append(snapshot_with_quality)
            else:
                rejected.append(snapshot_with_quality)

        healthy_count = sum(1 for report in reports if report.quality_status == "healthy")
        degraded_count = sum(1 for report in reports if report.quality_status == "degraded")
        suspicious_count = sum(1 for report in reports if report.quality_status == "suspicious")
        invalid_count = sum(1 for report in reports if report.quality_status == "invalid")

        collection_flags: list[str] = []
        if invalid_count > 0:
            collection_flags.append("invalid_snapshots_present")
        if suspicious_count > 0:
            collection_flags.append("suspicious_snapshots_present")
        if degraded_count > 0:
            collection_flags.append("degraded_snapshots_present")

        if invalid_count > 0 or suspicious_count > 0:
            collection_status = "suspicious"
        elif degraded_count > 0:
            collection_status = "degraded"
        else:
            collection_status = "healthy"

        return DataQualityGateResult(
            accepted_snapshots=accepted,
            rejected_snapshots=rejected,
            snapshot_reports=reports,
            collection_status=collection_status,
            collection_flags=collection_flags,
            total_snapshots=len(snapshots),
            accepted_count=len(accepted),
            rejected_count=len(rejected),
            healthy_count=healthy_count,
            degraded_count=degraded_count,
            suspicious_count=suspicious_count,
            invalid_count=invalid_count,
        )

    def _evaluate_snapshot(
        self,
        snapshot: MarketSnapshot,
        now_ms: int,
        cross_exchange_flags: list[str],
    ) -> SnapshotQualityReport:
        blockers: list[str] = []
        warnings: list[str] = []

        self._structural_checks(snapshot, blockers)
        derived_ok = self._derived_checks(snapshot, blockers)
        exchange_rules_ok = self._exchange_rule_checks(snapshot, blockers)
        self._funding_time_checks(snapshot, blockers)

        freshness_ok = self._freshness_checks(snapshot, now_ms, blockers, warnings)
        self._basis_warning_checks(snapshot, warnings)

        cross_exchange_ok = True
        for flag in cross_exchange_flags:
            if flag == "severe_cross_exchange_price_outlier":
                blockers.append(flag)
                cross_exchange_ok = False
            else:
                warnings.append(flag)

        quality_score, quality_status = self._quality_score_and_status(blockers, warnings)
        can_enter_scanner, watchlist_only = self._scanner_policy(quality_status)

        return SnapshotQualityReport(
            exchange=snapshot.exchange,
            symbol=snapshot.base_symbol,
            normalized_symbol=snapshot.normalized_symbol,
            timestamp_ms=snapshot.timestamp_ms,
            quality_score=quality_score,
            quality_status=quality_status,
            quality_flags=list(dict.fromkeys(blockers + warnings)),
            quality_blockers=list(dict.fromkeys(blockers)),
            quality_warnings=list(dict.fromkeys(warnings)),
            can_enter_scanner=can_enter_scanner,
            watchlist_only=watchlist_only,
            freshness_ok=freshness_ok,
            derived_checks_ok=derived_ok,
            exchange_rule_checks_ok=exchange_rules_ok,
            cross_exchange_checks_ok=cross_exchange_ok,
        )

    @staticmethod
    def _structural_checks(snapshot: MarketSnapshot, blockers: list[str]) -> None:
        if not snapshot.exchange:
            blockers.append("missing_exchange")
        if not snapshot.base_symbol:
            blockers.append("missing_base_symbol")
        if not snapshot.normalized_symbol:
            blockers.append("missing_normalized_symbol")
        if not snapshot.instrument_id:
            blockers.append("missing_instrument_id")
        if snapshot.mark_price is None:
            blockers.append("missing_mark_price")
        elif snapshot.mark_price <= 0:
            blockers.append("non_positive_mark_price")
        if snapshot.funding_period_hours is None:
            blockers.append("missing_funding_period_hours")
        elif snapshot.funding_period_hours <= 0:
            blockers.append("non_positive_funding_period_hours")
        if snapshot.timestamp_ms is None:
            blockers.append("missing_timestamp_ms")
        elif snapshot.timestamp_ms <= 0:
            blockers.append("non_positive_timestamp_ms")
        if snapshot.open_interest_usd is not None and snapshot.open_interest_usd < 0:
            blockers.append("negative_open_interest_usd")
        if snapshot.quote_volume_24h_usd is not None and snapshot.quote_volume_24h_usd < 0:
            blockers.append("negative_quote_volume_24h_usd")

    @staticmethod
    def _derived_checks(snapshot: MarketSnapshot, blockers: list[str]) -> bool:
        ok = True
        if snapshot.funding_rate is not None and snapshot.funding_period_hours not in (None, 0):
            expected_hourly = snapshot.funding_rate / snapshot.funding_period_hours
            if snapshot.hourly_funding_rate is None or abs(snapshot.hourly_funding_rate - expected_hourly) > 1e-12:
                blockers.append("hourly_funding_rate_mismatch")
                ok = False
            expected_hourly_bps = expected_hourly * 10_000
            if (
                snapshot.hourly_funding_rate_bps is None
                or abs(snapshot.hourly_funding_rate_bps - expected_hourly_bps) > 1e-9
            ):
                blockers.append("hourly_funding_rate_bps_mismatch")
                ok = False
        return ok

    @staticmethod
    def _funding_time_checks(snapshot: MarketSnapshot, blockers: list[str]) -> None:
        if snapshot.funding_time_ms is None or snapshot.next_funding_time_ms is None:
            return
        if snapshot.next_funding_time_ms <= snapshot.funding_time_ms:
            blockers.append("next_funding_not_after_funding_time")
            return
        if snapshot.funding_period_hours in (None, 0):
            return
        expected_window_ms = snapshot.funding_period_hours * 60 * 60 * 1000
        tolerance_ms = FUNDING_WINDOW_TOLERANCE_HOURS * 60 * 60 * 1000
        actual_window_ms = snapshot.next_funding_time_ms - snapshot.funding_time_ms
        if abs(actual_window_ms - expected_window_ms) > tolerance_ms:
            blockers.append("funding_window_mismatch")

    @staticmethod
    def _exchange_rule_checks(snapshot: MarketSnapshot, blockers: list[str]) -> bool:
        expected = EXPECTED_FUNDING_PERIOD_HOURS.get(snapshot.exchange.lower()) if snapshot.exchange else None
        if expected is None:
            return True
        if snapshot.funding_period_hours != expected:
            blockers.append("invalid_funding_period_for_exchange")
            return False
        return True

    @staticmethod
    def _freshness_checks(
        snapshot: MarketSnapshot,
        now_ms: int,
        blockers: list[str],
        warnings: list[str],
    ) -> bool:
        if snapshot.timestamp_ms is None:
            return False
        age_ms = now_ms - snapshot.timestamp_ms
        if age_ms > SNAPSHOT_STALE_BLOCK_MS:
            blockers.append("timestamp_too_stale")
            return False
        if age_ms > SNAPSHOT_STALE_WARNING_MS:
            warnings.append("timestamp_stale")
            return False
        return True

    @staticmethod
    def _basis_warning_checks(snapshot: MarketSnapshot, warnings: list[str]) -> None:
        if snapshot.mark_price is None or snapshot.mark_price <= FLOAT_TOLERANCE:
            return

        if snapshot.index_price not in (None, 0):
            mark_index_basis = abs(snapshot.mark_price - snapshot.index_price) / abs(snapshot.index_price)
            if mark_index_basis >= MARK_INDEX_BASIS_SUSPICIOUS:
                warnings.append("abnormal_mark_index_basis_suspicious")
            elif mark_index_basis >= MARK_INDEX_BASIS_WARNING:
                warnings.append("abnormal_mark_index_basis")

        if snapshot.last_price not in (None, 0) and snapshot.mark_price > FLOAT_TOLERANCE:
            last_mark_basis = abs(snapshot.last_price - snapshot.mark_price) / abs(snapshot.mark_price)
            if last_mark_basis >= LAST_MARK_BASIS_SUSPICIOUS:
                warnings.append("abnormal_last_mark_basis_suspicious")
            elif last_mark_basis >= LAST_MARK_BASIS_WARNING:
                warnings.append("abnormal_last_mark_basis")

    @staticmethod
    def _cross_exchange_flags(snapshots: list[MarketSnapshot]) -> dict[int, list[str]]:
        grouped: dict[str, list[MarketSnapshot]] = {}
        flags_by_snapshot: dict[int, list[str]] = {}

        for snapshot in snapshots:
            grouped.setdefault(snapshot.normalized_symbol, []).append(snapshot)
            flags_by_snapshot[id(snapshot)] = []

        for symbol_snapshots in grouped.values():
            valid = [snapshot for snapshot in symbol_snapshots if snapshot.mark_price is not None and snapshot.mark_price > 0]
            if len(valid) < 2:
                for snapshot in symbol_snapshots:
                    flags_by_snapshot[id(snapshot)].append("insufficient_cross_exchange_peers")
                continue

            median_price = statistics.median(snapshot.mark_price for snapshot in valid)
            if median_price <= FLOAT_TOLERANCE:
                continue

            for snapshot in valid:
                deviation = abs(snapshot.mark_price - median_price) / median_price
                if deviation >= CROSS_EXCHANGE_PRICE_BLOCK:
                    flags_by_snapshot[id(snapshot)].append("severe_cross_exchange_price_outlier")
                elif deviation >= CROSS_EXCHANGE_PRICE_SUSPICIOUS:
                    flags_by_snapshot[id(snapshot)].append("cross_exchange_price_outlier_suspicious")
                elif deviation >= CROSS_EXCHANGE_PRICE_WARNING:
                    flags_by_snapshot[id(snapshot)].append("cross_exchange_price_outlier")

        return flags_by_snapshot

    @staticmethod
    def _quality_score_and_status(blockers: list[str], warnings: list[str]) -> tuple[float, str]:
        if blockers:
            return 0.0, "invalid"

        penalty = 0.0
        for warning in warnings:
            penalty += {
                "timestamp_stale": 0.20,
                "abnormal_mark_index_basis": 0.08,
                "abnormal_mark_index_basis_suspicious": 0.22,
                "abnormal_last_mark_basis": 0.08,
                "abnormal_last_mark_basis_suspicious": 0.22,
                "cross_exchange_price_outlier": 0.10,
                "cross_exchange_price_outlier_suspicious": 0.25,
                "insufficient_cross_exchange_peers": 0.05,
            }.get(warning, 0.05)

        quality_score = max(0.0, 1.0 - penalty)
        if quality_score >= 0.85:
            return quality_score, "healthy"
        if quality_score >= 0.60:
            return quality_score, "degraded"
        return quality_score, "suspicious"

    @staticmethod
    def _scanner_policy(quality_status: str) -> tuple[bool, bool]:
        if quality_status in {"healthy", "degraded"}:
            return True, False
        if quality_status == "suspicious":
            return False, True
        return False, False

    def _now_ms(self) -> int:
        if self._fixed_now_ms is not None:
            return self._fixed_now_ms
        return int(time.time() * 1000)
