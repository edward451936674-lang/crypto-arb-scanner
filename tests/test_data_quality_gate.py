from app.models.market import MarketSnapshot
from app.services.data_quality_gate import MarketDataQualityGate


def _snapshot(
    *,
    exchange: str = "binance",
    base_symbol: str = "BTC",
    normalized_symbol: str = "BTC-USDT-PERP",
    instrument_id: str = "BTCUSDT",
    mark_price: float | None = 100.0,
    index_price: float | None = 100.0,
    last_price: float | None = 100.0,
    funding_rate: float | None = 0.0008,
    funding_period_hours: int | None = 8,
    funding_time_ms: int | None = 1_710_000_000_000,
    next_funding_time_ms: int | None = 1_710_028_800_000,
    timestamp_ms: int = 1_710_000_000_000,
    open_interest_usd: float | None = 20_000_000.0,
    quote_volume_24h_usd: float | None = 30_000_000.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=exchange,
        venue_type="dex" if exchange in {"hyperliquid", "lighter"} else "cex",
        base_symbol=base_symbol,
        normalized_symbol=normalized_symbol,
        instrument_id=instrument_id,
        mark_price=mark_price,
        index_price=index_price,
        last_price=last_price,
        funding_rate=funding_rate,
        funding_rate_source="current",
        funding_time_ms=funding_time_ms,
        next_funding_time_ms=next_funding_time_ms,
        funding_period_hours=funding_period_hours,
        timestamp_ms=timestamp_ms,
        open_interest_usd=open_interest_usd,
        quote_volume_24h_usd=quote_volume_24h_usd,
    )


def test_reject_missing_mark_price() -> None:
    now_ms = 1_710_000_100_000
    snapshot = _snapshot(timestamp_ms=now_ms).model_copy(
        update={"mark_price": None, "index_price": None, "last_price": None}
    )
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "missing_mark_price" in report.quality_blockers
    assert report.quality_status == "invalid"
    assert result.accepted_count == 0
    assert result.rejected_count == 1


def test_reject_non_positive_mark_price() -> None:
    now_ms = 1_710_000_100_000
    snapshot = _snapshot(mark_price=0.0, index_price=100.0, timestamp_ms=now_ms)
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "non_positive_mark_price" in report.quality_blockers
    assert report.quality_status == "invalid"


def test_reject_invalid_funding_period_for_exchange() -> None:
    now_ms = 1_710_000_100_000
    snapshot = _snapshot(exchange="binance", funding_period_hours=4, timestamp_ms=now_ms)
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "invalid_funding_period_for_exchange" in report.quality_blockers
    assert report.quality_status == "invalid"


def test_reject_next_funding_not_after_current() -> None:
    now_ms = 1_710_000_100_000
    snapshot = _snapshot(
        funding_time_ms=1_710_028_800_000,
        next_funding_time_ms=1_710_000_000_000,
        timestamp_ms=now_ms,
    )
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "next_funding_not_after_funding_time" in report.quality_blockers
    assert report.quality_status == "invalid"


def test_reject_hourly_funding_rate_mismatch() -> None:
    now_ms = 1_710_000_100_000
    snapshot = _snapshot(timestamp_ms=now_ms)
    snapshot.hourly_funding_rate = 0.1234
    snapshot.hourly_funding_rate_bps = 1234.0

    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])
    report = result.snapshot_reports[0]

    assert "hourly_funding_rate_mismatch" in report.quality_blockers
    assert "hourly_funding_rate_bps_mismatch" in report.quality_blockers
    assert report.quality_status == "invalid"


def test_stale_snapshot_warning() -> None:
    now_ms = 1_710_000_200_000
    snapshot = _snapshot(timestamp_ms=now_ms - 130_000)
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "timestamp_stale" in report.quality_warnings
    assert report.quality_status == "degraded"
    assert report.can_enter_scanner is True


def test_severe_stale_snapshot_rejection() -> None:
    now_ms = 1_710_000_500_000
    snapshot = _snapshot(timestamp_ms=now_ms - 310_000)
    result = MarketDataQualityGate(now_ms=now_ms).evaluate([snapshot])

    report = result.snapshot_reports[0]
    assert "timestamp_too_stale" in report.quality_blockers
    assert report.quality_status == "invalid"
    assert report.can_enter_scanner is False


def test_cross_exchange_price_outlier_warning() -> None:
    now_ms = 1_710_000_100_000
    snapshots = [
        _snapshot(exchange="binance", mark_price=100.0, timestamp_ms=now_ms),
        _snapshot(exchange="okx", instrument_id="BTC-USDT-SWAP", mark_price=100.0, timestamp_ms=now_ms),
        _snapshot(exchange="hyperliquid", instrument_id="BTC", mark_price=102.0, timestamp_ms=now_ms),
    ]
    result = MarketDataQualityGate(now_ms=now_ms).evaluate(snapshots)

    warnings = [warning for report in result.snapshot_reports for warning in report.quality_warnings]
    assert "cross_exchange_price_outlier" in warnings


def test_severe_cross_exchange_price_outlier_rejection() -> None:
    now_ms = 1_710_000_100_000
    snapshots = [
        _snapshot(exchange="binance", mark_price=100.0, timestamp_ms=now_ms),
        _snapshot(exchange="okx", instrument_id="BTC-USDT-SWAP", mark_price=100.0, timestamp_ms=now_ms),
        _snapshot(exchange="hyperliquid", instrument_id="BTC", mark_price=120.0, timestamp_ms=now_ms),
    ]
    result = MarketDataQualityGate(now_ms=now_ms).evaluate(snapshots)

    hyperliquid_report = next(report for report in result.snapshot_reports if report.exchange == "hyperliquid")
    assert "severe_cross_exchange_price_outlier" in hyperliquid_report.quality_blockers
    assert hyperliquid_report.quality_status == "invalid"


def test_accepted_snapshots_include_only_healthy_or_degraded() -> None:
    now_ms = 1_710_000_100_000
    healthy = _snapshot(exchange="binance", timestamp_ms=now_ms)
    degraded = _snapshot(exchange="okx", instrument_id="BTC-USDT-SWAP", timestamp_ms=now_ms - 130_000)
    invalid = _snapshot(exchange="hyperliquid", instrument_id="BTC", mark_price=-1.0, timestamp_ms=now_ms)

    result = MarketDataQualityGate(now_ms=now_ms).evaluate([healthy, degraded, invalid])

    assert result.accepted_count == 2
    assert result.rejected_count == 1
    statuses = [report.quality_status for report in result.snapshot_reports]
    assert "healthy" in statuses
    assert "degraded" in statuses
    assert "invalid" in statuses
    accepted_by_exchange = {snapshot.exchange: snapshot for snapshot in result.accepted_snapshots}
    assert accepted_by_exchange["binance"].data_quality_status == "healthy"
    assert accepted_by_exchange["okx"].data_quality_status == "degraded"
    assert accepted_by_exchange["binance"].data_quality_score is not None
    assert isinstance(accepted_by_exchange["binance"].data_quality_flags, list)
    assert accepted_by_exchange["okx"].data_quality_score is not None
    assert "timestamp_stale" in accepted_by_exchange["okx"].data_quality_flags


def test_suspicious_snapshot_rejected_from_scanner_admission() -> None:
    now_ms = 1_710_000_100_000
    suspicious = _snapshot(
        exchange="binance",
        mark_price=100.0,
        index_price=200.0,
        last_price=200.0,
        timestamp_ms=now_ms,
    )

    result = MarketDataQualityGate(now_ms=now_ms).evaluate([suspicious])
    report = result.snapshot_reports[0]

    assert report.quality_status == "suspicious"
    assert report.can_enter_scanner is False
    assert report.watchlist_only is True
    assert result.rejected_count == 1


def test_invalid_snapshot_rejected_from_scanner_admission() -> None:
    now_ms = 1_710_000_100_000
    invalid = _snapshot(timestamp_ms=now_ms).model_copy(
        update={"mark_price": None, "index_price": None, "last_price": None}
    )

    result = MarketDataQualityGate(now_ms=now_ms).evaluate([invalid])
    report = result.snapshot_reports[0]

    assert report.quality_status == "invalid"
    assert report.can_enter_scanner is False
    assert report.watchlist_only is False
    assert result.rejected_count == 1


def test_basis_checks_do_not_crash_when_mark_price_missing_and_index_present() -> None:
    now_ms = 1_710_000_100_000
    malformed = _snapshot(timestamp_ms=now_ms).model_copy(
        update={"mark_price": None, "index_price": 100.0, "last_price": 100.0}
    )

    result = MarketDataQualityGate(now_ms=now_ms).evaluate([malformed])
    report = result.snapshot_reports[0]

    assert report.quality_status == "invalid"
    assert "missing_mark_price" in report.quality_blockers
