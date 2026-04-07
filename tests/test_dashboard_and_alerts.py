import asyncio

from fastapi.testclient import TestClient

from app import main as main_module
from app.main import app
from app.models.market import Opportunity, OpportunitiesResponse
from app.services.telegram_notifier import TelegramNotifier


def _opportunity(
    *,
    symbol: str = "BTC",
    long_exchange: str = "binance",
    short_exchange: str = "okx",
    net_edge_bps: float = 25.0,
    execution_mode: str = "normal",
    data_quality_status: str = "healthy",
) -> Opportunity:
    payload = {
        "symbol": symbol,
        "long_exchange": long_exchange,
        "short_exchange": short_exchange,
        "long_price": 100.0,
        "short_price": 101.0,
        "price_spread_abs": 1.0,
        "price_spread_bps": 99.0,
        "estimated_edge_bps": 24.0,
        "holding_hours": 8,
        "expected_funding_edge_bps": 2.0,
        "estimated_fee_bps": 6.0,
        "net_edge_bps": net_edge_bps,
        "funding_confidence_score": 0.9,
        "funding_confidence_label": "high",
        "risk_adjusted_edge_bps": 20.0,
        "data_quality_status": data_quality_status,
        "data_quality_score": 0.95,
        "opportunity_grade": "tradable",
        "is_tradable": True,
        "position_size_multiplier": 1.0,
        "suggested_position_pct": 0.04,
        "max_position_pct": 0.05,
        "execution_mode": execution_mode,
        "final_position_pct": 0.03,
    }
    return Opportunity.model_validate(payload)


def test_dashboard_route_returns_html(monkeypatch) -> None:
    async def fake_dashboard_rows(_: list[str]) -> list[main_module.DashboardRow]:
        return [
            main_module.DashboardRow(
                opportunity=_opportunity(),
                why_not_tradable="live candidate",
                replay_net_after_cost_bps=12.5,
                replay_confidence_label="high",
                replay_passes_min_trade_gate=True,
            )
        ]

    monkeypatch.setattr(main_module, "_build_dashboard_rows", fake_dashboard_rows)

    client = TestClient(app)
    response = client.get("/dashboard")

    assert response.status_code == 200
    body = response.text
    assert "symbol" in body
    assert "long_exchange" in body
    assert "estimated_net_edge_bps" in body
    assert "opportunity_grade" in body
    assert "execution_mode" in body
    assert "why_not_tradable" in body
    assert "replay_net_after_cost_bps" in body
    assert "replay_confidence_label" in body
    assert "replay_passes_min_trade_gate" in body
    assert "live candidate" in body
    assert "12.50" in body
    assert "high" in body
    assert "yes" in body


def test_why_not_tradable_label_scenarios() -> None:
    mixed_funding = _opportunity(execution_mode="paper")
    mixed_funding.risk_flags = ["mixed_funding_sources"]
    assert (
        main_module._why_not_tradable_label(
            opportunity=mixed_funding,
            replay_net_after_cost_bps=15.0,
            replay_passes_min_trade_gate=True,
        )
        == "mixed funding semantics"
    )

    period_mismatch = _opportunity(execution_mode="paper")
    period_mismatch.risk_flags = ["different_funding_periods"]
    assert (
        main_module._why_not_tradable_label(
            opportunity=period_mismatch,
            replay_net_after_cost_bps=9.0,
            replay_passes_min_trade_gate=False,
        )
        == "funding period mismatch"
    )

    weak_replay = _opportunity(execution_mode="normal")
    assert (
        main_module._why_not_tradable_label(
            opportunity=weak_replay,
            replay_net_after_cost_bps=4.0,
            replay_passes_min_trade_gate=False,
        )
        == "replay edge too weak after costs"
    )


def test_telegram_formatting_helper() -> None:
    message = TelegramNotifier.format_opportunity_alert(_opportunity().model_dump())
    assert "BTC" in message
    assert "long binance / short okx" in message
    assert "net edge" in message


def test_alert_route_filters_and_sends(monkeypatch) -> None:
    opportunities = [
        _opportunity(symbol="BTC", net_edge_bps=30.0, execution_mode="normal", data_quality_status="healthy"),
        _opportunity(symbol="ETH", net_edge_bps=8.0, execution_mode="normal", data_quality_status="healthy"),
        _opportunity(symbol="SOL", net_edge_bps=35.0, execution_mode="paper", data_quality_status="healthy"),
        _opportunity(symbol="XRP", net_edge_bps=22.0, execution_mode="normal", data_quality_status="invalid"),
    ]

    async def fake_build(_: list[str]) -> OpportunitiesResponse:
        return OpportunitiesResponse(requested_symbols=["BTC", "ETH"], opportunities=opportunities, snapshot_errors=[])

    sent: list[str] = []

    async def fake_send(self: TelegramNotifier, text: str) -> bool:
        sent.append(text)
        return True

    monkeypatch.setattr(main_module, "_build_opportunities_response", fake_build)
    monkeypatch.setattr(main_module.settings, "telegram_bot_token", "token")
    monkeypatch.setattr(main_module.settings, "telegram_chat_id", "chat")
    monkeypatch.setattr(TelegramNotifier, "send_text", fake_send)

    client = TestClient(app)
    response = client.post("/api/v1/alerts/telegram/opportunities", params={"min_net_edge_bps": 15, "top_n": 3})

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated"] == 4
    assert payload["sent"] == 1
    assert len(sent) == 1
    assert any(skip["reason"] == "below_min_net_edge" for skip in payload["skipped"])
    assert any(skip["reason"] == "non_live_execution_mode" for skip in payload["skipped"])
    assert any(skip["reason"] == "poor_data_quality" for skip in payload["skipped"])


def test_alert_route_requires_telegram_config(monkeypatch) -> None:
    async def fake_build(_: list[str]) -> OpportunitiesResponse:
        return OpportunitiesResponse(requested_symbols=["BTC"], opportunities=[_opportunity()], snapshot_errors=[])

    monkeypatch.setattr(main_module, "_build_opportunities_response", fake_build)
    monkeypatch.setattr(main_module.settings, "telegram_bot_token", "")
    monkeypatch.setattr(main_module.settings, "telegram_chat_id", "")

    client = TestClient(app)
    response = client.post("/api/v1/alerts/telegram/opportunities")
    assert response.status_code == 400


def test_build_dashboard_rows_includes_replay_details(monkeypatch) -> None:
    market_snapshot_payload = {
        "exchange": "binance",
        "venue_type": "cex",
        "base_symbol": "BTC",
        "normalized_symbol": "BTCUSDT",
        "instrument_id": "BTCUSDT",
        "mark_price": 100.0,
        "funding_rate": 0.0002,
        "funding_rate_source": "predicted",
        "funding_time_ms": 1_700_000_000_000,
        "next_funding_time_ms": 1_700_000_360_000,
        "funding_period_hours": 8,
        "timestamp_ms": 1_700_000_000_000,
    }
    long_snapshot = main_module.MarketSnapshot.model_validate(market_snapshot_payload)
    short_snapshot = main_module.MarketSnapshot.model_validate(
        {**market_snapshot_payload, "exchange": "okx", "funding_rate": 0.0005}
    )

    async def fake_context(_: list[str]) -> main_module._ScanContext:
        return main_module._ScanContext(
            requested_symbols=["BTC"],
            opportunities=[_opportunity()],
            snapshot_errors=[],
            accepted_snapshots=[long_snapshot, short_snapshot],
        )

    monkeypatch.setattr(main_module, "_build_scan_context", fake_context)

    rows = asyncio.run(main_module._build_dashboard_rows(["BTC"]))
    assert len(rows) == 1
    row = rows[0]
    assert row.replay_net_after_cost_bps is not None
    assert row.replay_confidence_label in {"high", "medium", "low"}
    assert row.replay_passes_min_trade_gate is not None
