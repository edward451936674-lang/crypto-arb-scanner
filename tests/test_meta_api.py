import asyncio

from app.core.config import Settings
from app.main import meta


async def _run_meta_with_settings(monkeypatch, settings: Settings) -> dict[str, object]:
    monkeypatch.setattr("app.main.settings", settings)
    return await meta()


def test_meta_includes_execution_policy_for_dev_default(monkeypatch) -> None:
    settings = Settings(
        execution_policy_profile="dev_default",
        execution_extended_size_up_enabled=False,
        execution_live_target_leverage=1.25,
        execution_live_max_allowed_leverage=1.75,
        execution_live_required_liquidation_buffer_pct=33.0,
        execution_live_remaining_total_cap_pct=0.2,
        execution_live_remaining_symbol_cap_pct=0.19,
        execution_live_remaining_long_exchange_cap_pct=0.18,
        execution_live_remaining_short_exchange_cap_pct=0.17,
    )

    response = asyncio.run(_run_meta_with_settings(monkeypatch, settings))

    assert response["execution_policy_profile"] == "dev_default"
    assert response["execution_account_state_provider"] == "null"
    assert response["execution_account_state_fixture_scenario"] == "roomy"
    assert response["execution_account_state_resolved"] is None
    assert response["execution_policy_resolved"] == {
        "extended_size_up_enabled": False,
        "live_target_leverage": 1.25,
        "live_max_allowed_leverage": 1.75,
        "live_required_liquidation_buffer_pct": 33.0,
        "live_remaining_total_cap_pct": 0.2,
        "live_remaining_symbol_cap_pct": 0.19,
        "live_remaining_long_exchange_cap_pct": 0.18,
        "live_remaining_short_exchange_cap_pct": 0.17,
    }


def test_meta_includes_execution_policy_for_named_profile(monkeypatch) -> None:
    response = asyncio.run(
        _run_meta_with_settings(
            monkeypatch,
            Settings(execution_policy_profile="paper_conservative"),
        )
    )

    assert response["execution_policy_profile"] == "paper_conservative"
    assert response["execution_policy_resolved"] == {
        "extended_size_up_enabled": False,
        "live_target_leverage": 1.0,
        "live_max_allowed_leverage": 1.5,
        "live_required_liquidation_buffer_pct": 30.0,
        "live_remaining_total_cap_pct": 0.05,
        "live_remaining_symbol_cap_pct": 0.05,
        "live_remaining_long_exchange_cap_pct": 0.05,
        "live_remaining_short_exchange_cap_pct": 0.05,
    }


def test_meta_includes_fixed_fixture_execution_account_state(monkeypatch) -> None:
    response = asyncio.run(
        _run_meta_with_settings(
            monkeypatch,
            Settings(
                execution_account_state_provider="fixed_fixture",
                execution_account_state_fixture_scenario="exhausted",
                execution_account_state_fixture_remaining_total_cap_pct=0.02,
            ),
        )
    )

    assert response["execution_account_state_provider"] == "fixed_fixture"
    assert response["execution_account_state_fixture_scenario"] == "exhausted"
    assert response["execution_account_state_resolved"] == {
        "remaining_total_cap_pct": 0.02,
        "remaining_symbol_cap_pct": 0.0,
        "remaining_long_exchange_cap_pct": 0.0,
        "remaining_short_exchange_cap_pct": 0.0,
    }
