from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "crypto-arb-scanner"
    app_env: str = "dev"
    request_timeout_seconds: float = 10.0
    default_symbols_csv: str = "BTC,ETH,SOL"

    enable_binance: bool = True
    enable_okx: bool = True
    enable_hyperliquid: bool = True
    enable_lighter: bool = True

    binance_base_url: str = "https://fapi.binance.com"
    okx_base_url: str = "https://www.okx.com"
    okx_ws_url: str = "wss://ws.okx.com:8443/ws/v5/public"
    okx_ws_timeout_seconds: float = 5.0

    hyperliquid_info_url: str = "https://api.hyperliquid.xyz/info"
    hyperliquid_dex: str = ""

    lighter_ws_url: str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
    lighter_markets_url: str = "https://explorer.elliot.ai/api/markets"
    lighter_ws_timeout_seconds: float = 5.0

    execution_extended_size_up_enabled: bool = True
    execution_policy_profile: str = "dev_default"
    execution_live_target_leverage: float = 1.5
    execution_live_max_allowed_leverage: float = 2.0
    execution_live_required_liquidation_buffer_pct: float = 28.0
    execution_live_remaining_total_cap_pct: float = 0.08
    execution_live_remaining_symbol_cap_pct: float = 0.08
    execution_live_remaining_long_exchange_cap_pct: float = 0.08
    execution_live_remaining_short_exchange_cap_pct: float = 0.08

    execution_account_state_provider: str = "null"
    execution_account_state_fixture_scenario: str = "roomy"
    execution_account_state_fixture_remaining_total_cap_pct: float | None = None
    execution_account_state_fixture_remaining_symbol_cap_pct: float | None = None
    execution_account_state_fixture_remaining_long_exchange_cap_pct: float | None = None
    execution_account_state_fixture_remaining_short_exchange_cap_pct: float | None = None
    execution_account_state_enabled: bool = False
    execution_account_state_fixture_total_notional_usd: float | None = None
    execution_account_state_fixture_remaining_total_notional_usd: float | None = None
    execution_account_state_fixture_remaining_symbol_notional_usd: dict[str, float] = {}
    execution_account_state_fixture_remaining_long_exchange_notional_usd: dict[str, float] = {}
    execution_account_state_fixture_remaining_short_exchange_notional_usd: dict[str, float] = {}
    execution_credential_readiness_enabled: bool = False
    execution_credential_fixture_configured_venues: dict[str, bool] = {}
    execution_policy_execution_enabled: bool = False
    execution_policy_allow_test_execution: bool = False
    execution_policy_allowed_venues: list[str] = ["binance", "okx", "hyperliquid", "lighter"]
    execution_policy_allowed_symbols: list[str] = []
    execution_policy_blocked_symbols: list[str] = []
    execution_policy_max_target_notional_usd: float | None = None
    live_execution_enabled: bool = False
    live_execution_allowed_venues: list[str] = []
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    observations_db_path: str = "./data/observations.sqlite3"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ARB_",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def default_symbols(self) -> list[str]:
        return [item.strip().upper() for item in self.default_symbols_csv.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
