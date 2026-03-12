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
