from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class TelegramNotifierConfig:
    bot_token: str
    chat_id: str


class TelegramNotifier:
    def __init__(self, config: TelegramNotifierConfig, *, timeout_seconds: float = 10.0) -> None:
        self._config = config
        self._timeout_seconds = timeout_seconds

    @property
    def is_configured(self) -> bool:
        return bool(self._config.bot_token and self._config.chat_id)

    @staticmethod
    def format_opportunity_alert(opportunity: dict[str, object]) -> str:
        symbol = str(opportunity.get("symbol", "?"))
        long_exchange = str(opportunity.get("long_exchange", "?"))
        short_exchange = str(opportunity.get("short_exchange", "?"))
        net_edge_bps = float(opportunity.get("net_edge_bps") or 0.0)
        execution_mode = str(opportunity.get("execution_mode", "unknown"))
        grade = str(opportunity.get("opportunity_grade", "unknown"))
        final_position_pct = float(opportunity.get("final_position_pct") or 0.0)
        data_quality_status = str(opportunity.get("data_quality_status", "unknown"))
        return (
            f"{symbol}: long {long_exchange} / short {short_exchange} | "
            f"net edge {net_edge_bps:.2f} bps | mode {execution_mode} | "
            f"grade {grade} | final pos {final_position_pct:.2%} | data quality {data_quality_status}"
        )

    async def send_text(self, text: str) -> bool:
        if not self.is_configured:
            return False

        url = f"https://api.telegram.org/bot{self._config.bot_token}/sendMessage"
        payload = {"chat_id": self._config.chat_id, "text": text}
        async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        return True
