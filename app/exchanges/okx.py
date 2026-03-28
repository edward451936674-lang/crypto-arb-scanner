from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets

from app.core.symbols import SymbolSpec
from app.models.market import MarketSnapshot

from .base import ExchangeClient, ExchangeClientError


class OkxClient(ExchangeClient):
    name = "okx"
    venue_type = "cex"

    async def fetch_snapshots(self, specs: list[SymbolSpec]) -> list[MarketSnapshot]:
        mark_tasks = [self._fetch_mark_price(spec) for spec in specs]
        mark_payloads = await asyncio.gather(*mark_tasks)
        mark_by_inst = {payload["instId"]: payload for payload in mark_payloads}

        funding_by_inst: dict[str, dict[str, Any]] = {}
        try:
            funding_by_inst = await self._fetch_current_funding_ws(specs)
        except Exception:
            funding_by_inst = {}

        missing_specs = [spec for spec in specs if spec.okx_inst_id not in funding_by_inst]
        if missing_specs:
            fallback_tasks = [self._fetch_funding_rate_history(spec) for spec in missing_specs]
            fallback_payloads = await asyncio.gather(*fallback_tasks)
            for payload in fallback_payloads:
                funding_by_inst[payload["instId"]] = payload

        snapshots: list[MarketSnapshot] = []
        for spec in specs:
            mark_payload = mark_by_inst.get(spec.okx_inst_id)
            funding_payload = funding_by_inst.get(spec.okx_inst_id)
            if mark_payload is None:
                raise ExchangeClientError(f"Missing OKX mark price for {spec.okx_inst_id}")

            timestamp_ms = self._to_int(mark_payload.get("ts")) or int(time.time() * 1000)
            funding_rate = None
            funding_source = None
            funding_time_ms = None
            next_funding_time_ms = None
            if funding_payload is not None:
                if funding_payload.get("source") == "history_fallback":
                    funding_rate = self._to_float(funding_payload.get("realizedRate"))
                    if funding_rate is None:
                        funding_rate = self._to_float(funding_payload.get("fundingRate"))
                    funding_source = "last_settled_fallback"
                    funding_time_ms = self._to_int(funding_payload.get("fundingTime"))
                else:
                    funding_rate = self._to_float(funding_payload.get("fundingRate"))
                    funding_source = "current"
                    funding_time_ms = self._to_int(funding_payload.get("fundingTime"))
                    next_funding_time_ms = self._to_int(funding_payload.get("nextFundingTime"))
                    timestamp_ms = self._to_int(funding_payload.get("ts")) or timestamp_ms

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    venue_type=self.venue_type,
                    base_symbol=spec.base_symbol,
                    normalized_symbol=spec.normalized_symbol,
                    instrument_id=spec.okx_inst_id,
                    mark_price=float(mark_payload["markPx"]),
                    index_price=None,
                    funding_rate=funding_rate,
                    funding_rate_source=funding_source,
                    funding_time_ms=funding_time_ms,
                    next_funding_time_ms=next_funding_time_ms,
                    funding_period_hours=8,
                    timestamp_ms=timestamp_ms,
                    raw={"mark": mark_payload, "funding": funding_payload or {}},
                )
            )

        return snapshots

    async def _fetch_mark_price(self, spec: SymbolSpec) -> dict[str, Any]:
        response = await self.http.get(
            f"{self.settings.okx_base_url}/api/v5/public/mark-price",
            params={"instType": "SWAP", "instId": spec.okx_inst_id},
        )
        response.raise_for_status()
        payload = response.json()
        data = self._require_list(payload, "data")
        if not data:
            raise ExchangeClientError(f"Empty OKX mark price response for {spec.okx_inst_id}")
        item = data[0]
        if not isinstance(item, dict):
            raise ExchangeClientError("Unexpected OKX mark price item shape")
        return item

    async def _fetch_current_funding_ws(self, specs: list[SymbolSpec]) -> dict[str, dict[str, Any]]:
        args = [{"channel": "funding-rate", "instId": spec.okx_inst_id} for spec in specs]
        pending = {spec.okx_inst_id for spec in specs}
        collected: dict[str, dict[str, Any]] = {}

        async with websockets.connect(self.settings.okx_ws_url, ping_interval=20, close_timeout=1) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            deadline = time.monotonic() + self.settings.okx_ws_timeout_seconds

            while pending and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                raw_message = await asyncio.wait_for(ws.recv(), timeout=max(remaining, 0.1))
                message = json.loads(raw_message)
                if message.get("event") == "error":
                    raise ExchangeClientError(f"OKX websocket error: {message}")
                if message.get("arg", {}).get("channel") != "funding-rate":
                    continue
                data = message.get("data")
                if not isinstance(data, list) or not data:
                    continue
                item = data[0]
                inst_id = item.get("instId")
                if not isinstance(inst_id, str):
                    continue
                collected[inst_id] = item
                pending.discard(inst_id)

        return collected

    async def _fetch_funding_rate_history(self, spec: SymbolSpec) -> dict[str, Any]:
        response = await self.http.get(
            f"{self.settings.okx_base_url}/api/v5/public/funding-rate-history",
            params={"instId": spec.okx_inst_id, "limit": 1},
        )
        response.raise_for_status()
        payload = response.json()
        data = self._require_list(payload, "data")
        if not data:
            raise ExchangeClientError(f"Empty OKX funding history response for {spec.okx_inst_id}")
        item = data[0]
        if not isinstance(item, dict):
            raise ExchangeClientError("Unexpected OKX funding history item shape")
        item["source"] = "history_fallback"
        return item
