from fastapi import FastAPI, HTTPException, Query

from app.core.config import get_settings
from app.core.symbols import parse_symbols, supported_symbols
from app.models.market import OpportunitiesResponse, Opportunity
from app.services.arbitrage_scanner import ArbitrageScannerService
from app.services.data_quality_gate import MarketDataQualityGate
from app.services.execution_sizing_policy import (
    ExecutionSizingPolicyEvaluator,
    build_execution_account_inputs,
)
from app.services.market_data import MarketDataService

settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Cross-exchange perpetual market data collector for arbitrage discovery.",
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "env": settings.app_env}


@app.get("/api/v1/meta")
async def meta() -> dict[str, object]:
    return {
        "supported_symbols": supported_symbols(),
        "enabled_exchanges": {
            "binance": settings.enable_binance,
            "okx": settings.enable_okx,
            "hyperliquid": settings.enable_hyperliquid,
            "lighter": settings.enable_lighter,
        },
        "default_symbols": settings.default_symbols,
    }


@app.get("/api/v1/snapshots")
async def get_snapshots(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    )
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    service = MarketDataService(settings)

    try:
        result = await service.fetch_snapshots(requested_symbols)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result.model_dump()


@app.get("/api/v1/opportunities")
async def get_opportunities(
    symbols: str | None = Query(
        default=None,
        description="Comma separated base symbols, e.g. BTC,ETH,SOL",
    )
) -> dict[str, object]:
    requested_symbols = parse_symbols(symbols) if symbols else settings.default_symbols
    market_data_service = MarketDataService(settings)
    scanner = ArbitrageScannerService()
    quality_gate = MarketDataQualityGate()

    try:
        market_data = await market_data_service.fetch_snapshots(requested_symbols)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    quality_result = quality_gate.evaluate(market_data.snapshots)
    accepted_snapshots = quality_result.accepted_snapshots

    response = OpportunitiesResponse(
        requested_symbols=market_data.requested_symbols,
        opportunities=_hydrate_execution_sizing_outputs(scanner.build_opportunities(accepted_snapshots)),
        snapshot_errors=market_data.errors,
    )
    return response.model_dump()


def _hydrate_execution_sizing_outputs(opportunities: list[Opportunity]) -> list[Opportunity]:
    hydrated = []
    for opportunity in opportunities:
        decision = ExecutionSizingPolicyEvaluator.evaluate(
            opportunity=opportunity,
            account_inputs=build_execution_account_inputs(settings, opportunity),
        )
        hydrated.append(
            opportunity.model_copy(
                update={
                    "extended_size_up_execution_ready": decision.extended_size_up_execution_ready,
                    "extended_size_up_execution_blockers": decision.extended_size_up_execution_blockers,
                    "execution_max_single_cap_pct": decision.execution_max_single_cap_pct,
                    "execution_cap_reasons": decision.execution_cap_reasons,
                }
            )
        )
    return hydrated
