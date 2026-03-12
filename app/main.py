from fastapi import FastAPI, HTTPException, Query

from app.core.config import get_settings
from app.core.symbols import parse_symbols, supported_symbols
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
