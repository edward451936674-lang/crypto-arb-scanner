from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolSpec:
    base_symbol: str
    normalized_symbol: str
    binance_symbol: str
    okx_inst_id: str
    hyperliquid_coin: str
    lighter_symbol: str


SUPPORTED_SYMBOL_SPECS: dict[str, SymbolSpec] = {
    "BTC": SymbolSpec(
        base_symbol="BTC",
        normalized_symbol="BTC-USDT-PERP",
        binance_symbol="BTCUSDT",
        okx_inst_id="BTC-USDT-SWAP",
        hyperliquid_coin="BTC",
        lighter_symbol="BTC",
    ),
    "ETH": SymbolSpec(
        base_symbol="ETH",
        normalized_symbol="ETH-USDT-PERP",
        binance_symbol="ETHUSDT",
        okx_inst_id="ETH-USDT-SWAP",
        hyperliquid_coin="ETH",
        lighter_symbol="ETH",
    ),
    "SOL": SymbolSpec(
        base_symbol="SOL",
        normalized_symbol="SOL-USDT-PERP",
        binance_symbol="SOLUSDT",
        okx_inst_id="SOL-USDT-SWAP",
        hyperliquid_coin="SOL",
        lighter_symbol="SOL",
    ),
}


def parse_symbols(raw_symbols: str | None) -> list[str]:
    if not raw_symbols:
        return []
    return [item.strip().upper() for item in raw_symbols.split(",") if item.strip()]


def resolve_symbol_specs(symbols: list[str]) -> list[SymbolSpec]:
    resolved: list[SymbolSpec] = []
    unsupported: list[str] = []

    for symbol in symbols:
        spec = SUPPORTED_SYMBOL_SPECS.get(symbol.upper())
        if spec is None:
            unsupported.append(symbol)
        else:
            resolved.append(spec)

    if unsupported:
        raise ValueError(
            f"Unsupported symbols: {', '.join(unsupported)}. "
            f"Supported symbols: {', '.join(sorted(SUPPORTED_SYMBOL_SPECS))}."
        )

    return resolved


def supported_symbols() -> list[str]:
    return sorted(SUPPORTED_SYMBOL_SPECS)
