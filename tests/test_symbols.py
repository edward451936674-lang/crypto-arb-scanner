from app.core.symbols import parse_symbols, resolve_symbol_specs, supported_symbols


def test_parse_symbols() -> None:
    assert parse_symbols("btc, eth ,SOL") == ["BTC", "ETH", "SOL"]


def test_supported_symbols() -> None:
    assert supported_symbols() == ["BTC", "ETH", "SOL"]


def test_resolve_symbol_specs() -> None:
    specs = resolve_symbol_specs(["BTC", "SOL"])
    assert specs[0].binance_symbol == "BTCUSDT"
    assert specs[1].okx_inst_id == "SOL-USDT-SWAP"
