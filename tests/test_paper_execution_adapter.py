import asyncio

from app.execution_adapters.paper import PaperExecutionAdapter


async def _exercise_adapter() -> dict[str, object]:
    adapter = PaperExecutionAdapter()
    placed_1 = await adapter.place_order(symbol="BTC", side="buy", quantity=1.25, price=30000.0)
    placed_2 = await adapter.place_order(symbol="BTC", side="buy", quantity=1.25, price=30000.0)
    cancelled = await adapter.cancel_order(order_id=placed_1["order_id"])
    status = await adapter.get_order_status(order_id=placed_1["order_id"])
    position = await adapter.get_position(symbol="BTC")
    balance = await adapter.get_balance(asset="USDT")
    return {
        "placed_1": placed_1,
        "placed_2": placed_2,
        "cancelled": cancelled,
        "status": status,
        "position": position,
        "balance": balance,
    }


def test_paper_execution_adapter_is_deterministic_and_non_live() -> None:
    payload = asyncio.run(_exercise_adapter())

    placed_1 = payload["placed_1"]
    placed_2 = payload["placed_2"]
    assert placed_1["order_id"] == placed_2["order_id"]
    assert placed_1["status"] == "accepted"
    assert placed_1["is_live"] is False

    assert payload["cancelled"]["status"] == "cancelled"
    assert payload["cancelled"]["is_live"] is False

    assert payload["status"]["status"] == "filled"
    assert payload["status"]["is_live"] is False

    assert payload["position"] == {
        "venue": "paper",
        "symbol": "BTC",
        "size": 0.0,
        "entry_price": None,
        "is_live": False,
    }
    assert payload["balance"] == {
        "venue": "paper",
        "asset": "USDT",
        "free": 100000.0,
        "locked": 0.0,
        "is_live": False,
    }
