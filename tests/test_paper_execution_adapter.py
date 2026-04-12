import asyncio

from app.models.execution import CancelIntent, OrderIntent
from app.execution_adapters.paper import PaperExecutionAdapter


async def _exercise_adapter() -> dict[str, object]:
    adapter = PaperExecutionAdapter()
    intent = OrderIntent(
        venue_id="paper",
        symbol="BTC",
        side="buy",
        order_type="limit",
        quantity=1.25,
        price=30000.0,
        client_order_id="cid-1",
        route_key="BTC:binance->okx",
    )
    placed_1 = await adapter.place_order(intent)
    placed_2 = await adapter.place_order(intent)
    cancelled = await adapter.cancel_order(
        CancelIntent(
            venue_id="paper",
            order_id=placed_1.order_status.order_id,
            symbol="BTC",
            route_key="BTC:binance->okx",
        )
    )
    status = await adapter.get_order_status(order_id=placed_1.order_status.order_id, symbol="BTC")
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
    assert placed_1.order_status.order_id == placed_2.order_status.order_id
    assert placed_1.order_status.status == "accepted"
    assert placed_1.is_live is False

    assert payload["cancelled"].order_status.status == "cancelled"
    assert payload["cancelled"].is_live is False

    assert payload["status"].status == "filled"
    assert payload["status"].is_live is False

    assert payload["position"].model_dump() == {
        "venue_id": "paper",
        "symbol": "BTC",
        "size": 0.0,
        "entry_price": None,
        "mark_price": None,
        "unrealized_pnl": None,
        "leverage": None,
        "route_key": None,
        "metadata": {"paper": True},
        "notes": None,
        "is_live": False,
    }
    assert payload["balance"].model_dump() == {
        "venue_id": "paper",
        "asset": "USDT",
        "free": 100000.0,
        "locked": 0.0,
        "equity": None,
        "available": None,
        "route_key": None,
        "metadata": {"paper": True},
        "notes": None,
        "is_live": False,
    }
