from __future__ import annotations

from weather_arb.execution_service import ExecutionService, ExecutionServiceConfig
from weather_arb.exchange_sim import SimExchangeExecutor
from weather_arb.order_store import SqliteOrderStore
from weather_arb.orders import ExecutionIntent, OrderSide, OrderStatus


def test_sqlite_order_store_idempotent_create(tmp_path) -> None:
    db = tmp_path / "orders.db"
    store = SqliteOrderStore(str(db))
    o1 = store.create_order(
        client_order_id="cid-1",
        event_id="e1",
        asset_id="a1",
        side=OrderSide.BUY,
        qty=1.0,
        limit_price=0.51,
    )
    o2 = store.create_order(
        client_order_id="cid-1",
        event_id="e1",
        asset_id="a1",
        side=OrderSide.BUY,
        qty=1.0,
        limit_price=0.51,
    )
    assert o1.order_id == o2.order_id
    store.close()


def test_execution_service_submit_and_refresh_fill(tmp_path) -> None:
    store = SqliteOrderStore(str(tmp_path / "orders.db"))
    exchange = SimExchangeExecutor(fill_after_sec=0)
    svc = ExecutionService(store, exchange, ExecutionServiceConfig(order_timeout_sec=15.0))

    intent = ExecutionIntent(
        event_id="e1",
        asset_id="a1",
        side=OrderSide.BUY,
        qty=3.0,
        limit_price=0.43,
        client_order_id="cid-2",
    )
    order = svc.submit(intent)
    assert order.status == OrderStatus.NEW

    refreshed = svc.refresh(order)
    assert refreshed.status == OrderStatus.FILLED
    assert refreshed.filled_qty == 3.0
    assert refreshed.avg_fill_price is not None
    store.close()


def test_execution_service_timeout_cancel(tmp_path) -> None:
    store = SqliteOrderStore(str(tmp_path / "orders.db"))
    exchange = SimExchangeExecutor(fill_after_sec=999)
    svc = ExecutionService(store, exchange, ExecutionServiceConfig(order_timeout_sec=0.0))

    intent = ExecutionIntent(
        event_id="e1",
        asset_id="a1",
        side=OrderSide.SELL,
        qty=2.0,
        limit_price=0.62,
        client_order_id="cid-3",
    )
    order = svc.submit(intent)
    updated = svc.refresh(order)
    assert updated.status in {OrderStatus.CANCELED, OrderStatus.FILLED}
    store.close()
