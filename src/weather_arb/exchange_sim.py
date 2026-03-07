from __future__ import annotations

import uuid
from dataclasses import dataclass

import pandas as pd

from .orders import ExecutionIntent, Fill, OrderStatus


@dataclass
class SimOrder:
    exchange_order_id: str
    status: OrderStatus
    qty: float
    filled_qty: float
    avg_fill_price: float | None
    created_at: pd.Timestamp
    updated_at: pd.Timestamp


class SimExchangeExecutor:
    """Simple deterministic executor for integration tests and dry-run real flow."""

    def __init__(self, fill_after_sec: float = 0.0) -> None:
        self.fill_after_sec = max(0.0, float(fill_after_sec))
        self.orders: dict[str, SimOrder] = {}

    def place_order(self, intent: ExecutionIntent) -> tuple[str, OrderStatus, str]:
        if intent.qty <= 0:
            return "", OrderStatus.REJECTED, "qty<=0"
        if not (0.0 < intent.limit_price < 1.0):
            return "", OrderStatus.REJECTED, "invalid_price"

        oid = uuid.uuid4().hex
        now = pd.Timestamp.now("UTC")
        self.orders[oid] = SimOrder(
            exchange_order_id=oid,
            status=OrderStatus.NEW,
            qty=float(intent.qty),
            filled_qty=0.0,
            avg_fill_price=None,
            created_at=now,
            updated_at=now,
        )
        return oid, OrderStatus.NEW, ""

    def cancel_order(self, exchange_order_id: str) -> bool:
        o = self.orders.get(exchange_order_id)
        if o is None:
            return False
        if o.status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.FAILED, OrderStatus.EXPIRED}:
            return True
        o.status = OrderStatus.CANCELED
        o.updated_at = pd.Timestamp.now("UTC")
        return True

    def get_order_update(self, exchange_order_id: str) -> tuple[OrderStatus, float, float | None, list[Fill], str]:
        o = self.orders.get(exchange_order_id)
        if o is None:
            return OrderStatus.FAILED, 0.0, None, [], "order_not_found"

        now = pd.Timestamp.now("UTC")
        fills: list[Fill] = []
        age = (now - o.created_at).total_seconds()
        if o.status == OrderStatus.NEW and age >= self.fill_after_sec:
            o.status = OrderStatus.FILLED
            o.filled_qty = o.qty
            o.avg_fill_price = 0.5
            o.updated_at = now
            fills.append(Fill(order_id=exchange_order_id, qty=o.qty, price=0.5, ts=now.isoformat()))

        return o.status, o.filled_qty, o.avg_fill_price, fills, ""
