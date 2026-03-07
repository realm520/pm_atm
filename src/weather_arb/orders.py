from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


TERMINAL_STATUSES: set[OrderStatus] = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
    OrderStatus.FAILED,
}


@dataclass(frozen=True)
class ExecutionIntent:
    event_id: str
    asset_id: str
    side: OrderSide
    qty: float
    limit_price: float
    timeout_sec: float = 15.0
    client_order_id: str = ""


@dataclass(frozen=True)
class Fill:
    order_id: str
    qty: float
    price: float
    ts: str


@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    client_order_id: str
    event_id: str
    asset_id: str
    side: OrderSide
    qty: float
    limit_price: float
    status: OrderStatus
    filled_qty: float
    avg_fill_price: float | None
    reject_reason: str = ""
    exchange_order_id: str = ""
    created_at: str = ""
    updated_at: str = ""


VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING_SUBMIT: {OrderStatus.NEW, OrderStatus.REJECTED, OrderStatus.FAILED},
    OrderStatus.NEW: {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCEL_REQUESTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED, OrderStatus.FAILED},
    OrderStatus.PARTIALLY_FILLED: {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCEL_REQUESTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.FAILED},
    OrderStatus.CANCEL_REQUESTED: {OrderStatus.CANCELED, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.FAILED},
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.EXPIRED: set(),
    OrderStatus.FAILED: set(),
}


def can_transition(src: OrderStatus, dst: OrderStatus) -> bool:
    return dst in VALID_TRANSITIONS.get(src, set())


def order_from_row(row: dict[str, Any]) -> OrderRecord:
    return OrderRecord(
        order_id=str(row["order_id"]),
        client_order_id=str(row["client_order_id"]),
        event_id=str(row["event_id"]),
        asset_id=str(row["asset_id"]),
        side=OrderSide(str(row["side"])),
        qty=float(row["qty"]),
        limit_price=float(row["limit_price"]),
        status=OrderStatus(str(row["status"])),
        filled_qty=float(row["filled_qty"]),
        avg_fill_price=float(row["avg_fill_price"]) if row["avg_fill_price"] is not None else None,
        reject_reason=str(row.get("reject_reason") or ""),
        exchange_order_id=str(row.get("exchange_order_id") or ""),
        created_at=str(row.get("created_at") or ""),
        updated_at=str(row.get("updated_at") or ""),
    )
