from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd

from .order_store import SqliteOrderStore
from .orders import ExecutionIntent, Fill, OrderRecord, OrderStatus


class ExchangeExecutionPort(Protocol):
    def place_order(self, intent: ExecutionIntent) -> tuple[str, OrderStatus, str]: ...
    def cancel_order(self, exchange_order_id: str) -> bool: ...
    def get_order_update(self, exchange_order_id: str) -> tuple[OrderStatus, float, float | None, list[Fill], str]: ...


@dataclass(frozen=True)
class ExecutionServiceConfig:
    order_timeout_sec: float = 15.0
    max_reject_rate_warn: float = 0.03
    max_reject_rate_crit: float = 0.08
    max_reject_rate_stop: float = 0.12
    max_consecutive_rejected_stop: int = 5


class ExecutionService:
    def __init__(self, store: SqliteOrderStore, exchange: ExchangeExecutionPort, config: ExecutionServiceConfig | None = None) -> None:
        self.store = store
        self.exchange = exchange
        self.cfg = config or ExecutionServiceConfig()
        self._submit_ts: dict[str, pd.Timestamp] = {}
        self._filled_seen: set[tuple[str, float, float, str]] = set()
        self._consecutive_rejected = 0

    def submit(self, intent: ExecutionIntent) -> OrderRecord:
        order = self.store.create_order(
            client_order_id=intent.client_order_id,
            event_id=intent.event_id,
            asset_id=intent.asset_id,
            side=intent.side,
            qty=intent.qty,
            limit_price=intent.limit_price,
            status=OrderStatus.PENDING_SUBMIT,
        )
        # idempotent
        if order.status != OrderStatus.PENDING_SUBMIT:
            return order

        try:
            exchange_order_id, status, reject_reason = self.exchange.place_order(intent)
            order = self.store.transition_order(
                order.order_id,
                status,
                reject_reason=reject_reason,
                exchange_order_id=exchange_order_id,
            )
            if status in {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}:
                self._submit_ts[order.order_id] = pd.Timestamp.now("UTC")
            if status == OrderStatus.REJECTED:
                self._consecutive_rejected += 1
            else:
                self._consecutive_rejected = 0
            return order
        except Exception as exc:
            self._consecutive_rejected += 1
            return self.store.transition_order(order.order_id, OrderStatus.FAILED, reject_reason=str(exc))

    def refresh(self, order: OrderRecord) -> OrderRecord:
        if order.status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED, OrderStatus.FAILED}:
            return order
        if not order.exchange_order_id:
            return order

        status, filled_qty, avg_fill_price, fills, reject_reason = self.exchange.get_order_update(order.exchange_order_id)
        for f in fills:
            key = (f.order_id, f.qty, f.price, f.ts)
            if key in self._filled_seen:
                continue
            self._filled_seen.add(key)
            self.store.add_fill(order.order_id, f.qty, f.price, f.ts)

        now = pd.Timestamp.now("UTC")
        submit_ts = self._submit_ts.get(order.order_id, now)
        age = (now - submit_ts).total_seconds()
        if age >= self.cfg.order_timeout_sec and status in {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}:
            self.store.transition_order(order.order_id, OrderStatus.CANCEL_REQUESTED, filled_qty=filled_qty, avg_fill_price=avg_fill_price)
            try:
                self.exchange.cancel_order(order.exchange_order_id)
                status = OrderStatus.CANCELED if filled_qty < order.qty else OrderStatus.FILLED
            except Exception:
                status = OrderStatus.FAILED

        order = self.store.transition_order(
            order.order_id,
            status,
            filled_qty=filled_qty,
            avg_fill_price=avg_fill_price,
            reject_reason=reject_reason,
        )
        if status == OrderStatus.REJECTED:
            self._consecutive_rejected += 1
        elif status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}:
            self._consecutive_rejected = 0
        return order

    def get_order_by_client_id(self, client_order_id: str) -> OrderRecord | None:
        return self.store.get_by_client_order_id(client_order_id)

    def refresh_recent(self, limit: int = 200) -> list[OrderRecord]:
        out: list[OrderRecord] = []
        for o in self.store.recent_orders(limit=limit):
            out.append(self.refresh(o))
        return out

    def risk_flags(self, minutes: int = 5) -> dict[str, bool | float | int]:
        s = self.store.stats_last_minutes(minutes)
        reject_rate = float(s["reject_rate"])
        return {
            "reject_rate": reject_rate,
            "reject_warn": reject_rate >= self.cfg.max_reject_rate_warn,
            "reject_crit": reject_rate >= self.cfg.max_reject_rate_crit,
            "hard_stop": reject_rate >= self.cfg.max_reject_rate_stop or self._consecutive_rejected >= self.cfg.max_consecutive_rejected_stop,
            "consecutive_rejected": self._consecutive_rejected,
        }
