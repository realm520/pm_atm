from __future__ import annotations

import sqlite3
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from .orders import OrderRecord, OrderSide, OrderStatus, can_transition, order_from_row


class SqliteOrderStore:
    def __init__(self, db_path: str = "state/orders.db") -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=3000")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
              order_id TEXT PRIMARY KEY,
              client_order_id TEXT UNIQUE NOT NULL,
              event_id TEXT NOT NULL,
              asset_id TEXT NOT NULL,
              side TEXT NOT NULL,
              qty REAL NOT NULL,
              limit_price REAL NOT NULL,
              status TEXT NOT NULL,
              filled_qty REAL NOT NULL DEFAULT 0,
              avg_fill_price REAL,
              reject_reason TEXT NOT NULL DEFAULT '',
              exchange_order_id TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_id TEXT NOT NULL,
              qty REAL NOT NULL,
              price REAL NOT NULL,
              ts TEXT NOT NULL,
              FOREIGN KEY(order_id) REFERENCES orders(order_id)
            )
            """
        )
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return pd.Timestamp.now("UTC").isoformat()

    def close(self) -> None:
        self.conn.close()

    def get_by_client_order_id(self, client_order_id: str) -> OrderRecord | None:
        row = self.conn.execute("SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone()
        if not row:
            return None
        return order_from_row(dict(row))

    def get_order(self, order_id: str) -> OrderRecord | None:
        row = self.conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
        if not row:
            return None
        return order_from_row(dict(row))

    def create_order(
        self,
        *,
        client_order_id: str,
        event_id: str,
        asset_id: str,
        side: OrderSide,
        qty: float,
        limit_price: float,
        status: OrderStatus = OrderStatus.PENDING_SUBMIT,
        exchange_order_id: str = "",
    ) -> OrderRecord:
        existing = self.get_by_client_order_id(client_order_id)
        if existing is not None:
            return existing

        now = self._now()
        order = OrderRecord(
            order_id=uuid.uuid4().hex,
            client_order_id=client_order_id,
            event_id=event_id,
            asset_id=asset_id,
            side=side,
            qty=qty,
            limit_price=limit_price,
            status=status,
            filled_qty=0.0,
            avg_fill_price=None,
            exchange_order_id=exchange_order_id,
            created_at=now,
            updated_at=now,
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_id, client_order_id, event_id, asset_id, side, qty, limit_price, status,
                               filled_qty, avg_fill_price, reject_reason, exchange_order_id, created_at, updated_at)
            VALUES(:order_id, :client_order_id, :event_id, :asset_id, :side, :qty, :limit_price, :status,
                   :filled_qty, :avg_fill_price, :reject_reason, :exchange_order_id, :created_at, :updated_at)
            """,
            {
                **asdict(order),
                "side": order.side.value,
                "status": order.status.value,
            },
        )
        self.conn.commit()
        return order

    def transition_order(
        self,
        order_id: str,
        to_status: OrderStatus,
        *,
        filled_qty: float | None = None,
        avg_fill_price: float | None = None,
        reject_reason: str | None = None,
        exchange_order_id: str | None = None,
    ) -> OrderRecord:
        order = self.get_order(order_id)
        if order is None:
            raise ValueError(f"order not found: {order_id}")
        if order.status != to_status and not can_transition(order.status, to_status):
            raise ValueError(f"invalid transition {order.status.value} -> {to_status.value}")

        n_filled_qty = order.filled_qty if filled_qty is None else float(filled_qty)
        n_avg_fill = order.avg_fill_price if avg_fill_price is None else float(avg_fill_price)
        n_reject = order.reject_reason if reject_reason is None else reject_reason
        n_exchange_id = order.exchange_order_id if exchange_order_id is None else exchange_order_id
        now = self._now()

        self.conn.execute(
            """
            UPDATE orders
               SET status=?, filled_qty=?, avg_fill_price=?, reject_reason=?, exchange_order_id=?, updated_at=?
             WHERE order_id=?
            """,
            (to_status.value, n_filled_qty, n_avg_fill, n_reject, n_exchange_id, now, order_id),
        )
        self.conn.commit()
        updated = self.get_order(order_id)
        if updated is None:
            raise RuntimeError("order disappeared after update")
        return updated

    def add_fill(self, order_id: str, qty: float, price: float, ts: str | None = None) -> None:
        fill_ts = ts or self._now()
        self.conn.execute("INSERT INTO fills(order_id, qty, price, ts) VALUES(?,?,?,?)", (order_id, qty, price, fill_ts))
        self.conn.commit()

    def recent_orders(self, limit: int = 200) -> list[OrderRecord]:
        rows = self.conn.execute("SELECT * FROM orders ORDER BY updated_at DESC LIMIT ?", (int(limit),)).fetchall()
        return [order_from_row(dict(r)) for r in rows]

    def stats_last_minutes(self, minutes: int = 5) -> dict[str, float]:
        since = (pd.Timestamp.now("UTC") - pd.Timedelta(minutes=int(minutes))).isoformat()
        rows = self.conn.execute("SELECT status, COUNT(*) as c FROM orders WHERE updated_at >= ? GROUP BY status", (since,)).fetchall()
        counts: dict[str, int] = {str(r["status"]): int(r["c"]) for r in rows}
        total = float(sum(counts.values()))
        rejected = float(counts.get(OrderStatus.REJECTED.value, 0))
        failed = float(counts.get(OrderStatus.FAILED.value, 0))
        timeout = float(counts.get(OrderStatus.EXPIRED.value, 0))
        return {
            "total": total,
            "rejected": rejected,
            "failed": failed,
            "expired": timeout,
            "reject_rate": (rejected / total) if total > 0 else 0.0,
            "failure_rate": ((rejected + failed) / total) if total > 0 else 0.0,
            "timeout_rate": (timeout / total) if total > 0 else 0.0,
        }
