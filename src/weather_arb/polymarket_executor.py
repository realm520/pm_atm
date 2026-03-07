from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .execution_service import ExchangeExecutionPort
from .orders import ExecutionIntent, Fill, OrderStatus


@dataclass(frozen=True)
class PolymarketExecutionConfig:
    """Configurable REST adapter for live execution.

    Note: endpoint paths are intentionally configurable because deployment
    environments often use different gateways/signing proxies.
    """

    base_url: str = ""
    api_key: str = ""
    timeout_sec: float = 10.0
    place_order_path: str = "/orders"
    cancel_order_path_tmpl: str = "/orders/{exchange_order_id}/cancel"
    get_order_path_tmpl: str = "/orders/{exchange_order_id}"


_STATUS_MAP: dict[str, OrderStatus] = {
    "PENDING_SUBMIT": OrderStatus.PENDING_SUBMIT,
    "NEW": OrderStatus.NEW,
    "OPEN": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCEL_REQUESTED": OrderStatus.CANCEL_REQUESTED,
    "CANCELED": OrderStatus.CANCELED,
    "CANCELLED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
    "FAILED": OrderStatus.FAILED,
}


class PolymarketLiveExecutor(ExchangeExecutionPort):
    def __init__(self, config: PolymarketExecutionConfig) -> None:
        if not config.base_url:
            raise ValueError("PolymarketExecutionConfig.base_url is required for live execution")
        self.cfg = config
        self.session = requests.Session()
        if config.api_key:
            self.session.headers.update({"Authorization": f"Bearer {config.api_key}"})

    def _url(self, path: str) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _map_status(self, raw: Any) -> OrderStatus:
        key = str(raw or "").strip().upper()
        return _STATUS_MAP.get(key, OrderStatus.FAILED)

    def place_order(self, intent: ExecutionIntent) -> tuple[str, OrderStatus, str]:
        payload = {
            "client_order_id": intent.client_order_id,
            "event_id": intent.event_id,
            "asset_id": intent.asset_id,
            "side": intent.side.value,
            "qty": intent.qty,
            "limit_price": intent.limit_price,
            "timeout_sec": intent.timeout_sec,
        }
        resp = self.session.post(self._url(self.cfg.place_order_path), json=payload, timeout=self.cfg.timeout_sec)
        if resp.status_code >= 400:
            return "", OrderStatus.REJECTED, f"http_{resp.status_code}"
        data = resp.json() if resp.content else {}
        exchange_order_id = str(data.get("exchange_order_id") or data.get("order_id") or "")
        status = self._map_status(data.get("status") or "NEW")
        reject_reason = str(data.get("reject_reason") or "")
        return exchange_order_id, status, reject_reason

    def cancel_order(self, exchange_order_id: str) -> bool:
        path = self.cfg.cancel_order_path_tmpl.format(exchange_order_id=exchange_order_id)
        resp = self.session.post(self._url(path), timeout=self.cfg.timeout_sec)
        return resp.status_code < 400

    def get_order_update(self, exchange_order_id: str) -> tuple[OrderStatus, float, float | None, list[Fill], str]:
        path = self.cfg.get_order_path_tmpl.format(exchange_order_id=exchange_order_id)
        resp = self.session.get(self._url(path), timeout=self.cfg.timeout_sec)
        if resp.status_code >= 400:
            return OrderStatus.FAILED, 0.0, None, [], f"http_{resp.status_code}"

        data = resp.json() if resp.content else {}
        status = self._map_status(data.get("status"))
        filled_qty = float(data.get("filled_qty") or 0.0)
        avg_fill_price_raw = data.get("avg_fill_price")
        avg_fill_price = float(avg_fill_price_raw) if avg_fill_price_raw is not None else None
        reject_reason = str(data.get("reject_reason") or "")

        fills: list[Fill] = []
        for f in data.get("fills") or []:
            try:
                fills.append(
                    Fill(
                        order_id=str(f.get("order_id") or exchange_order_id),
                        qty=float(f["qty"]),
                        price=float(f["price"]),
                        ts=str(f.get("ts") or ""),
                    )
                )
            except Exception:
                continue

        return status, filled_qty, avg_fill_price, fills, reject_reason
