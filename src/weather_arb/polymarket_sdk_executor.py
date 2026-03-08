from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .execution_service import ExchangeExecutionPort
from .orders import ExecutionIntent, Fill, OrderStatus
from .polymarket_account import PolymarketAccount


@dataclass(frozen=True)
class PolymarketSdkExecutorConfig:
    order_type: str = "GTC"


class PolymarketSdkExecutor(ExchangeExecutionPort):
    def __init__(self, *, account: PolymarketAccount, private_key: str, config: PolymarketSdkExecutorConfig | None = None) -> None:
        self.account = account
        self.private_key = private_key
        self.cfg = config or PolymarketSdkExecutorConfig()
        self.client = self._build_client()

    def _build_client(self):
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        return ClobClient(
            host=self.account.host,
            chain_id=self.account.chain_id,
            key=self.private_key,
            creds=ApiCreds(
                api_key=self.account.creds.apiKey,
                api_secret=self.account.creds.secret,
                api_passphrase=self.account.creds.passphrase,
            ),
            signature_type=self.account.signature_type,
            funder=self.account.funder,
        )

    @staticmethod
    def _status(raw: Any) -> OrderStatus:
        s = str(raw or "").upper()
        if s in {"LIVE", "OPEN", "NEW"}:
            return OrderStatus.NEW
        if s in {"MATCHED", "FILLED"}:
            return OrderStatus.FILLED
        if s in {"CANCELED", "CANCELLED"}:
            return OrderStatus.CANCELED
        if s in {"PARTIALLY_FILLED", "PARTIAL"}:
            return OrderStatus.PARTIALLY_FILLED
        if s in {"REJECTED"}:
            return OrderStatus.REJECTED
        return OrderStatus.FAILED

    @staticmethod
    def _get(obj: Any, key: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    def place_order(self, intent: ExecutionIntent) -> tuple[str, OrderStatus, str]:
        from py_clob_client.clob_types import OrderArgs

        order_args = OrderArgs(token_id=intent.asset_id, price=float(intent.limit_price), size=float(intent.qty), side=intent.side.value)
        try:
            signed = self.client.create_order(order_args)
            resp = self.client.post_order(signed, self.cfg.order_type)
            oid = str(self._get(resp, "orderID", "") or self._get(resp, "id", ""))
            st = self._status(self._get(resp, "status", "NEW"))
            return oid, st, ""
        except Exception as exc:
            return "", OrderStatus.REJECTED, str(exc)

    def cancel_order(self, exchange_order_id: str) -> bool:
        try:
            self.client.cancel(exchange_order_id)
            return True
        except Exception:
            return False

    def get_order_update(self, exchange_order_id: str) -> tuple[OrderStatus, float, float | None, list[Fill], str]:
        try:
            resp = self.client.get_order(exchange_order_id)
        except Exception as exc:
            return OrderStatus.FAILED, 0.0, None, [], str(exc)

        status = self._status(self._get(resp, "status", ""))
        filled = float(self._get(resp, "size_matched", 0.0) or self._get(resp, "filled_size", 0.0) or 0.0)
        avg = self._get(resp, "avg_price", None)
        avg_fill = float(avg) if avg is not None else None
        return status, filled, avg_fill, [], ""
