from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .polymarket_account import PolymarketAccount


@dataclass(frozen=True)
class DirectOrderRequest:
    token_id: str
    price: float
    size: float
    side: str  # BUY / SELL


class PolymarketDirectTrader:
    """Programmatic order placement via official py_clob_client."""

    @staticmethod
    def _build_client(account: PolymarketAccount, private_key: str):
        try:
            from py_clob_client.client import ClobClient
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("py_clob_client is required. Install dependency: py-clob-client") from exc

        return ClobClient(
            host=account.host,
            chain_id=account.chain_id,
            key=private_key,
            creds={
                "apiKey": account.creds.apiKey,
                "secret": account.creds.secret,
                "passphrase": account.creds.passphrase,
            },
            signature_type=account.signature_type,
            funder=account.funder,
        )

    def place_order(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        req: DirectOrderRequest,
        order_type: str = "GTC",
    ) -> dict[str, Any]:
        client = self._build_client(account, private_key)
        side = str(req.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

        # py_clob_client uses create_order + post_order flow.
        order_args = {
            "token_id": str(req.token_id),
            "price": float(req.price),
            "size": float(req.size),
            "side": side,
        }
        signed_order = client.create_order(order_args)
        return client.post_order(signed_order, order_type)

    def cancel_order(self, *, account: PolymarketAccount, private_key: str, order_id: str) -> Any:
        client = self._build_client(account, private_key)
        return client.cancel(order_id)

    def get_open_orders(self, *, account: PolymarketAccount, private_key: str) -> Any:
        client = self._build_client(account, private_key)
        return client.get_orders()
