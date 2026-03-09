from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .polymarket_account import PolymarketAccount
from .polymarket_utils import sanitize_order_amounts


@dataclass
class PositionPnl:
    token_id: str
    market: str
    net_qty: float           # 净持仓（正=多头）
    avg_cost: float          # 买入均价
    current_price: float     # 当前市场最新成交价
    unrealized_pnl: float    # 未实现盈亏
    realized_pnl: float      # 已实现盈亏（已平仓部分）
    total_bought: float
    total_sold: float


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

        from py_clob_client.clob_types import ApiCreds

        return ClobClient(
            host=account.host,
            chain_id=account.chain_id,
            key=private_key,
            creds=ApiCreds(
                api_key=account.creds.apiKey,
                api_secret=account.creds.secret,
                api_passphrase=account.creds.passphrase,
            ),
            signature_type=account.signature_type,
            funder=account.funder,
        )

    def _post_order(self, client: Any, req: DirectOrderRequest, order_type: str = "GTC") -> dict[str, Any]:
        side = str(req.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

        from py_clob_client.clob_types import OrderArgs

        price, size = sanitize_order_amounts(side, float(req.price), float(req.size))
        order_args = OrderArgs(
            token_id=str(req.token_id),
            price=price,
            size=size,
            side=side,
        )
        signed_order = client.create_order(order_args)
        return client.post_order(signed_order, order_type)

    def place_order(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        req: DirectOrderRequest,
        order_type: str = "GTC",
    ) -> dict[str, Any]:
        client = self._build_client(account, private_key)
        return self._post_order(client, req, order_type)

    def cancel_order(self, *, account: PolymarketAccount, private_key: str, order_id: str) -> Any:
        client = self._build_client(account, private_key)
        return client.cancel(order_id)

    def get_open_orders(self, *, account: PolymarketAccount, private_key: str) -> Any:
        client = self._build_client(account, private_key)
        return client.get_orders()

    def get_trades(self, *, account: PolymarketAccount, private_key: str) -> list[dict]:
        client = self._build_client(account, private_key)
        return client.get_trades()

    def get_positions_pnl(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        open_only: bool = False,
    ) -> list[PositionPnl]:
        """从 Polymarket data API 查询实际持仓盈亏（基于链上余额，非成交流水重建）。"""
        resp = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": account.funder, "sizeThreshold": 0.01, "limit": 500},
            timeout=15,
        )
        resp.raise_for_status()
        raw: list[dict] = resp.json()

        results: list[PositionPnl] = []
        for p in raw:
            net_qty = float(p.get("size", 0))
            if open_only and net_qty <= 0:
                continue

            total_bought = float(p.get("totalBought", 0))
            avg_cost = float(p.get("avgPrice", 0))
            current_price = float(p.get("curPrice", 0))
            unrealized_pnl = float(p.get("cashPnl", 0))
            realized_pnl = float(p.get("realizedPnl", 0))
            total_sold = max(0.0, total_bought - net_qty)

            results.append(
                PositionPnl(
                    token_id=str(p.get("asset", "")),
                    market=str(p.get("title", "")),
                    net_qty=net_qty,
                    avg_cost=avg_cost,
                    current_price=current_price,
                    unrealized_pnl=unrealized_pnl,
                    realized_pnl=realized_pnl,
                    total_bought=total_bought,
                    total_sold=total_sold,
                )
            )

        results.sort(key=lambda x: abs(x.net_qty), reverse=True)
        return results

    def close_all_positions(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        min_qty: float = 0.0,
        price_offset: float = 0.0,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """平掉所有净多仓位（net_qty > min_qty）。

        Args:
            min_qty: 低于此数量的仓位跳过（默认 0，即平所有净多头）。
            price_offset: 在当前价基础上的价格偏移（负值=向下调整，默认 0）。
            dry_run: 若为 True，只返回待执行订单列表，不实际下单。

        Returns:
            每个仓位的执行结果列表。
        """
        positions = self.get_positions_pnl(account=account, private_key=private_key, open_only=True)
        positions = [p for p in positions if p.net_qty > min_qty]

        results: list[dict[str, Any]] = []
        client = None if dry_run else self._build_client(account, private_key)

        for pos in positions:
            price = max(0.01, min(0.99, round(pos.current_price + price_offset, 4)))
            entry: dict[str, Any] = {
                "token_id": pos.token_id,
                "market": pos.market,
                "net_qty": pos.net_qty,
                "sell_price": price,
            }
            if dry_run:
                entry["status"] = "skipped (dry_run)"
                results.append(entry)
                continue

            try:
                resp = self._post_order(
                    client,
                    DirectOrderRequest(token_id=pos.token_id, price=price, size=pos.net_qty, side="SELL"),
                )
                entry["status"] = "ok"
                entry["response"] = resp
            except Exception as exc:
                entry["status"] = "error"
                entry["error"] = str(exc)
            results.append(entry)

        return results
