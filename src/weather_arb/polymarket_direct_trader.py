from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from py_clob_client.clob_types import BookParams

from .polymarket_account import PolymarketAccount


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
        from py_clob_client.clob_types import OrderArgs

        order_args = OrderArgs(
            token_id=str(req.token_id),
            price=float(req.price),
            size=float(req.size),
            side=side,
        )
        signed_order = client.create_order(order_args)
        return client.post_order(signed_order, order_type)

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
        """按 token_id 聚合成交记录，结合当前市场价计算持仓盈亏。"""
        client = self._build_client(account, private_key)
        trades: list[dict] = client.get_trades()

        # 按 asset_id 聚合
        buckets: dict[str, dict] = defaultdict(lambda: {
            "market": "",
            "buy_qty": 0.0,
            "buy_cost": 0.0,
            "sell_qty": 0.0,
            "sell_revenue": 0.0,
        })
        for t in trades:
            tid = t.get("asset_id") or t.get("token_id", "")
            if not tid:
                continue
            b = buckets[tid]
            if not b["market"]:
                b["market"] = t.get("market", "")
            side = str(t.get("side", "")).upper()
            price = float(t.get("price", 0))
            size = float(t.get("size", 0))
            if side == "BUY":
                b["buy_qty"] += size
                b["buy_cost"] += price * size
            elif side == "SELL":
                b["sell_qty"] += size
                b["sell_revenue"] += price * size

        # 批量获取当前价格
        price_map: dict[str, float] = {}
        if buckets:
            book_params = [BookParams(token_id=tid) for tid in buckets]
            prices = client.get_last_trades_prices(book_params)
            for p in prices:
                price_map[p.get("token_id", "")] = float(p.get("price", 0))

        results: list[PositionPnl] = []
        for tid, b in buckets.items():
            buy_qty = b["buy_qty"]
            sell_qty = b["sell_qty"]
            buy_cost = b["buy_cost"]
            sell_revenue = b["sell_revenue"]
            net_qty = buy_qty - sell_qty

            if open_only and net_qty <= 0:
                continue

            avg_cost = (buy_cost / buy_qty) if buy_qty > 0 else 0.0
            realized_pnl = sell_revenue - (avg_cost * sell_qty)
            current_price = price_map.get(tid, 0.0)
            unrealized_pnl = (current_price - avg_cost) * net_qty if net_qty > 0 else 0.0

            results.append(
                PositionPnl(
                    token_id=tid,
                    market=b["market"],
                    net_qty=net_qty,
                    avg_cost=avg_cost,
                    current_price=current_price,
                    unrealized_pnl=unrealized_pnl,
                    realized_pnl=realized_pnl,
                    total_bought=buy_qty,
                    total_sold=sell_qty,
                )
            )

        results.sort(key=lambda x: abs(x.net_qty), reverse=True)
        return results
