from __future__ import annotations

import collections
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


@dataclass
class TradePnlSummary:
    """从成交历史计算的盈亏，包含已平仓标的。"""
    token_id: str
    market: str              # outcome 描述（如 "Yes" / "No"）
    condition_id: str
    net_qty: float           # 当前净持仓
    avg_cost: float          # 买入均价（USDC/share）
    total_bought_qty: float
    total_sold_qty: float
    total_cost_usdc: float   # 总买入 USDC
    total_proceeds_usdc: float  # 总卖出 USDC
    realized_pnl: float      # 已实现盈亏
    current_price: float     # 当前价（0 = 未查到）
    unrealized_pnl: float    # 未实现盈亏
    n_trades: int


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

    def get_trades_all(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        limit_per_page: int = 500,
    ) -> list[dict]:
        """获取全部历史成交，自动翻页。"""
        try:
            from py_clob_client.clob_types import TradeParams  # type: ignore[import]
            _has_trade_params = True
        except ImportError:
            _has_trade_params = False

        client = self._build_client(account, private_key)
        all_trades: list[dict] = []
        cursor = ""
        while True:
            if _has_trade_params:
                try:
                    params = TradeParams(limit=limit_per_page)  # type: ignore[possibly-undefined]
                    if cursor:
                        params.next_cursor = cursor  # type: ignore[attr-defined]
                    raw = client.get_trades(params)
                except Exception:
                    raw = client.get_trades()
            else:
                raw = client.get_trades()

            if isinstance(raw, list):
                all_trades.extend(raw)
                break
            elif isinstance(raw, dict):
                items: list[dict] = raw.get("data") or []
                all_trades.extend(items)
                cursor = str(raw.get("next_cursor") or "")
                if not cursor or not items:
                    break
            else:
                break
        return all_trades

    def compute_pnl_from_trades(
        self,
        *,
        account: PolymarketAccount,
        private_key: str,
        open_only: bool = False,
    ) -> list[TradePnlSummary]:
        """从 CLOB 成交历史计算完整盈亏（含已平仓标的）。

        使用平均成本法：
        - 每次 BUY 更新持仓均价
        - SELL 按当前均价计算已实现盈亏
        - 剩余净仓位用实时价格计算未实现盈亏
        """
        trades = self.get_trades_all(account=account, private_key=private_key)

        # 按 asset_id 分组
        by_asset: dict[str, list[dict]] = collections.defaultdict(list)
        for t in trades:
            aid = str(t.get("asset_id") or t.get("outcome_index") or "")
            if aid:
                by_asset[aid].append(t)

        # 批量查询当前价格（用 data-api positions 接口补充）
        try:
            pos_resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": account.funder, "sizeThreshold": 0.0, "limit": 500},
                timeout=15,
            )
            pos_resp.raise_for_status()
            cur_prices: dict[str, float] = {
                str(p.get("asset", "")): float(p.get("curPrice") or 0)
                for p in pos_resp.json()
                if p.get("asset")
            }
        except Exception:
            cur_prices = {}

        results: list[TradePnlSummary] = []
        for asset_id, asset_trades in by_asset.items():
            # 按时间正序处理
            asset_trades.sort(key=lambda x: str(x.get("match_time") or x.get("created_at") or ""))

            net_qty = 0.0
            total_cost_usdc = 0.0   # 累计买入 USDC
            total_bought_qty = 0.0
            total_sold_qty = 0.0
            total_proceeds_usdc = 0.0
            realized_pnl = 0.0
            market = ""
            condition_id = ""

            for t in asset_trades:
                side = str(t.get("side") or "").upper()
                try:
                    price = float(t.get("price") or 0)
                    size = float(t.get("size") or 0)
                except Exception:
                    continue
                if price <= 0 or size <= 0:
                    continue
                if not market:
                    market = str(t.get("outcome") or t.get("market") or "")
                if not condition_id:
                    condition_id = str(t.get("market") or "")

                if side == "BUY":
                    total_cost_usdc += price * size
                    total_bought_qty += size
                    net_qty += size
                elif side == "SELL":
                    avg = total_cost_usdc / total_bought_qty if total_bought_qty > 0 else 0.0
                    realized_pnl += (price - avg) * size
                    total_proceeds_usdc += price * size
                    total_sold_qty += size
                    net_qty -= size
                    # 按比例减少成本基础
                    if total_bought_qty > 0:
                        sold_ratio = size / total_bought_qty
                        total_cost_usdc *= max(0.0, 1.0 - sold_ratio)
                        total_bought_qty -= size

            net_qty = max(0.0, net_qty)
            avg_cost = (total_cost_usdc / total_bought_qty) if total_bought_qty > 0 else 0.0
            current_price = cur_prices.get(asset_id, 0.0)
            unrealized_pnl = (current_price - avg_cost) * net_qty if net_qty > 0 and current_price > 0 else 0.0

            if open_only and net_qty <= 0:
                continue

            results.append(TradePnlSummary(
                token_id=asset_id,
                market=market,
                condition_id=condition_id,
                net_qty=round(net_qty, 4),
                avg_cost=round(avg_cost, 4),
                total_bought_qty=round(total_bought_qty + total_sold_qty, 4),
                total_sold_qty=round(total_sold_qty, 4),
                total_cost_usdc=round(total_cost_usdc, 4),
                total_proceeds_usdc=round(total_proceeds_usdc, 4),
                realized_pnl=round(realized_pnl, 4),
                current_price=round(current_price, 4),
                unrealized_pnl=round(unrealized_pnl, 4),
                n_trades=len(asset_trades),
            ))

        results.sort(key=lambda x: abs(x.realized_pnl + x.unrealized_pnl), reverse=True)
        return results

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
