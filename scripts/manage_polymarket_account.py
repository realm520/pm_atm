#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os

from weather_arb.polymarket_account import PolymarketAccountManager
from weather_arb.polymarket_direct_trader import DirectOrderRequest, PolymarketDirectTrader


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket account bootstrap + direct trading")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Create/derive API credentials and save account")
    p_init.add_argument("--name", required=True)
    p_init.add_argument("--wallet-address", required=True)
    p_init.add_argument("--funder", required=True)
    p_init.add_argument("--signature-type", type=int, default=2)
    p_init.add_argument("--nonce", type=int, default=0)
    p_init.add_argument("--chain-id", type=int, default=137)
    p_init.add_argument("--host", default="https://clob.polymarket.com")
    p_init.add_argument("--vault", default="state/polymarket_accounts.json")

    p_list = sub.add_parser("list", help="List managed accounts")
    p_list.add_argument("--vault", default="state/polymarket_accounts.json")

    p_order = sub.add_parser("place-order", help="Place order directly via py_clob_client")
    p_order.add_argument("--name", required=True)
    p_order.add_argument("--token-id", required=True)
    p_order.add_argument("--price", type=float, required=True)
    p_order.add_argument("--size", type=float, required=True)
    p_order.add_argument("--side", choices=["BUY", "SELL"], required=True)
    p_order.add_argument("--order-type", default="GTC")
    p_order.add_argument("--vault", default="state/polymarket_accounts.json")

    p_cancel = sub.add_parser("cancel-order", help="Cancel an order")
    p_cancel.add_argument("--name", required=True)
    p_cancel.add_argument("--order-id", required=True)
    p_cancel.add_argument("--vault", default="state/polymarket_accounts.json")

    p_set_funder = sub.add_parser("set-funder", help="Update account funder/signature type")
    p_set_funder.add_argument("--name", required=True)
    p_set_funder.add_argument("--funder", required=True)
    p_set_funder.add_argument("--signature-type", type=int, default=None)
    p_set_funder.add_argument("--vault", default="state/polymarket_accounts.json")

    p_open_orders = sub.add_parser("open-orders", help="List open orders for account")
    p_open_orders.add_argument("--name", required=True)
    p_open_orders.add_argument("--vault", default="state/polymarket_accounts.json")

    p_trades = sub.add_parser("trades", help="List trade history for account")
    p_trades.add_argument("--name", required=True)
    p_trades.add_argument("--vault", default="state/polymarket_accounts.json")

    p_positions = sub.add_parser("positions", help="Show positions and P&L (aggregated from trade history)")
    p_positions.add_argument("--name", required=True)
    p_positions.add_argument("--vault", default="state/polymarket_accounts.json")
    p_positions.add_argument("--open-only", action="store_true", help="只显示净持仓 > 0 的标的")

    p_close = sub.add_parser("close-all", help="平掉所有净多仓位（发市价 SELL 单）")
    p_close.add_argument("--name", required=True)
    p_close.add_argument("--vault", default="state/polymarket_accounts.json")
    p_close.add_argument("--min-qty", type=float, default=0.0, help="跳过 net_qty <= 此值的仓位（默认 0）")
    p_close.add_argument("--price-offset", type=float, default=0.0, help="价格偏移（如 -0.02 表示当前价下调 0.02）")
    p_close.add_argument("--dry-run", action="store_true", help="只显示将要执行的操作，不实际下单")

    p_deposit = sub.add_parser("show-deposit-addresses", help="Fetch bridge deposit addresses for account funder")
    p_deposit.add_argument("--name", required=True)
    p_deposit.add_argument("--vault", default="state/polymarket_accounts.json")

    args = parser.parse_args()

    manager = PolymarketAccountManager(vault_path=args.vault)

    if args.cmd == "init":
        private_key = os.getenv("POLY_PRIVATE_KEY", "")
        if not private_key:
            raise ValueError("POLY_PRIVATE_KEY env is required for init")
        acct = manager.create_or_derive_account(
            name=args.name,
            private_key=private_key,
            wallet_address=args.wallet_address,
            funder=args.funder,
            signature_type=args.signature_type,
            nonce=args.nonce,
            chain_id=args.chain_id,
            host=args.host,
        )
        safe = {
            "name": acct.name,
            "chain_id": acct.chain_id,
            "host": acct.host,
            "signature_type": acct.signature_type,
            "funder": acct.funder,
            "wallet_address": acct.wallet_address,
            "nonce": acct.nonce,
            "apiKey": acct.creds.apiKey,
        }
        print(json.dumps(safe, ensure_ascii=False, indent=2))
        return

    if args.cmd == "list":
        out = []
        for a in manager.list_accounts():
            out.append(
                {
                    "name": a.name,
                    "wallet_address": a.wallet_address,
                    "funder": a.funder,
                    "signature_type": a.signature_type,
                    "chain_id": a.chain_id,
                    "host": a.host,
                    "nonce": a.nonce,
                    "apiKey": a.creds.apiKey,
                }
            )
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    if args.cmd == "set-funder":
        acct = manager.update_funder(name=args.name, funder=args.funder, signature_type=args.signature_type)
        print(
            json.dumps(
                {
                    "name": acct.name,
                    "wallet_address": acct.wallet_address,
                    "funder": acct.funder,
                    "signature_type": acct.signature_type,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.cmd == "show-deposit-addresses":
        acct = manager.get_account(args.name)
        res = manager.get_bridge_deposit_addresses(acct.funder)
        print(json.dumps({"name": acct.name, "funder": acct.funder, "deposit": res}, ensure_ascii=False, indent=2))
        return

    private_key = os.getenv("POLY_PRIVATE_KEY", "")
    if not private_key:
        raise ValueError("POLY_PRIVATE_KEY env is required for trading commands")

    account = manager.get_account(args.name)
    trader = PolymarketDirectTrader()

    if args.cmd == "place-order":
        res = trader.place_order(
            account=account,
            private_key=private_key,
            req=DirectOrderRequest(
                token_id=args.token_id,
                price=args.price,
                size=args.size,
                side=args.side,
            ),
            order_type=args.order_type,
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    if args.cmd == "cancel-order":
        res = trader.cancel_order(account=account, private_key=private_key, order_id=args.order_id)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    if args.cmd == "open-orders":
        res = trader.get_open_orders(account=account, private_key=private_key)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    if args.cmd == "trades":
        res = trader.get_trades(account=account, private_key=private_key)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    if args.cmd == "close-all":
        results = trader.close_all_positions(
            account=account,
            private_key=private_key,
            min_qty=args.min_qty,
            price_offset=args.price_offset,
            dry_run=args.dry_run,
        )
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return

    if args.cmd == "positions":
        positions = trader.get_positions_pnl(
            account=account, private_key=private_key, open_only=args.open_only
        )
        out = [
            {
                "token_id": p.token_id,
                "market": p.market,
                "net_qty": round(p.net_qty, 4),
                "avg_cost": round(p.avg_cost, 4),
                "current_price": round(p.current_price, 4),
                "unrealized_pnl": round(p.unrealized_pnl, 4),
                "realized_pnl": round(p.realized_pnl, 4),
                "total_bought": round(p.total_bought, 4),
                "total_sold": round(p.total_sold, 4),
            }
            for p in positions
        ]
        total_unrealized = sum(p.unrealized_pnl for p in positions)
        total_realized = sum(p.realized_pnl for p in positions)
        result = {
            "positions": out,
            "summary": {
                "total_unrealized_pnl": round(total_unrealized, 4),
                "total_realized_pnl": round(total_realized, 4),
                "total_pnl": round(total_unrealized + total_realized, 4),
            },
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return


if __name__ == "__main__":
    main()
