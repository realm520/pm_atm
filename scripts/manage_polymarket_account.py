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


if __name__ == "__main__":
    main()
