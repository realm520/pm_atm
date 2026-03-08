#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

from weather_arb.polymarket_account import PolymarketAccountManager
from weather_arb.polymarket_direct_trader import DirectOrderRequest, PolymarketDirectTrader


def main() -> None:
    p = argparse.ArgumentParser(description="Smoke test: place minimal order then cancel")
    p.add_argument("--account-name", required=True)
    p.add_argument("--vault", default="state/polymarket_accounts.json")
    p.add_argument("--token-id", required=True)
    p.add_argument("--price", type=float, default=0.01)
    p.add_argument("--size", type=float, default=1.0)
    p.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    p.add_argument("--sleep-sec", type=float, default=1.5)
    args = p.parse_args()

    pk = os.environ.get("POLY_PRIVATE_KEY", "")
    if not pk:
        raise ValueError("POLY_PRIVATE_KEY env is required")

    account = PolymarketAccountManager(args.vault).get_account(args.account_name)
    trader = PolymarketDirectTrader()

    order = trader.place_order(
        account=account,
        private_key=pk,
        req=DirectOrderRequest(token_id=args.token_id, price=args.price, size=args.size, side=args.side),
        order_type="GTC",
    )
    print("[smoke] placed", json.dumps(order, ensure_ascii=False))

    oid = order.get("orderID") or order.get("id")
    if not oid:
        raise RuntimeError("missing order id in response")

    time.sleep(args.sleep_sec)
    cancel = trader.cancel_order(account=account, private_key=pk, order_id=str(oid))
    print("[smoke] canceled", json.dumps(cancel, ensure_ascii=False))
    print("[smoke][OK]")


if __name__ == "__main__":
    main()
