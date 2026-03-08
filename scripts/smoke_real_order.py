#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

import requests

from weather_arb.polymarket import PolymarketClient
from weather_arb.polymarket_account import PolymarketAccountManager
from weather_arb.polymarket_direct_trader import DirectOrderRequest, PolymarketDirectTrader

CLOB_HOST = "https://clob.polymarket.com"


def fetch_any_token_id() -> str:
    """从 Polymarket 自动获取一个可用的 token id（用于 smoke test）。"""
    client = PolymarketClient()
    markets = client.list_markets(active=True, limit=20)
    print(f"[smoke] fetched {len(markets)} markets from gamma-api")
    checked = 0
    for market in markets:
        for tid in PolymarketClient.parse_clob_token_ids(market):
            checked += 1
            try:
                r = requests.get(f"{CLOB_HOST}/tick-size?token_id={tid}", timeout=5)
                if r.status_code == 200:
                    print(f"[smoke] auto token_id={tid} (checked {checked} tokens)")
                    return tid
                print(f"[smoke]   tick-size {r.status_code} for tid={tid[:24]}")
            except requests.RequestException as e:
                print(f"[smoke]   request error for tid={tid[:24]}: {e}")
    raise RuntimeError(f"no valid token found after checking {checked} token ids; set --token-id manually")


def main() -> None:
    p = argparse.ArgumentParser(description="Smoke test: place minimal order then cancel")
    p.add_argument("--account-name", required=True)
    p.add_argument("--vault", default="state/polymarket_accounts.json")
    p.add_argument("--token-id", default=None, help="Token id; auto-fetched from Polymarket if omitted")
    p.add_argument("--price", type=float, default=0.01)
    p.add_argument("--size", type=float, default=1.0)
    p.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    p.add_argument("--sleep-sec", type=float, default=1.5)
    args = p.parse_args()

    pk = os.environ.get("POLY_PRIVATE_KEY", "")
    if not pk:
        raise ValueError("POLY_PRIVATE_KEY env is required")

    token_id = args.token_id or fetch_any_token_id()

    account = PolymarketAccountManager(args.vault).get_account(args.account_name)
    trader = PolymarketDirectTrader()

    order = trader.place_order(
        account=account,
        private_key=pk,
        req=DirectOrderRequest(token_id=token_id, price=args.price, size=args.size, side=args.side),
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
