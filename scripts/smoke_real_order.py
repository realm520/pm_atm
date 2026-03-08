#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

from weather_arb.polymarket_account import PolymarketAccountManager
from weather_arb.polymarket_direct_trader import DirectOrderRequest, PolymarketDirectTrader


def fetch_any_token_id() -> str:
    """从 CLOB 直接获取一个接受挂单的 token id（用于 smoke test）。"""
    from py_clob_client.client import ClobClient

    # 使用只读模式，dummy key 仅用于实例化，get_sampling_simplified_markets 不需要签名
    client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key="0x" + "0" * 64)
    resp = client.get_sampling_simplified_markets()
    markets = resp.get("data", [])
    print(f"[smoke] fetched {len(markets)} markets from CLOB")
    for market in markets:
        if not market.get("accepting_orders"):
            continue
        for token in market.get("tokens", []):
            tid = token.get("token_id", "")
            if tid:
                print(f"[smoke] auto token_id={tid}")
                return tid
    raise RuntimeError("no accepting_orders token found; set --token-id manually")


def main() -> None:
    p = argparse.ArgumentParser(description="Smoke test: place minimal order then cancel")
    p.add_argument("--account-name", required=True)
    p.add_argument("--vault", default="state/polymarket_accounts.json")
    p.add_argument("--token-id", default=None, help="Token id; auto-fetched from Polymarket if omitted")
    p.add_argument("--price", type=float, default=0.01)
    p.add_argument("--size", type=float, default=5.0)
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
