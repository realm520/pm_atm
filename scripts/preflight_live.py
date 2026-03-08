#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams

from weather_arb.polymarket_account import PolymarketAccountManager


def fail(msg: str) -> None:
    print(f"[preflight][FAIL] {msg}")
    sys.exit(1)


def main() -> None:
    p = argparse.ArgumentParser(description="Preflight checks for live trading")
    p.add_argument("--account-name", required=True)
    p.add_argument("--vault", default="state/polymarket_accounts.json")
    p.add_argument("--min-usdc", type=float, default=1.0)
    p.add_argument("--require-unblocked", action="store_true")
    p.add_argument("--require-allowance", action="store_true", help="Fail when allowance is zero")
    p.add_argument("--auto-approve-allowance", action="store_true", help="Try update_balance_allowance when allowance is zero")
    args = p.parse_args()

    pk = os.environ.get("POLY_PRIVATE_KEY", "")
    if not pk:
        fail("POLY_PRIVATE_KEY is missing")

    acct = PolymarketAccountManager(args.vault).get_account(args.account_name)

    geo = requests.get("https://polymarket.com/api/geoblock", timeout=8).json()
    print(f"[preflight] geoblock={geo}")
    if args.require_unblocked and bool(geo.get("blocked")):
        fail("geoblock blocked=true")

    client = ClobClient(
        host=acct.host,
        chain_id=acct.chain_id,
        key=pk,
        creds=ApiCreds(api_key=acct.creds.apiKey, api_secret=acct.creds.secret, api_passphrase=acct.creds.passphrase),
        signature_type=acct.signature_type,
        funder=acct.funder,
    )

    params = BalanceAllowanceParams(asset_type="COLLATERAL", signature_type=acct.signature_type)
    bal = client.get_balance_allowance(params)
    print(f"[preflight] collateral={json.dumps(bal, ensure_ascii=False)}")

    avail = float((bal or {}).get("balance", 0) or 0) / 1_000_000.0
    if avail < args.min_usdc:
        fail(f"insufficient collateral balance={avail} < min_usdc={args.min_usdc}")

    allowances = (bal or {}).get("allowances") or {}
    max_allowance = 0
    try:
        if allowances:
            max_allowance = max(int(v) for v in allowances.values())
    except Exception:
        max_allowance = 0

    if max_allowance <= 0 and args.auto_approve_allowance:
        print("[preflight] allowance is zero, trying update_balance_allowance...")
        client.update_balance_allowance(params)
        bal = client.get_balance_allowance(params)
        allowances = (bal or {}).get("allowances") or {}
        try:
            max_allowance = max(int(v) for v in allowances.values()) if allowances else 0
        except Exception:
            max_allowance = 0
        print(f"[preflight] collateral_after_approve={json.dumps(bal, ensure_ascii=False)}")

    if args.require_allowance and max_allowance <= 0:
        fail("allowance is zero (use --auto-approve-allowance or approve in UI)")

    print("[preflight][OK] checks passed")


if __name__ == "__main__":
    main()
