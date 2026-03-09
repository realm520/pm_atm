#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams
from py_clob_client.config import get_contract_config

from weather_arb.polymarket_account import PolymarketAccountManager

MAX_UINT256 = 2**256 - 1
APPROVE_SELECTOR = bytes.fromhex("095ea7b3")           # keccak256("approve(address,uint256)")[:4]
SET_APPROVAL_FOR_ALL_SELECTOR = bytes.fromhex("a22cb465")  # keccak256("setApprovalForAll(address,bool)")[:4]
IS_APPROVED_FOR_ALL_SELECTOR = bytes.fromhex("e985e9c5")   # keccak256("isApprovedForAll(address,address)")[:4]
DEFAULT_POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"


def fail(msg: str) -> None:
    print(f"[preflight][FAIL] {msg}")
    sys.exit(1)


def _rpc(rpc_url: str, method: str, params: list) -> object:
    resp = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        err = data["error"]
        raise RuntimeError(f"RPC error [{method}]: {err.get('message', err) if isinstance(err, dict) else err}")
    return data["result"]


def _wait_receipt(rpc_url: str, tx_hash: str, timeout: int = 90) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        receipt = _rpc(rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            return receipt
        time.sleep(3)
    raise TimeoutError(f"tx {tx_hash} not confirmed within {timeout}s")


def onchain_approve_usdc(pk: str, chain_id: int, usdc: str, spenders: list[str], rpc_url: str) -> None:
    from eth_account import Account
    from eth_abi import encode

    acct = Account.from_key(pk)
    wallet = acct.address
    nonce = int(_rpc(rpc_url, "eth_getTransactionCount", [wallet, "pending"]), 16)
    gas_price = int(int(_rpc(rpc_url, "eth_gasPrice", []), 16) * 1.2)

    for spender in spenders:
        calldata = "0x" + (APPROVE_SELECTOR + encode(["address", "uint256"], [spender, MAX_UINT256])).hex()
        gas = int(
            int(_rpc(rpc_url, "eth_estimateGas", [{"from": wallet, "to": usdc, "data": calldata}]), 16) * 1.3
        )

        signed = acct.sign_transaction({
            "nonce": nonce,
            "to": usdc,
            "value": 0,
            "gas": gas,
            "gasPrice": gas_price,
            "data": calldata,
            "chainId": chain_id,
        })
        raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction
        tx_hash = _rpc(rpc_url, "eth_sendRawTransaction", ["0x" + raw.hex()])
        print(f"[preflight] approve tx sent: spender={spender} txhash={tx_hash}")

        receipt = _wait_receipt(rpc_url, tx_hash)
        status = int(receipt.get("status", "0x0"), 16)
        if status != 1:
            raise RuntimeError(f"approve tx reverted: txhash={tx_hash}")
        print(f"[preflight] approve confirmed: spender={spender}")
        nonce += 1


def onchain_set_approval_for_all(pk: str, chain_id: int, ctf: str, operators: list[str], rpc_url: str) -> None:
    from eth_account import Account
    from eth_abi import encode

    acct = Account.from_key(pk)
    wallet = acct.address
    nonce = int(_rpc(rpc_url, "eth_getTransactionCount", [wallet, "pending"]), 16)
    gas_price = int(int(_rpc(rpc_url, "eth_gasPrice", []), 16) * 1.5)

    for operator in operators:
        calldata = "0x" + (SET_APPROVAL_FOR_ALL_SELECTOR + encode(["address", "bool"], [operator, True])).hex()
        try:
            gas = int(int(_rpc(rpc_url, "eth_estimateGas", [{"from": wallet, "to": ctf, "data": calldata}]), 16) * 1.3)
        except Exception:
            gas = 100_000

        signed = acct.sign_transaction({
            "nonce": nonce,
            "to": ctf,
            "value": 0,
            "gas": gas,
            "gasPrice": gas_price,
            "data": calldata,
            "chainId": chain_id,
        })
        raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction
        tx_hash = _rpc(rpc_url, "eth_sendRawTransaction", ["0x" + raw.hex()])
        print(f"[preflight] setApprovalForAll tx sent: operator={operator} txhash={tx_hash}")

        receipt = _wait_receipt(rpc_url, tx_hash)
        status = int(receipt.get("status", "0x0"), 16)
        if status != 1:
            raise RuntimeError(f"setApprovalForAll tx reverted: txhash={tx_hash}")
        print(f"[preflight] setApprovalForAll confirmed: operator={operator}")
        nonce += 1


def main() -> None:
    p = argparse.ArgumentParser(description="Preflight checks for live trading")
    p.add_argument("--account-name", required=True)
    p.add_argument("--vault", default="state/polymarket_accounts.json")
    p.add_argument("--min-usdc", type=float, default=1.0)
    p.add_argument("--require-unblocked", action="store_true")
    p.add_argument("--require-allowance", action="store_true", help="Fail when allowance is zero")
    p.add_argument(
        "--auto-approve-allowance",
        action="store_true",
        help="Submit onchain USDC approve() for all Polymarket spenders, then sync Polymarket state",
    )
    p.add_argument("--polygon-rpc", default=DEFAULT_POLYGON_RPC, help="Polygon JSON-RPC endpoint")
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

    def _max_allowance(als: dict) -> int:
        try:
            return max((int(v) for v in als.values()), default=0) if als else 0
        except Exception:
            return 0

    allowances = (bal or {}).get("allowances") or {}
    max_allowance = _max_allowance(allowances)

    if max_allowance <= 0 and args.auto_approve_allowance:
        zero_spenders = [s for s, v in allowances.items() if _max_allowance({s: v}) == 0]
        if zero_spenders:
            contract_cfg = get_contract_config(acct.chain_id)
            print(f"[preflight] onchain approve: usdc={contract_cfg.collateral} spenders={zero_spenders} rpc={args.polygon_rpc}")
            onchain_approve_usdc(pk, acct.chain_id, contract_cfg.collateral, zero_spenders, args.polygon_rpc)
        # sync Polymarket state after onchain approve
        client.update_balance_allowance(params)
        bal = client.get_balance_allowance(params)
        allowances = (bal or {}).get("allowances") or {}
        max_allowance = _max_allowance(allowances)
        print(f"[preflight] collateral_after_approve={json.dumps(bal, ensure_ascii=False)}")

    if args.require_allowance and max_allowance <= 0:
        fail("allowance is zero (use --auto-approve-allowance or approve in UI)")

    # --- CONDITIONAL (CTF token / ERC-1155) setApprovalForAll check ---
    # setApprovalForAll 是全局 operator 授权（非 per-token），链上查 isApprovedForAll 最准确
    from eth_abi import encode as abi_encode
    contract_cfg = get_contract_config(acct.chain_id)
    ctf_addr = contract_cfg.conditional_tokens
    exchange_operator = contract_cfg.exchange
    owner = acct.wallet_address
    try:
        calldata = "0x" + (IS_APPROVED_FOR_ALL_SELECTOR + abi_encode(["address", "address"], [owner, exchange_operator])).hex()
        result = _rpc(args.polygon_rpc, "eth_call", [{"to": ctf_addr, "data": calldata}, "latest"])
        is_approved = int(result, 16) != 0
        print(f"[preflight] isApprovedForAll(owner={owner}, operator={exchange_operator}) = {is_approved}")

        if not is_approved:
            if args.auto_approve_allowance:
                print(f"[preflight] onchain setApprovalForAll: ctf={ctf_addr} operator={exchange_operator}")
                onchain_set_approval_for_all(pk, acct.chain_id, ctf_addr, [exchange_operator], args.polygon_rpc)
            elif args.require_allowance:
                fail(f"CTF conditional setApprovalForAll not set for operator={exchange_operator} (use --auto-approve-allowance)")
    except SystemExit:
        raise
    except Exception as exc:
        msg = f"conditional approval check failed: {exc}"
        if args.require_allowance:
            fail(msg)
        print(f"[preflight][warning] {msg}")

    print("[preflight][OK] checks passed")


if __name__ == "__main__":
    main()
