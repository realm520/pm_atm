from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Protocol

import requests


@dataclass(frozen=True)
class PolymarketApiCreds:
    apiKey: str
    secret: str
    passphrase: str


@dataclass(frozen=True)
class PolymarketAccount:
    name: str
    chain_id: int
    host: str
    signature_type: int
    funder: str
    wallet_address: str
    nonce: int
    creds: PolymarketApiCreds


class ClobLikeClient(Protocol):
    def create_or_derive_api_creds(self, nonce: int = 0) -> dict[str, Any]: ...


class PolymarketAccountManager:
    """Manage Polymarket API credentials for programmatic trading.

    Notes:
    - Polymarket has no email/password registration endpoint like Web2 apps.
    - Account bootstrap is wallet-based: private key -> L1 auth -> L2 creds.
    """

    def __init__(self, vault_path: str = "state/polymarket_accounts.json") -> None:
        self.vault_path = Path(vault_path)
        self.vault_path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, Any]:
        if not self.vault_path.exists():
            return {"accounts": []}
        with open(self.vault_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"accounts": []}
        data.setdefault("accounts", [])
        return data

    def _save(self, data: dict[str, Any]) -> None:
        with open(self.vault_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.chmod(self.vault_path, 0o600)

    @staticmethod
    def _build_client(host: str, chain_id: int, private_key: str):
        try:
            from py_clob_client.client import ClobClient
        except Exception as exc:  # pragma: no cover - import environment dependent
            raise RuntimeError("py_clob_client is required. Install dependency: py-clob-client") from exc
        return ClobClient(host=host, chain_id=chain_id, key=private_key)

    def create_or_derive_account(
        self,
        *,
        name: str,
        private_key: str,
        wallet_address: str,
        funder: str,
        signature_type: int,
        nonce: int = 0,
        chain_id: int = 137,
        host: str = "https://clob.polymarket.com",
        client: ClobLikeClient | None = None,
    ) -> PolymarketAccount:
        c = client or self._build_client(host=host, chain_id=chain_id, private_key=private_key)
        raw = c.create_or_derive_api_creds(nonce=nonce)
        if isinstance(raw, dict):
            api_key = raw.get("apiKey")
            secret = raw.get("secret")
            passphrase = raw.get("passphrase")
        else:
            api_key = getattr(raw, "api_key", None) or getattr(raw, "apiKey", None)
            secret = getattr(raw, "api_secret", None) or getattr(raw, "secret", None)
            passphrase = getattr(raw, "api_passphrase", None) or getattr(raw, "passphrase", None)

        if not api_key or not secret or not passphrase:
            raise RuntimeError("create_or_derive_api_creds returned incomplete credentials")

        creds = PolymarketApiCreds(
            apiKey=str(api_key),
            secret=str(secret),
            passphrase=str(passphrase),
        )
        account = PolymarketAccount(
            name=name,
            chain_id=chain_id,
            host=host,
            signature_type=int(signature_type),
            funder=funder,
            wallet_address=wallet_address,
            nonce=int(nonce),
            creds=creds,
        )

        data = self._load()
        accounts = [a for a in data["accounts"] if str(a.get("name")) != name]
        accounts.append(
            {
                **asdict(account),
                "creds": asdict(account.creds),
            }
        )
        data["accounts"] = accounts
        self._save(data)
        return account

    def list_accounts(self) -> list[PolymarketAccount]:
        data = self._load()
        out: list[PolymarketAccount] = []
        for a in data.get("accounts", []):
            creds_raw = a.get("creds") or {}
            out.append(
                PolymarketAccount(
                    name=str(a.get("name")),
                    chain_id=int(a.get("chain_id", 137)),
                    host=str(a.get("host", "https://clob.polymarket.com")),
                    signature_type=int(a.get("signature_type", 2)),
                    funder=str(a.get("funder", "")),
                    wallet_address=str(a.get("wallet_address", "")),
                    nonce=int(a.get("nonce", 0)),
                    creds=PolymarketApiCreds(
                        apiKey=str(creds_raw.get("apiKey", "")),
                        secret=str(creds_raw.get("secret", "")),
                        passphrase=str(creds_raw.get("passphrase", "")),
                    ),
                )
            )
        return out

    def get_account(self, name: str) -> PolymarketAccount:
        for a in self.list_accounts():
            if a.name == name:
                return a
        raise KeyError(f"account not found: {name}")

    def update_funder(self, *, name: str, funder: str, signature_type: int | None = None) -> PolymarketAccount:
        data = self._load()
        updated = None
        for a in data.get("accounts", []):
            if str(a.get("name")) != name:
                continue
            a["funder"] = funder
            if signature_type is not None:
                a["signature_type"] = int(signature_type)
            updated = a
            break

        if updated is None:
            raise KeyError(f"account not found: {name}")

        self._save(data)
        return self.get_account(name)

    def get_bridge_deposit_addresses(self, funder_address: str, timeout_sec: float = 10.0) -> dict[str, Any]:
        resp = requests.post(
            "https://bridge.polymarket.com/deposit",
            json={"address": funder_address},
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError("unexpected bridge response")
        return data
