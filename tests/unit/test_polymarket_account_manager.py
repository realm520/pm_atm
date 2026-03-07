from __future__ import annotations

from weather_arb.polymarket_account import PolymarketAccountManager


class FakeClient:
    def create_or_derive_api_creds(self, nonce: int = 0):
        return {
            "apiKey": f"k-{nonce}",
            "secret": "s-1",
            "passphrase": "p-1",
        }


def test_account_manager_create_and_list(tmp_path) -> None:
    vault = tmp_path / "accounts.json"
    m = PolymarketAccountManager(str(vault))
    acct = m.create_or_derive_account(
        name="main",
        private_key="0xabc",
        wallet_address="0xw",
        funder="0xf",
        signature_type=2,
        nonce=7,
        chain_id=137,
        host="https://clob.polymarket.com",
        client=FakeClient(),
    )
    assert acct.name == "main"
    assert acct.creds.apiKey == "k-7"

    all_acc = m.list_accounts()
    assert len(all_acc) == 1
    assert all_acc[0].name == "main"
    assert all_acc[0].creds.secret == "s-1"
