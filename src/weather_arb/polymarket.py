from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class PolymarketConfig:
    base_url: str = "https://gamma-api.polymarket.com"
    timeout_sec: int = 10


class PolymarketClient:
    """Lightweight read-only Polymarket client for market discovery and snapshots."""

    def __init__(self, config: PolymarketConfig | None = None) -> None:
        self.cfg = config or PolymarketConfig()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(url, params=params, timeout=self.cfg.timeout_sec)
        resp.raise_for_status()
        return resp.json()

    def list_markets(self, limit: int = 50, active: bool = True) -> list[dict[str, Any]]:
        params = {"limit": limit, "active": str(active).lower()}
        data = self._get("markets", params=params)
        return data if isinstance(data, list) else data.get("markets", [])

    def get_market(self, market_id: str) -> dict[str, Any]:
        return self._get(f"markets/{market_id}")

    def market_price(self, market_id: str) -> float:
        market = self.get_market(market_id)
        # 兼容不同字段名
        for key in ("lastTradePrice", "last_trade_price", "outcomePrice", "price"):
            if key in market:
                return float(market[key])
        raise ValueError(f"No price field found for market {market_id}")
