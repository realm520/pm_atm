"""Historical trade cache: fetch, persist, and compute P&L statistics.

Cache file (JSON) format:
  {
    "trades": [{"id": ..., "asset_id": ..., "side": ..., "price": ...,
                "size": ..., "match_time": ..., "outcome": ..., "market": ...}, ...],
    "known_ids": [...],
    "last_updated": "ISO8601"
  }
"""
from __future__ import annotations

import collections
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .polymarket_direct_trader import PolymarketDirectTrader
    from .polymarket_account import PolymarketAccount


@dataclass
class HistoricalStats:
    n_completed_trades: int
    realized_pnl: float
    n_winning_trades: int
    last_updated: str

    @classmethod
    def zero(cls) -> "HistoricalStats":
        return cls(n_completed_trades=0, realized_pnl=0.0, n_winning_trades=0, last_updated="")


class TradeHistoryCache:
    """Local cache of Polymarket CLOB trade history with incremental refresh."""

    def __init__(self, cache_path: str = "state/trade_history_cache.json") -> None:
        self.cache_path = Path(cache_path)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_raw(self) -> dict[str, Any]:
        if not self.cache_path.exists():
            return {"trades": [], "known_ids": [], "last_updated": ""}
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return {"trades": [], "known_ids": [], "last_updated": ""}
        if not isinstance(data, dict):
            return {"trades": [], "known_ids": [], "last_updated": ""}
        data.setdefault("trades", [])
        data.setdefault("known_ids", [])
        data.setdefault("last_updated", "")
        return data

    def _save(self, data: dict[str, Any]) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # Refresh (incremental)
    # ------------------------------------------------------------------

    def refresh(
        self,
        trader: "PolymarketDirectTrader",
        account: "PolymarketAccount",
        private_key: str,
    ) -> tuple[int, "HistoricalStats"]:
        """Pull new trades from CLOB, merge into cache, and return stats.

        Returns ``(n_new_trades, HistoricalStats)`` so callers can inject
        stats without a second disk read.
        """
        data = self._load_raw()
        known_ids: set[str] = set(data.get("known_ids", []))

        try:
            raw_trades = trader.get_trades_all(account=account, private_key=private_key)
        except Exception as exc:
            print(f"[trade_history] fetch failed: {exc}", flush=True)
            return 0

        n_new = 0
        for t in raw_trades:
            tid = str(t.get("id") or t.get("trade_id") or "")
            if not tid or tid in known_ids:
                continue
            price = float(t.get("price") or 0)
            size = float(t.get("size") or 0)
            if price <= 0 or size <= 0:
                continue
            known_ids.add(tid)
            data["trades"].append(
                {
                    "id": tid,
                    "asset_id": str(t.get("asset_id") or ""),
                    "side": str(t.get("side") or "").upper(),
                    "price": price,
                    "size": size,
                    "match_time": str(t.get("match_time") or t.get("created_at") or ""),
                    "outcome": str(t.get("outcome") or ""),
                    "market": str(t.get("market") or ""),
                }
            )
            n_new += 1

        data["known_ids"] = list(known_ids)
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._save(data)
        print(
            f"[trade_history] refreshed: +{n_new} new, {len(data['trades'])} total cached"
            f" (last_updated={data['last_updated']})",
            flush=True,
        )
        return n_new, self.compute_stats(data)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def compute_stats(self, _data: dict[str, Any] | None = None) -> HistoricalStats:
        """Compute aggregate P&L statistics from cached trades.

        Each SELL transaction is one "completed trade".  P&L is computed
        per-asset using the average-cost method, consistent with how the
        live runner tracks session P&L.

        Pass ``_data`` to skip a redundant disk read when the caller already
        has the loaded cache (e.g. right after ``refresh()``).
        """
        data = _data if _data is not None else self._load_raw()
        trades: list[dict] = data.get("trades", [])

        by_asset: dict[str, list[dict]] = collections.defaultdict(list)
        for t in trades:
            aid = t.get("asset_id", "")
            if aid:
                by_asset[aid].append(t)

        n_completed = 0
        realized_pnl = 0.0
        n_winning = 0

        for asset_trades in by_asset.values():
            asset_trades.sort(key=lambda x: x.get("match_time", ""))

            total_bought_qty = 0.0
            total_cost_usdc = 0.0

            for t in asset_trades:
                side = t.get("side", "").upper()
                price = float(t.get("price") or 0)
                size = float(t.get("size") or 0)
                if price <= 0 or size <= 0:
                    continue

                if side == "BUY":
                    total_cost_usdc += price * size
                    total_bought_qty += size
                elif side == "SELL":
                    avg_cost = total_cost_usdc / total_bought_qty if total_bought_qty > 0 else 0.0
                    trade_pnl = (price - avg_cost) * size
                    realized_pnl += trade_pnl
                    n_completed += 1
                    if trade_pnl > 0:
                        n_winning += 1
                    # Reduce cost basis proportionally
                    if total_bought_qty > 0:
                        ratio = min(size / total_bought_qty, 1.0)
                        total_cost_usdc *= max(0.0, 1.0 - ratio)
                        total_bought_qty = max(0.0, total_bought_qty - size)

        return HistoricalStats(
            n_completed_trades=n_completed,
            realized_pnl=round(realized_pnl, 4),
            n_winning_trades=n_winning,
            last_updated=data.get("last_updated", ""),
        )
