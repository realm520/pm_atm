from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import requests

from .polymarket import PolymarketClient


@dataclass(frozen=True)
class RealtimeConfig:
    poll_interval_sec: float = 2.0


class MarketDataStreamer:
    """Realtime-like streamer via short-interval polling.

    Note: Kept as polling to avoid hard-coding unstable websocket endpoints.
    You can later replace this class with an actual WS transport while keeping the same callback API.
    """

    def __init__(self, client: PolymarketClient | None = None, config: RealtimeConfig | None = None) -> None:
        self.client = client or PolymarketClient()
        self.cfg = config or RealtimeConfig()
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    async def stream_market(self, market_id: str, on_tick) -> None:
        self._stopped = False
        while not self._stopped:
            try:
                market = self.client.get_market(market_id)
                await on_tick(market)
            except requests.RequestException:
                # network blip: keep loop alive
                pass
            await asyncio.sleep(self.cfg.poll_interval_sec)

    async def stream_many(self, market_ids: list[str], on_tick) -> None:
        tasks = [asyncio.create_task(self.stream_market(m, on_tick)) for m in market_ids]
        await asyncio.gather(*tasks)
