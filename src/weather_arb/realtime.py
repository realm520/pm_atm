from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import requests
import websockets
from websockets.exceptions import WebSocketException

from .polymarket import PolymarketClient

TickHandler = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class RealtimeConfig:
    poll_interval_sec: float = 2.0
    reconnect_delay_sec: float = 2.0


class PollingMarketStreamer:
    """Realtime-like streamer via short-interval polling."""

    def __init__(self, client: PolymarketClient | None = None, config: RealtimeConfig | None = None) -> None:
        self.client = client or PolymarketClient()
        self.cfg = config or RealtimeConfig()
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    async def stream_market(self, market_id: str, on_tick: TickHandler) -> None:
        self._stopped = False
        while not self._stopped:
            try:
                market = self.client.get_market(market_id)
                await on_tick(market)
            except requests.RequestException:
                pass
            await asyncio.sleep(self.cfg.poll_interval_sec)


class WebSocketMarketStreamer:
    """Generic websocket streamer.

    You must pass a `ws_url` and optional `subscribe_message` matching your provider.
    """

    def __init__(self, ws_url: str, subscribe_message: dict[str, Any] | None = None, config: RealtimeConfig | None = None) -> None:
        self.ws_url = ws_url
        self.subscribe_message = subscribe_message
        self.cfg = config or RealtimeConfig()
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    async def stream(self, on_tick: TickHandler) -> None:
        self._stopped = False
        while not self._stopped:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    if self.subscribe_message:
                        await ws.send(json.dumps(self.subscribe_message))

                    async for message in ws:
                        if self._stopped:
                            break
                        try:
                            payload = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(payload, dict):
                            await on_tick(payload)
            except (OSError, WebSocketException):
                await asyncio.sleep(self.cfg.reconnect_delay_sec)
