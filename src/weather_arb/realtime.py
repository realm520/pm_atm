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


class PolymarketWSStreamer(WebSocketMarketStreamer):
    """Polymarket-specific websocket streamer with multi-market subscription and payload normalization."""

    def __init__(
        self,
        market_ids: list[str],
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        subscribe_message: dict[str, Any] | None = None,
        config: RealtimeConfig | None = None,
    ) -> None:
        self.market_ids = [str(m) for m in market_ids]
        default_sub = {
            "type": "subscribe",
            "channel": "market",
            "market_ids": self.market_ids,
        }
        super().__init__(ws_url=ws_url, subscribe_message=subscribe_message or default_sub, config=config)

    @staticmethod
    def _normalize_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []

        if "data" in payload and isinstance(payload["data"], list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    candidates.append(item)
        elif "data" in payload and isinstance(payload["data"], dict):
            candidates.append(payload["data"])
        else:
            candidates.append(payload)

        out: list[dict[str, Any]] = []
        for c in candidates:
            market_id = c.get("market_id") or c.get("marketId") or c.get("id")
            if market_id is None:
                continue

            price = c.get("price")
            if price is None:
                for k in ("lastTradePrice", "last_trade_price", "outcomePrice"):
                    if c.get(k) is not None:
                        price = c.get(k)
                        break
            if price is None:
                continue

            out.append(
                {
                    "id": str(market_id),
                    "price": float(price),
                    "bestBid": c.get("bestBid") or c.get("best_bid"),
                    "bestAsk": c.get("bestAsk") or c.get("best_ask"),
                    "timestamp": c.get("timestamp") or c.get("ts"),
                }
            )
        return out

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

                        if not isinstance(payload, dict):
                            continue

                        for tick in self._normalize_payload(payload):
                            if self.market_ids and tick["id"] not in self.market_ids:
                                continue
                            await on_tick(tick)
            except (OSError, WebSocketException):
                await asyncio.sleep(self.cfg.reconnect_delay_sec)
