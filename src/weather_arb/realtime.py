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
    """Polymarket market channel streamer (asset/token subscriptions)."""

    def __init__(
        self,
        asset_ids: list[str],
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        subscribe_message: dict[str, Any] | None = None,
        config: RealtimeConfig | None = None,
        asset_to_market_id: dict[str, str] | None = None,
        condition_to_market_id: dict[str, str] | None = None,
        debug_raw_path: str | None = None,
        subscribe_chunk_size: int = 200,
    ) -> None:
        self.asset_ids = [str(a) for a in asset_ids]
        self.asset_to_market_id = asset_to_market_id or {}
        self.condition_to_market_id = {str(k).lower(): str(v) for k, v in (condition_to_market_id or {}).items()}
        self.debug_raw_path = debug_raw_path
        self.subscribe_chunk_size = max(1, int(subscribe_chunk_size))
        self._custom_subscribe_message = subscribe_message
        default_sub = {
            "assets_ids": self.asset_ids,
            "type": "market",
            "initial_dump": True,
            "level": 2,
            "custom_feature_enabled": True,
        }
        super().__init__(ws_url=ws_url, subscribe_message=subscribe_message or default_sub, config=config)

    def _append_raw(self, message: str) -> None:
        if not self.debug_raw_path:
            return
        from pathlib import Path

        p = Path(self.debug_raw_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(message)
            f.write("\n")

    def _normalize_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        event_type = str(payload.get("event_type") or "")
        out: list[dict[str, Any]] = []

        if event_type == "price_change":
            market_cond = str(payload.get("market") or "").lower()
            mapped_market_id = self.condition_to_market_id.get(market_cond)
            ts = payload.get("timestamp")
            for ch in payload.get("price_changes", []):
                asset_id = str(ch.get("asset_id") or "")
                if not asset_id:
                    continue
                market_id = mapped_market_id or self.asset_to_market_id.get(asset_id)
                if not market_id:
                    continue
                price = ch.get("price")
                if price is None:
                    continue
                out.append(
                    {
                        "id": str(market_id),
                        "price": float(price),
                        "bestBid": ch.get("best_bid"),
                        "bestAsk": ch.get("best_ask"),
                        "timestamp": ts,
                        "asset_id": asset_id,
                        "event_type": event_type,
                    }
                )
            return out

        if event_type in {"book", "last_trade_price", "best_bid_ask"}:
            asset_id = str(payload.get("asset_id") or "")
            if not asset_id:
                return out
            market_id = self.asset_to_market_id.get(asset_id)
            if not market_id:
                return out

            price = payload.get("price")
            if price is None:
                if payload.get("bids"):
                    price = payload["bids"][0].get("price")
                if price is None and payload.get("asks"):
                    price = payload["asks"][0].get("price")
            if price is None:
                return out

            out.append(
                {
                    "id": str(market_id),
                    "price": float(price),
                    "bestBid": payload.get("best_bid"),
                    "bestAsk": payload.get("best_ask"),
                    "timestamp": payload.get("timestamp"),
                    "asset_id": asset_id,
                    "event_type": event_type,
                }
            )
            return out

        return out

    async def _ping_loop(self, ws) -> None:
        while not self._stopped:
            await asyncio.sleep(10)
            if self._stopped:
                break
            try:
                await ws.send("PING")
            except Exception:
                break

    async def stream(self, on_tick: TickHandler) -> None:
        self._stopped = False
        while not self._stopped:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    if self._custom_subscribe_message:
                        await ws.send(json.dumps(self._custom_subscribe_message))
                    else:
                        first = self.asset_ids[: self.subscribe_chunk_size]
                        if first:
                            await ws.send(
                                json.dumps(
                                    {
                                        "assets_ids": first,
                                        "type": "market",
                                        "initial_dump": True,
                                        "level": 2,
                                        "custom_feature_enabled": True,
                                    }
                                )
                            )
                        remain = self.asset_ids[self.subscribe_chunk_size :]
                        for i in range(0, len(remain), self.subscribe_chunk_size):
                            chunk = remain[i : i + self.subscribe_chunk_size]
                            await ws.send(json.dumps({"operation": "subscribe", "assets_ids": chunk, "level": 2, "custom_feature_enabled": True}))
                            await asyncio.sleep(0.05)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for message in ws:
                            if self._stopped:
                                break
                            if isinstance(message, bytes):
                                continue
                            if message == "PONG":
                                continue
                            self._append_raw(message)
                            try:
                                payload = json.loads(message)
                            except json.JSONDecodeError:
                                continue

                            payloads: list[dict[str, Any]] = []
                            if isinstance(payload, dict):
                                payloads = [payload]
                            elif isinstance(payload, list):
                                payloads = [p for p in payload if isinstance(p, dict)]
                            else:
                                continue

                            for p in payloads:
                                for tick in self._normalize_payload(p):
                                    await on_tick(tick)
                    finally:
                        ping_task.cancel()
            except (OSError, WebSocketException):
                await asyncio.sleep(self.cfg.reconnect_delay_sec)
