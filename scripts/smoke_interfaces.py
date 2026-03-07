#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio

from weather_arb.engine import PaperArbEngine
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider
from weather_arb.polymarket import PolymarketClient
from weather_arb.realtime import PollingMarketStreamer, RealtimeConfig


async def smoke_live_ticks(market_id: str, ticks: int) -> None:
    streamer = PollingMarketStreamer(config=RealtimeConfig(poll_interval_sec=1.0))
    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.58),
        config=LiveRunnerConfig(eval_every_ticks=2, out_csv="outputs/live_trades_smoke.csv"),
    )
    counter = {"n": 0}

    async def on_tick(tick):
        counter["n"] += 1
        print(
            f"tick={counter['n']} id={tick.get('id')} px={tick.get('lastTradePrice')} "
            f"bid={tick.get('bestBid')} ask={tick.get('bestAsk')}"
        )
        await runner.on_tick(tick)
        if counter["n"] >= ticks:
            streamer.stop()

    await streamer.stream_market(market_id, on_tick)


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-test Polymarket connectivity + live runner")
    parser.add_argument("--ticks", type=int, default=5)
    parser.add_argument("--market-id", default="")
    args = parser.parse_args()

    client = PolymarketClient()
    market_id = args.market_id

    if not market_id:
        markets = client.list_markets(limit=30, active=True, closed=False)
        if not markets:
            raise RuntimeError("No active open markets found")
        market_id = str(markets[0]["id"])

    market = client.get_market(market_id)
    price = client.market_price(market_id)
    print(f"market_id={market_id} question={market.get('question')} price={price}")

    asyncio.run(smoke_live_ticks(market_id, args.ticks))
    print("smoke ok")


if __name__ == "__main__":
    main()
