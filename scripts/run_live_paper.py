#!/usr/bin/env python3
from __future__ import annotations

import argparse

import json

from weather_arb.engine import PaperArbEngine
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider, run_async
from weather_arb.realtime import PollingMarketStreamer, RealtimeConfig, WebSocketMarketStreamer
from weather_arb.weather_provider import OpenMeteoMultiModelProvider, WeatherEventConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Run live paper trading runner")
    parser.add_argument("--mode", choices=["poll", "ws"], default="poll")
    parser.add_argument("--market-id", help="Required for poll mode")
    parser.add_argument("--ws-url", help="Required for ws mode")
    parser.add_argument("--subscribe-json", help='Optional WS subscribe payload JSON string, e.g. {"type":"sub"}')
    parser.add_argument("--eval-every", type=int, default=10)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--out-csv", default="outputs/live_trades.csv")
    parser.add_argument("--static-prob", type=float, default=0.55, help="Fallback model probability")
    parser.add_argument("--weather-config", default="", help="JSON file: {market_id: {latitude, longitude, variable, threshold, direction, horizon_hours}}")
    args = parser.parse_args()

    engine = PaperArbEngine()

    forecast_provider = StaticForecastProvider(args.static_prob)
    if args.weather_config:
        with open(args.weather_config, "r", encoding="utf-8") as f:
            raw = json.load(f)
        event_map = {
            str(k): WeatherEventConfig(**v)
            for k, v in raw.items()
        }
        forecast_provider = OpenMeteoMultiModelProvider(event_map=event_map)

    runner = LivePaperRunner(
        engine=engine,
        forecast_provider=forecast_provider,
        config=LiveRunnerConfig(eval_every_ticks=args.eval_every, out_csv=args.out_csv),
    )

    if args.mode == "poll":
        if not args.market_id:
            raise ValueError("--market-id is required in poll mode")
        streamer = PollingMarketStreamer(config=RealtimeConfig(poll_interval_sec=args.poll_interval))
        run_async(runner.run_polling(streamer, args.market_id))
        return

    if not args.ws_url:
        raise ValueError("--ws-url is required in ws mode")

    subscribe_message = None
    if args.subscribe_json:
        import json

        subscribe_message = json.loads(args.subscribe_json)

    ws_streamer = WebSocketMarketStreamer(ws_url=args.ws_url, subscribe_message=subscribe_message)
    run_async(runner.run_ws(ws_streamer))


if __name__ == "__main__":
    main()
