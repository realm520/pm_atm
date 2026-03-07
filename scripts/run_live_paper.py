#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from weather_arb.engine import PaperArbEngine
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider, run_async
from weather_arb.realtime import PollingMarketStreamer, PolymarketWSStreamer, RealtimeConfig, WebSocketMarketStreamer
from weather_arb.weather_provider import OpenMeteoConfig, OpenMeteoMultiModelProvider, WeatherEventConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Run live paper trading runner")
    parser.add_argument("--mode", choices=["poll", "ws"], default="poll")
    parser.add_argument("--market-id", help="Required for poll mode")
    parser.add_argument("--market-ids", default="", help="Comma-separated market ids for ws mode")
    parser.add_argument("--all-from-weather-config", action="store_true", help="Use all market ids from --weather-config (ws mode)")
    parser.add_argument("--ws-url", help="Required for ws mode")
    parser.add_argument("--subscribe-json", help='Optional WS subscribe payload JSON string, e.g. {"type":"sub"}')
    parser.add_argument("--eval-every", type=int, default=10)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--out-csv", default="outputs/live_trades.csv")
    parser.add_argument("--summary-csv", default="outputs/live_summary.csv")
    parser.add_argument("--events-jsonl", default="logs/live_events.jsonl")
    parser.add_argument("--error-log", default="logs/live_errors.log")
    parser.add_argument("--run-meta", default="logs/live_run_meta.json")
    parser.add_argument("--max-seconds", type=float, default=0, help="Auto-stop after N seconds (0 = run forever)")
    parser.add_argument("--static-prob", type=float, default=0.55, help="Fallback model probability")
    parser.add_argument("--weather-config", default="", help="JSON file: {market_id: {latitude, longitude, variable, threshold, direction, horizon_hours}}")
    parser.add_argument("--weather-cache-ttl", type=int, default=300, help="Weather forecast cache ttl seconds")
    parser.add_argument("--ws-provider", choices=["generic", "polymarket"], default="polymarket")
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
        forecast_provider = OpenMeteoMultiModelProvider(
            event_map=event_map,
            config=OpenMeteoConfig(cache_ttl_sec=args.weather_cache_ttl),
        )

    for p in [args.out_csv, args.summary_csv, args.events_jsonl, args.error_log, args.run_meta]:
        Path(p).parent.mkdir(parents=True, exist_ok=True)

    run_meta = {
        "mode": args.mode,
        "market_id": args.market_id,
        "market_ids": args.market_ids,
        "all_from_weather_config": args.all_from_weather_config,
        "ws_url": args.ws_url,
        "eval_every": args.eval_every,
        "poll_interval": args.poll_interval,
        "out_csv": args.out_csv,
        "summary_csv": args.summary_csv,
        "events_jsonl": args.events_jsonl,
        "error_log": args.error_log,
        "max_seconds": args.max_seconds,
        "weather_config": args.weather_config,
        "weather_cache_ttl": args.weather_cache_ttl,
        "ws_provider": args.ws_provider,
    }
    with open(args.run_meta, "w", encoding="utf-8") as f:
        json.dump(run_meta, f, ensure_ascii=False, indent=2)

    runner = LivePaperRunner(
        engine=engine,
        forecast_provider=forecast_provider,
        config=LiveRunnerConfig(
            eval_every_ticks=args.eval_every,
            out_csv=args.out_csv,
            summary_csv=args.summary_csv,
            events_jsonl=args.events_jsonl,
            error_log=args.error_log,
        ),
    )

    if args.mode == "poll":
        if not args.market_id:
            raise ValueError("--market-id is required in poll mode")
        streamer = PollingMarketStreamer(config=RealtimeConfig(poll_interval_sec=args.poll_interval))
        max_seconds = args.max_seconds if args.max_seconds and args.max_seconds > 0 else None
        run_async(runner.run_polling(streamer, args.market_id, max_seconds=max_seconds))
        return

    max_seconds = args.max_seconds if args.max_seconds and args.max_seconds > 0 else None

    market_ids = [m.strip() for m in args.market_ids.split(",") if m.strip()]
    if args.all_from_weather_config:
        if not args.weather_config:
            raise ValueError("--all-from-weather-config requires --weather-config")
        with open(args.weather_config, "r", encoding="utf-8") as f:
            _raw_cfg = json.load(f)
        market_ids = sorted(str(k) for k in _raw_cfg.keys())

    if args.ws_provider == "polymarket":
        if not market_ids:
            if args.market_id:
                market_ids = [args.market_id]
            else:
                raise ValueError("--market-ids or --market-id is required in ws mode for polymarket")

        ws_url = args.ws_url or "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        subscribe_message = json.loads(args.subscribe_json) if args.subscribe_json else None
        ws_streamer = PolymarketWSStreamer(market_ids=market_ids, ws_url=ws_url, subscribe_message=subscribe_message)
        run_async(runner.run_ws(ws_streamer, max_seconds=max_seconds))
        return

    if not args.ws_url:
        raise ValueError("--ws-url is required in ws mode for generic provider")

    subscribe_message = None
    if args.subscribe_json:
        subscribe_message = json.loads(args.subscribe_json)

    ws_streamer = WebSocketMarketStreamer(ws_url=args.ws_url, subscribe_message=subscribe_message)
    run_async(runner.run_ws(ws_streamer, max_seconds=max_seconds))


if __name__ == "__main__":
    main()
