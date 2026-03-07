#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, TypeVar

from weather_arb.engine import EngineConfig, PaperArbEngine
from weather_arb.execution import ExecutionConfig
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider, run_async
from weather_arb.polymarket import PolymarketClient
from weather_arb.realtime import PollingMarketStreamer, PolymarketWSStreamer, RealtimeConfig, WebSocketMarketStreamer
from weather_arb.risk import RiskConfig
from weather_arb.strategy import StrategyConfig
from weather_arb.weather_provider import OpenMeteoConfig, OpenMeteoMultiModelProvider, WeatherEventConfig


T = TypeVar("T")


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a JSON object: {path}")
    return data


def _load_dataclass_config(path: str | None, cls: type[T]) -> T | None:
    if not path:
        return None
    raw = _load_json(path)
    return cls(**raw)


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
    parser.add_argument("--ws-raw-log", default="logs/live_ws_raw.jsonl", help="Raw WS messages for diagnostics")
    parser.add_argument("--max-seconds", type=float, default=0, help="Auto-stop after N seconds (0 = run forever)")
    parser.add_argument("--static-prob", type=float, default=0.55, help="Fallback model probability")
    parser.add_argument("--weather-config", default="", help="JSON file: {market_id: {latitude, longitude, variable, threshold, direction, horizon_hours}}")
    parser.add_argument("--weather-cache-ttl", type=int, default=300, help="Weather forecast cache ttl seconds")
    parser.add_argument("--ws-provider", choices=["generic", "polymarket"], default="polymarket")
    parser.add_argument("--strategy-config", default="", help="JSON file for StrategyConfig overrides")
    parser.add_argument("--risk-config", default="", help="JSON file for RiskConfig overrides")
    parser.add_argument("--execution-config", default="", help="JSON file for ExecutionConfig overrides")
    parser.add_argument("--engine-config", default="", help="JSON file for EngineConfig overrides")
    args = parser.parse_args()

    strategy_cfg = _load_dataclass_config(args.strategy_config or None, StrategyConfig)
    risk_cfg = _load_dataclass_config(args.risk_config or None, RiskConfig)
    execution_cfg = _load_dataclass_config(args.execution_config or None, ExecutionConfig)
    engine_cfg = _load_dataclass_config(args.engine_config or None, EngineConfig)

    engine = PaperArbEngine(
        strategy_cfg=strategy_cfg,
        risk_cfg=risk_cfg,
        execution_cfg=execution_cfg,
        engine_cfg=engine_cfg,
    )

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

    for p in [args.out_csv, args.summary_csv, args.events_jsonl, args.error_log, args.run_meta, args.ws_raw_log]:
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
        "ws_raw_log": args.ws_raw_log,
        "strategy_config": args.strategy_config,
        "risk_config": args.risk_config,
        "execution_config": args.execution_config,
        "engine_config": args.engine_config,
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

        client = PolymarketClient()
        asset_ids: list[str] = []
        asset_to_market_id: dict[str, str] = {}
        condition_to_market_id: dict[str, str] = {}

        for mid in market_ids:
            m = client.get_market(str(mid))
            raw_tokens = m.get("clobTokenIds")
            tokens: list[str] = []
            if isinstance(raw_tokens, str) and raw_tokens:
                tokens = [str(x) for x in json.loads(raw_tokens)]
            elif isinstance(raw_tokens, list):
                tokens = [str(x) for x in raw_tokens]

            for t in tokens:
                asset_ids.append(t)
                asset_to_market_id[t] = str(mid)

            cond = m.get("conditionId")
            if cond:
                condition_to_market_id[str(cond).lower()] = str(mid)

        if not asset_ids:
            raise ValueError("No clobTokenIds found for selected market ids")

        ws_url = args.ws_url or "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        subscribe_message = json.loads(args.subscribe_json) if args.subscribe_json else None
        ws_streamer = PolymarketWSStreamer(
            asset_ids=sorted(set(asset_ids)),
            ws_url=ws_url,
            subscribe_message=subscribe_message,
            asset_to_market_id=asset_to_market_id,
            condition_to_market_id=condition_to_market_id,
            debug_raw_path=args.ws_raw_log,
        )
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
