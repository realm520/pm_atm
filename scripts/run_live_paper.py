#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, TypeVar

from weather_arb.engine import EngineConfig, PaperArbEngine
from weather_arb.execution import ExecutionConfig
from weather_arb.execution_service import ExecutionService
from weather_arb.exchange_sim import SimExchangeExecutor
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, run_async
from weather_arb.order_store import SqliteOrderStore
from weather_arb.polymarket import PolymarketClient
from weather_arb.polymarket_account import PolymarketAccountManager
from weather_arb.polymarket_executor import PolymarketExecutionConfig, PolymarketLiveExecutor
from weather_arb.polymarket_sdk_executor import PolymarketSdkExecutor
from weather_arb.realtime import PollingMarketStreamer, PolymarketWSStreamer, RealtimeConfig, WebSocketMarketStreamer
from weather_arb.risk import RiskConfig
from weather_arb.strategy import StrategyConfig
from weather_arb.strategy_premarket_no import PremarketNoConfig, PremarketNoLadderStrategy
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
    parser.add_argument("--alerts-jsonl", default="logs/live_alerts.jsonl")
    parser.add_argument("--kill-switch-path", default="", help="If file exists, runner halts gracefully")
    parser.add_argument("--hard-daily-loss-limit", type=float, default=-12.0, help="Circuit breaker when total_pnl <= this value")
    parser.add_argument("--no-daily-loss-circuit-breaker", action="store_true", help="Disable the daily loss circuit breaker")
    parser.add_argument("--max-runtime-errors", type=int, default=50, help="Circuit breaker after N on_tick runtime errors")
    parser.add_argument("--alert-cooldown-sec", type=float, default=120.0, help="Min seconds between identical alert codes")
    parser.add_argument("--telegram-bot-token", default="", help="Optional Telegram bot token for runtime alerts")
    parser.add_argument("--telegram-chat-id", default="", help="Optional Telegram chat id for runtime alerts")
    parser.add_argument("--telegram-thread-id", type=int, default=0, help="Optional Telegram topic(thread) id for alerts")
    parser.add_argument("--ws-raw-log", default="logs/live_ws_raw.jsonl", help="Raw WS messages for diagnostics")
    parser.add_argument("--max-seconds", type=float, default=0, help="Auto-stop after N seconds (0 = run forever)")
    parser.add_argument("--weather-config", required=True, help="JSON file: {market_id: {latitude, longitude, variable, threshold, direction, horizon_hours}}")
    parser.add_argument("--weather-cache-ttl", type=int, default=300, help="Weather forecast cache ttl seconds")
    parser.add_argument("--ws-provider", choices=["generic", "polymarket"], default="polymarket")
    parser.add_argument("--strategy-kind", choices=["weather", "premarket-no"], default="weather")
    parser.add_argument("--strategy-config", default="", help="JSON file for StrategyConfig overrides")
    parser.add_argument("--premarket-strategy-config", default="", help="JSON file for PremarketNoConfig overrides")
    parser.add_argument("--risk-config", default="", help="JSON file for RiskConfig overrides")
    parser.add_argument("--execution-config", default="", help="JSON file for ExecutionConfig overrides")
    parser.add_argument("--engine-config", default="", help="JSON file for EngineConfig overrides")
    parser.add_argument("--execution-mode", choices=["paper", "live-sim", "live", "live-sdk"], default="paper")
    parser.add_argument("--orders-db", default="state/orders.db", help="Order state sqlite path")
    parser.add_argument("--poly-exec-base-url", default="", help="Live execution gateway base URL")
    parser.add_argument("--poly-exec-api-key", default="", help="Live execution API key")
    parser.add_argument("--poly-account-name", default="", help="Account name in vault for live-sdk mode")
    parser.add_argument("--poly-account-vault", default="state/polymarket_accounts.json", help="Account vault path for live-sdk mode")
    parser.add_argument("--liquidate-on-startup", action="store_true", help="On startup, immediately SELL all existing positions instead of bootstrapping them")
    args = parser.parse_args()

    strategy_cfg = _load_dataclass_config(args.strategy_config or None, StrategyConfig)
    premarket_strategy_cfg = _load_dataclass_config(args.premarket_strategy_config or None, PremarketNoConfig)
    risk_cfg = _load_dataclass_config(args.risk_config or None, RiskConfig)
    execution_cfg = _load_dataclass_config(args.execution_config or None, ExecutionConfig)
    engine_cfg = _load_dataclass_config(args.engine_config or None, EngineConfig)

    strategy_impl = None
    if args.strategy_kind == "premarket-no":
        strategy_impl = PremarketNoLadderStrategy(premarket_strategy_cfg or PremarketNoConfig())

    engine = PaperArbEngine(
        strategy_cfg=strategy_cfg,
        risk_cfg=risk_cfg,
        execution_cfg=execution_cfg,
        engine_cfg=engine_cfg,
        strategy=strategy_impl,
    )

    raw = _load_json(args.weather_config)
    event_map = {
        str(k): WeatherEventConfig(**v)
        for k, v in raw.items()
    }
    forecast_provider = OpenMeteoMultiModelProvider(
        event_map=event_map,
        config=OpenMeteoConfig(cache_ttl_sec=args.weather_cache_ttl),
    )

    for p in [args.out_csv, args.summary_csv, args.events_jsonl, args.error_log, args.run_meta, args.alerts_jsonl, args.ws_raw_log]:
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
        "alerts_jsonl": args.alerts_jsonl,
        "kill_switch_path": args.kill_switch_path,
        "hard_daily_loss_limit": args.hard_daily_loss_limit,
        "max_runtime_errors": args.max_runtime_errors,
        "alert_cooldown_sec": args.alert_cooldown_sec,
        "telegram_alert_enabled": bool(args.telegram_bot_token and args.telegram_chat_id),
        "telegram_thread_id": args.telegram_thread_id,
        "max_seconds": args.max_seconds,
        "weather_config": args.weather_config,
        "weather_cache_ttl": args.weather_cache_ttl,
        "ws_provider": args.ws_provider,
        "ws_raw_log": args.ws_raw_log,
        "strategy_kind": args.strategy_kind,
        "strategy_config": args.strategy_config,
        "premarket_strategy_config": args.premarket_strategy_config,
        "risk_config": args.risk_config,
        "execution_config": args.execution_config,
        "engine_config": args.engine_config,
        "execution_mode": args.execution_mode,
        "orders_db": args.orders_db,
        "poly_exec_base_url": bool(args.poly_exec_base_url),
        "poly_account_name": args.poly_account_name,
        "poly_account_vault": args.poly_account_vault,
    }
    with open(args.run_meta, "w", encoding="utf-8") as f:
        json.dump(run_meta, f, ensure_ascii=False, indent=2)

    execution_service = None
    if args.execution_mode in {"live", "live-sim", "live-sdk"}:
        print(f"[startup] execution_mode={args.execution_mode}, initializing order store at {args.orders_db}", flush=True)
        store = SqliteOrderStore(args.orders_db)
        if args.execution_mode == "live-sim":
            execution_service = ExecutionService(store=store, exchange=SimExchangeExecutor(fill_after_sec=0.2))
        elif args.execution_mode == "live-sdk":
            if not args.poly_account_name:
                raise ValueError("--poly-account-name is required in --execution-mode live-sdk")
            private_key = os.environ.get("POLY_PRIVATE_KEY", "")
            if not private_key:
                raise ValueError("POLY_PRIVATE_KEY env is required in --execution-mode live-sdk")
            print(f"[startup] loading account={args.poly_account_name} from vault={args.poly_account_vault}", flush=True)
            account = PolymarketAccountManager(args.poly_account_vault).get_account(args.poly_account_name)
            print(f"[startup] account loaded: name={account.name} wallet={account.wallet_address} chain={account.chain_id}", flush=True)
            execution_service = ExecutionService(store=store, exchange=PolymarketSdkExecutor(account=account, private_key=private_key))
            print(f"[startup] live-sdk executor ready", flush=True)
        else:
            if not args.poly_exec_base_url:
                raise ValueError("--poly-exec-base-url is required in --execution-mode live")
            execution_service = ExecutionService(
                store=store,
                exchange=PolymarketLiveExecutor(
                    PolymarketExecutionConfig(
                        base_url=args.poly_exec_base_url,
                        api_key=args.poly_exec_api_key,
                    )
                ),
            )

    runner = LivePaperRunner(
        engine=engine,
        forecast_provider=forecast_provider,
        config=LiveRunnerConfig(
            eval_every_ticks=args.eval_every,
            out_csv=args.out_csv,
            summary_csv=args.summary_csv,
            events_jsonl=args.events_jsonl,
            error_log=args.error_log,
            alerts_jsonl=args.alerts_jsonl,
            kill_switch_path=args.kill_switch_path,
            hard_daily_loss_limit=args.hard_daily_loss_limit,
            enable_daily_loss_circuit_breaker=not args.no_daily_loss_circuit_breaker,
            max_runtime_errors=args.max_runtime_errors,
            alert_cooldown_sec=args.alert_cooldown_sec,
            telegram_bot_token=args.telegram_bot_token,
            telegram_chat_id=args.telegram_chat_id,
            telegram_thread_id=args.telegram_thread_id,
        ),
        execution_service=execution_service,
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
        market_ids = sorted(event_map.keys())

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
        market_yes_no: dict[str, tuple[str, str]] = {}

        print(f"[startup] fetching market data for {len(market_ids)} markets: {market_ids[:5]}{'...' if len(market_ids)>5 else ''}", flush=True)
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

            # clobTokenIds[0]=YES, clobTokenIds[1]=NO（Polymarket 约定）
            if len(tokens) >= 2:
                market_yes_no[str(mid)] = (tokens[0], tokens[1])
                print(f"[startup] market={mid} yes={tokens[0][:16]}... no={tokens[1][:16]}...", flush=True)

            cond = m.get("conditionId")
            if cond:
                condition_to_market_id[str(cond).lower()] = str(mid)

        if not asset_ids:
            raise ValueError("No clobTokenIds found for selected market ids")

        print(f"[startup] resolved {len(asset_ids)} asset_ids, {len(market_yes_no)} yes/no pairs from {len(market_ids)} markets", flush=True)

        # 把 NO token 映射注入 runner（runner 在 WS section 之前创建，此处补充）
        runner.event_no_asset_id.update({k: v[1] for k, v in market_yes_no.items()})

        # Bootstrap or liquidate existing positions from exchange at startup
        _executor = getattr(execution_service, "exchange", None)
        if _executor is not None and hasattr(_executor, "get_positions_snapshot"):
            print("[startup] querying existing open positions from exchange...", flush=True)
            try:
                _snapshots = _executor.get_positions_snapshot(asset_ids=asset_ids)
                if args.liquidate_on_startup:
                    print("[startup] --liquidate-on-startup: closing all existing positions...", flush=True)
                    runner.liquidate_positions_at_startup(_snapshots, asset_to_market_id, market_yes_no)
                else:
                    runner.bootstrap_positions_from_snapshot(_snapshots, asset_to_market_id, market_yes_no)
            except Exception as _exc:
                print(f"[startup] position snapshot failed (non-fatal): {_exc}", flush=True)
        else:
            print("[startup] position bootstrap skipped (paper mode or executor does not support get_positions_snapshot)", flush=True)

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
