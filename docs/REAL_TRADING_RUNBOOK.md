# Real Trading Runbook

## 1) 环境变量

```bash
export POLY_EXEC_BASE_URL='https://<your-trading-gateway>'
export POLY_EXEC_API_KEY='<your-api-key>'
export TG_BOT_TOKEN='<telegram-bot-token>'
```

## 2) 启动（保守参数 + 监控 topic=52）

```bash
uv run python scripts/run_live_paper.py --mode ws \
  --weather-config config/weather_events.generated.json \
  --all-from-weather-config \
  --strategy-config config/strategy.prod.conservative.json \
  --risk-config config/risk.prod.conservative.json \
  --engine-config config/engine.prod.conservative.json \
  --hard-daily-loss-limit -12 \
  --max-runtime-errors 50 \
  --kill-switch-path /tmp/weather_arb.stop \
  --alerts-jsonl logs/live_alerts_ws_all_6h.jsonl \
  --telegram-bot-token "$TG_BOT_TOKEN" \
  --telegram-chat-id -1003837508045 \
  --telegram-thread-id 52 \
  --max-seconds 21600
```

## 3) 紧急停机

```bash
touch /tmp/weather_arb.stop
```

## 4) 核对

- `logs/live_alerts_ws_all_6h.jsonl`
- `logs/live_errors_ws_all_6h.log`
- `outputs/live_summary_ws_all_6h.csv`
