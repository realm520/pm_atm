#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PID_FILE="logs/live_prod.pid"
LOG_FILE="logs/live_prod.out.log"
ERR_FILE="logs/live_prod.err.log"
STOP_FILE="/tmp/weather_arb.stop"

TG_CHAT_ID_DEFAULT="-1003837508045"
TG_THREAD_ID_DEFAULT="52"

usage() {
  cat <<'EOF'
Usage: scripts/run_live_prod.sh <check|preflight|smoke|start|stop|status|health>

Environment variables required for start/check:
  POLY_PRIVATE_KEY     Private key for official SDK live execution
  POLY_ACCOUNT_NAME    Account name in state/polymarket_accounts.json
  TG_BOT_TOKEN         Telegram bot token for alerts

Optional environment variables:
  POLY_ACCOUNT_VAULT   Default: state/polymarket_accounts.json
  TG_CHAT_ID           Default: -1003837508045
  TG_THREAD_ID         Default: 52
  LIVE_MAX_SECONDS     Default: 21600
  MIN_USDC             Default: 1
  REQUIRE_ALLOWANCE    Default: 1 (1=must have allowance)
  AUTO_APPROVE_ALLOWANCE Default: 0 (1=try update allowance)
  SMOKE_TOKEN_ID       Token id for smoke order command
EOF
}

ensure_logs_dir() {
  mkdir -p logs outputs state
}

check_env() {
  local missing=0
  for k in POLY_PRIVATE_KEY POLY_ACCOUNT_NAME TG_BOT_TOKEN; do
    if [[ -z "${!k:-}" ]]; then
      echo "[check] missing env: $k"
      missing=1
    fi
  done

  if [[ ! -f config/weather_events.generated.json ]]; then
    echo "[check] missing config/weather_events.generated.json"
    missing=1
  fi

  if [[ "$missing" -ne 0 ]]; then
    echo "[check] FAILED"
    exit 1
  fi
  echo "[check] OK"
}

is_running() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

preflight_live() {
  check_env
  local extra=()
  if [[ "${REQUIRE_ALLOWANCE:-1}" == "1" ]]; then
    extra+=(--require-allowance)
  fi
  if [[ "${AUTO_APPROVE_ALLOWANCE:-0}" == "1" ]]; then
    extra+=(--auto-approve-allowance)
  fi

  uv run python scripts/preflight_live.py \
    --account-name "${POLY_ACCOUNT_NAME}" \
    --vault "${POLY_ACCOUNT_VAULT:-state/polymarket_accounts.json}" \
    --min-usdc "${MIN_USDC:-1}" \
    --require-unblocked \
    "${extra[@]}"
}

smoke_live() {
  check_env
  local extra=()
  if [[ -n "${SMOKE_TOKEN_ID:-}" ]]; then
    extra+=(--token-id "${SMOKE_TOKEN_ID}")
  fi
  uv run python scripts/smoke_real_order.py \
    --account-name "${POLY_ACCOUNT_NAME}" \
    --vault "${POLY_ACCOUNT_VAULT:-state/polymarket_accounts.json}" \
    --price "${SMOKE_PRICE:-0.01}" \
    --size "${SMOKE_SIZE:-5}" \
    --side "${SMOKE_SIDE:-BUY}" \
    "${extra[@]}"
}

health_live() {
  uv run python scripts/live_health_report.py --orders-db state/orders.live.db --minutes "${HEALTH_WINDOW_MIN:-5}"
}

start_live() {
  ensure_logs_dir
  check_env
  preflight_live

  if is_running; then
    echo "[start] already running: pid=$(cat "$PID_FILE")"
    exit 0
  fi

  rm -f "$STOP_FILE"

  local tg_chat_id="${TG_CHAT_ID:-$TG_CHAT_ID_DEFAULT}"
  local tg_thread_id="${TG_THREAD_ID:-$TG_THREAD_ID_DEFAULT}"
  local max_seconds="${LIVE_MAX_SECONDS:-0}"

  nohup uv run python -u scripts/run_live_paper.py \
    --mode ws \
    --execution-mode live-sdk \
    --orders-db state/orders.live.db \
    --poly-account-name "${POLY_ACCOUNT_NAME}" \
    --poly-account-vault "${POLY_ACCOUNT_VAULT:-state/polymarket_accounts.json}" \
    --weather-config config/weather_events.generated.json \
    --all-from-weather-config \
    --strategy-config config/strategy.prod.conservative.json \
    --risk-config config/risk.prod.conservative.json \
    --engine-config config/engine.prod.conservative.json \
    --hard-daily-loss-limit "${HARD_DAILY_LOSS_LIMIT:--4}" \
    --max-runtime-errors 50 \
    --kill-switch-path "$STOP_FILE" \
    --alerts-jsonl logs/live_alerts_ws_all_6h.jsonl \
    --events-jsonl logs/live_events_ws_all_6h.jsonl \
    --error-log logs/live_errors_ws_all_6h.log \
    --run-meta logs/live_run_meta_ws_all_6h.json \
    --summary-csv outputs/live_summary_ws_all_6h.csv \
    --out-csv outputs/live_trades_ws_all_6h.csv \
    --ws-raw-log logs/live_ws_raw_all_6h.jsonl \
    --telegram-bot-token "${TG_BOT_TOKEN}" \
    --telegram-chat-id "$tg_chat_id" \
    --telegram-thread-id "$tg_thread_id" \
    --max-seconds "$max_seconds" \
    >"$LOG_FILE" 2>"$ERR_FILE" &

  echo $! > "$PID_FILE"
  echo "[start] started pid=$(cat "$PID_FILE")"
  echo "[start] logs: $LOG_FILE / $ERR_FILE"
}

stop_live() {
  ensure_logs_dir
  touch "$STOP_FILE"

  if is_running; then
    local pid
    pid="$(cat "$PID_FILE")"
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      echo "[stop] force kill pid=$pid"
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    echo "[stop] stopped"
  else
    echo "[stop] not running"
  fi
}

status_live() {
  if is_running; then
    local pid
    pid="$(cat "$PID_FILE")"
    echo "[status] running pid=$pid"
  else
    echo "[status] stopped"
  fi
}

cmd="${1:-}"
case "$cmd" in
  check) check_env ;;
  preflight) preflight_live ;;
  smoke) smoke_live ;;
  health) health_live ;;
  start) start_live ;;
  stop) stop_live ;;
  status) status_live ;;
  *) usage; exit 1 ;;
esac
