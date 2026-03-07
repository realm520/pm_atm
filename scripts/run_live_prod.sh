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
Usage: scripts/run_live_prod.sh <check|start|stop|status>

Environment variables required for start/check:
  POLY_EXEC_BASE_URL   Live execution gateway URL
  POLY_EXEC_API_KEY    Live execution key (optional if gateway allows)
  TG_BOT_TOKEN         Telegram bot token for alerts

Optional environment variables:
  TG_CHAT_ID           Default: -1003837508045
  TG_THREAD_ID         Default: 52
  LIVE_MAX_SECONDS     Default: 21600
EOF
}

ensure_logs_dir() {
  mkdir -p logs outputs state
}

check_env() {
  local missing=0
  for k in POLY_EXEC_BASE_URL TG_BOT_TOKEN; do
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

start_live() {
  ensure_logs_dir
  check_env

  if is_running; then
    echo "[start] already running: pid=$(cat "$PID_FILE")"
    exit 0
  fi

  rm -f "$STOP_FILE"

  local tg_chat_id="${TG_CHAT_ID:-$TG_CHAT_ID_DEFAULT}"
  local tg_thread_id="${TG_THREAD_ID:-$TG_THREAD_ID_DEFAULT}"
  local max_seconds="${LIVE_MAX_SECONDS:-21600}"

  nohup uv run python -u scripts/run_live_paper.py \
    --mode ws \
    --execution-mode live \
    --orders-db state/orders.live.db \
    --poly-exec-base-url "${POLY_EXEC_BASE_URL}" \
    --poly-exec-api-key "${POLY_EXEC_API_KEY:-}" \
    --weather-config config/weather_events.generated.json \
    --all-from-weather-config \
    --strategy-config config/strategy.prod.conservative.json \
    --risk-config config/risk.prod.conservative.json \
    --engine-config config/engine.prod.conservative.json \
    --hard-daily-loss-limit -12 \
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
  start) start_live ;;
  stop) stop_live ;;
  status) status_live ;;
  *) usage; exit 1 ;;
esac
