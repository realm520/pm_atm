#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PY_CMD="cd $ROOT_DIR && uv run python scripts/scan_all_weather_markets.py"

FULL_JOB="5 0 * * * $PY_CMD --full >> $ROOT_DIR/config/weather_scan_full.log 2>&1"
INCR_JOB="*/10 * * * * $PY_CMD >> $ROOT_DIR/config/weather_scan_incremental.log 2>&1"

TMP_FILE="$(mktemp)"
crontab -l 2>/dev/null | grep -v "scan_all_weather_markets.py" > "$TMP_FILE" || true
{
  cat "$TMP_FILE"
  echo "$FULL_JOB"
  echo "$INCR_JOB"
} | crontab -
rm -f "$TMP_FILE"

echo "Installed cron jobs:"
echo "  - Full scan daily at 00:05"
echo "  - Incremental scan every 10 minutes"
