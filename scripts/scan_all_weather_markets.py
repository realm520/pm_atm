#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from weather_arb.event_mapping import build_event_map_from_markets
from weather_arb.polymarket import PolymarketClient

WEATHER_HINTS = [
    "weather",
    "temperature",
    "temp",
    "snow",
    "snowfall",
    "rain",
    "rainfall",
    "precip",
    "precipitation",
    "frost",
    "freeze",
    "heat",
    "humid",
    "wind",
]


def is_weather_market(question: str) -> bool:
    q = (question or "").lower()
    return any(k in q for k in WEATHER_HINTS)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_open_markets(client: PolymarketClient, limit: int) -> list[dict[str, Any]]:
    return client.list_markets(limit=limit, active=True, closed=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan open weather markets and maintain generated weather config")
    parser.add_argument("--limit", type=int, default=500, help="How many open markets to scan")
    parser.add_argument("--config", default="config/weather_events.generated.json", help="Main generated config path")
    parser.add_argument("--state", default="config/weather_scan_state.json", help="Scanner state path")
    parser.add_argument("--snapshot-dir", default="config/snapshots", help="Versioned snapshot folder")
    parser.add_argument("--full", action="store_true", help="Full rebuild: replace config from current open weather markets")
    args = parser.parse_args()

    client = PolymarketClient()
    config_path = Path(args.config)
    state_path = Path(args.state)
    snapshot_dir = Path(args.snapshot_dir)

    all_open = fetch_open_markets(client, args.limit)
    weather_open = [m for m in all_open if is_weather_market(str(m.get("question") or ""))]

    existing_cfg = load_json(config_path)
    state = load_json(state_path)
    known_ids = set(state.get("known_open_weather_ids", []))

    current_ids = {str(m.get("id")) for m in weather_open}
    newly_seen = [m for m in weather_open if str(m.get("id")) not in known_ids]

    if args.full:
        targets = weather_open
    else:
        targets = newly_seen

    mapped = build_event_map_from_markets(targets)

    if args.full:
        merged = mapped
    else:
        merged = dict(existing_cfg)
        merged.update(mapped)
        # prune closed/no-longer-open weather markets
        merged = {k: v for k, v in merged.items() if k in current_ids}

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = snapshot_dir / f"weather_events.{now}.json"

    save_json(config_path, merged)
    save_json(snapshot_path, merged)

    new_state = {
        "updated_at": now,
        "known_open_weather_ids": sorted(current_ids),
        "open_weather_count": len(current_ids),
        "mapped_count": len(mapped),
        "config_count": len(merged),
        "mode": "full" if args.full else "incremental",
    }
    save_json(state_path, new_state)

    print(
        f"mode={new_state['mode']} open_weather={len(current_ids)} newly_seen={len(newly_seen)} "
        f"mapped={len(mapped)} config_total={len(merged)}"
    )
    print(f"config={config_path}")
    print(f"snapshot={snapshot_path}")
    print(f"state={state_path}")


if __name__ == "__main__":
    main()
