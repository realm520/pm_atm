#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from weather_arb.event_mapping import build_event_map_from_markets
from weather_arb.polymarket import PolymarketClient

WEATHER_PATTERNS = [
    r"\bhighest temperature\b",
    r"\btemperature\b",
    r"\btemp\b",
    r"\bsnow\b",
    r"\bsnowfall\b",
    r"\brain\b",
    r"\brainfall\b",
    r"\bprecip\w*\b",
    r"\bhumidity\b",
    r"\bheat index\b",
    r"\bheatwave\b",
    r"\bhurricane\b",
    r"\btyphoon\b",
    r"\bnamed storm\b",
    r"\bwind speed\b",
]

# common false positives from sports/people/movie titles/politics
NON_WEATHER_PATTERNS = [
    r"\bmiami heat\b",
    r"\bred storm\b",
    r"\bgolden hurricane\b",
    r"\bjonas wind\b",
    r"\bwinds of winter\b",
    r"\bcharacter of rain\b",
    r"\bfreeze .* rents\b",
    r"\bjacob frost\b",
]


def classify_weather_market(question: str) -> tuple[bool, str]:
    q = (question or "").lower()
    if any(re.search(p, q) for p in NON_WEATHER_PATTERNS):
        return False, "non_weather_false_positive"

    has_weather_term = any(re.search(p, q) for p in WEATHER_PATTERNS)
    if not has_weather_term:
        return False, "no_weather_term"

    # stronger confidence for temp/precip contracts: place + numeric/unit cues
    has_measurement = bool(re.search(r"(-?\d+(?:\.\d+)?)\s*(°?c|°?f|ºc|ºf|inches|inch|mm)", q))
    has_place_hint = any(k in q for k in [" in ", " nyc", " london", " paris", " seattle", " tokyo", " sao paulo", " us "])

    if any(k in q for k in ["temperature", "snow", "rain", "precip"]):
        if has_measurement or has_place_hint:
            return True, "temp_precip_with_context"
        return False, "temp_precip_missing_measurement_or_place"

    # hurricane/named-storm contracts may not have measurements
    if any(k in q for k in ["hurricane", "typhoon", "named storm", "wind speed"]):
        return True, "storm_or_wind_market"

    return False, "weather_term_but_low_confidence"


def is_weather_market(question: str) -> bool:
    ok, _ = classify_weather_market(question)
    return ok


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_open_markets(client: PolymarketClient, limit: int, page_size: int = 500, verbose: bool = False) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0

    while True:
        if limit > 0 and len(out) >= limit:
            break

        batch_size = page_size if limit <= 0 else min(page_size, limit - len(out))
        try:
            page = client.list_markets(limit=batch_size, offset=offset, active=True, closed=False)
        except Exception as exc:
            print(f"[scan][error] list_markets failed offset={offset} batch_size={batch_size}: {type(exc).__name__}: {exc}")
            break

        if verbose:
            print(f"[scan] fetched page offset={offset} requested={batch_size} got={len(page)}")

        if not page:
            break

        out.extend(page)
        if len(page) < batch_size:
            break
        offset += len(page)

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan open weather markets and maintain generated weather config")
    parser.add_argument("--limit", type=int, default=0, help="How many open markets to scan (0 = all)")
    parser.add_argument("--config", default="config/weather_events.generated.json", help="Main generated config path")
    parser.add_argument("--state", default="config/weather_scan_state.json", help="Scanner state path")
    parser.add_argument("--snapshot-dir", default="config/snapshots", help="Versioned snapshot folder")
    parser.add_argument("--full", action="store_true", help="Full rebuild: replace config from current open weather markets")
    parser.add_argument("--verbose", action="store_true", help="Print diagnostics for why markets were/weren't classified as weather")
    parser.add_argument("--sample", type=int, default=10, help="How many sample rows to print in verbose mode")
    parser.add_argument("--allow-empty-full", action="store_true", help="Allow full mode to overwrite config with empty result")
    args = parser.parse_args()

    client = PolymarketClient()
    config_path = Path(args.config)
    state_path = Path(args.state)
    snapshot_dir = Path(args.snapshot_dir)

    all_open = fetch_open_markets(client, args.limit, verbose=args.verbose)

    weather_open: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    rejected_samples: list[tuple[str, str, str]] = []
    accepted_samples: list[tuple[str, str, str]] = []

    for m in all_open:
        mid = str(m.get("id") or "")
        q = str(m.get("question") or "")
        ok, reason = classify_weather_market(q)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if ok:
            weather_open.append(m)
            if len(accepted_samples) < args.sample:
                accepted_samples.append((mid, reason, q))
        else:
            if len(rejected_samples) < args.sample:
                rejected_samples.append((mid, reason, q))

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
        if not mapped and existing_cfg and not args.allow_empty_full:
            print("[scan][warn] full mode produced 0 mapped markets; keeping existing config. Use --allow-empty-full to overwrite.")
            merged = dict(existing_cfg)
        else:
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
        f"mode={new_state['mode']} scanned_open={len(all_open)} open_weather={len(current_ids)} newly_seen={len(newly_seen)} "
        f"mapped={len(mapped)} config_total={len(merged)}"
    )
    print(f"config={config_path}")
    print(f"snapshot={snapshot_path}")
    print(f"state={state_path}")

    if args.verbose:
        print("[scan][diag] classification reason counts:")
        for k in sorted(reason_counts.keys()):
            print(f"  - {k}: {reason_counts[k]}")

        print(f"[scan][diag] accepted samples (top {len(accepted_samples)}):")
        for mid, reason, q in accepted_samples:
            print(f"  + id={mid} reason={reason} q={q[:180]}")

        print(f"[scan][diag] rejected samples (top {len(rejected_samples)}):")
        for mid, reason, q in rejected_samples:
            print(f"  - id={mid} reason={reason} q={q[:180]}")


if __name__ == "__main__":
    main()
