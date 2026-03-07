#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from weather_arb.event_mapping import build_event_map_from_markets
from weather_arb.polymarket import PolymarketClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate weather event config JSON from market questions")
    parser.add_argument("--market-id", action="append", default=[], help="Specific market id(s)")
    parser.add_argument("--limit", type=int, default=50, help="How many open markets to scan when no market-id is provided")
    parser.add_argument("--out", default="config/weather_events.generated.json", help="Output JSON path")
    args = parser.parse_args()

    client = PolymarketClient()

    markets = []
    if args.market_id:
        for mid in args.market_id:
            markets.append(client.get_market(str(mid)))
    else:
        markets = client.list_markets(limit=args.limit, active=True, closed=False)

    event_map = build_event_map_from_markets(markets)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(event_map, f, ensure_ascii=False, indent=2)

    print(f"generated={len(event_map)} path={out_path}")


if __name__ == "__main__":
    main()
