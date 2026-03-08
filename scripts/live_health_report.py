#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from weather_arb.order_store import SqliteOrderStore


def main() -> None:
    p = argparse.ArgumentParser(description="Live execution health report")
    p.add_argument("--orders-db", default="state/orders.live.db")
    p.add_argument("--minutes", type=int, default=5)
    p.add_argument("--reject-warn", type=float, default=0.03)
    p.add_argument("--reject-crit", type=float, default=0.08)
    p.add_argument("--timeout-crit", type=float, default=0.1)
    args = p.parse_args()

    if not Path(args.orders_db).exists():
        print(json.dumps({"ok": False, "reason": "orders db missing", "path": args.orders_db}))
        return

    store = SqliteOrderStore(args.orders_db)
    stats = store.stats_last_minutes(args.minutes)

    conn = sqlite3.connect(args.orders_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT COUNT(*) n,
               AVG(CASE WHEN avg_fill_price IS NOT NULL THEN ABS(avg_fill_price - limit_price) * 10000 END) as avg_slippage_bps
          FROM orders
         WHERE updated_at >= datetime('now', ?)
        """,
        (f"-{int(args.minutes)} minutes",),
    ).fetchone()
    conn.close()

    avg_slip = float(row["avg_slippage_bps"] or 0.0)
    out = {
        "ok": True,
        "window_minutes": args.minutes,
        "stats": stats,
        "avg_slippage_bps": avg_slip,
        "alerts": {
            "reject_warn": stats["reject_rate"] >= args.reject_warn,
            "reject_crit": stats["reject_rate"] >= args.reject_crit,
            "timeout_crit": stats["timeout_rate"] >= args.timeout_crit,
        },
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
