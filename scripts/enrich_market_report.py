#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from weather_arb.polymarket import PolymarketClient


def _df_to_md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows = []
    for _, r in df.iterrows():
        vals = [str(r[c]).replace("\n", " ") for c in cols]
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join([header, sep, *rows])


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich market ranking/report with market titles")
    parser.add_argument("--ranking", default="outputs/market_pnl_ranking_ws_all_30m.csv")
    parser.add_argument("--trades", default="outputs/live_trades_ws_all_30m.csv")
    parser.add_argument("--out-ranking", default="outputs/market_pnl_ranking_ws_all_30m_enriched.csv")
    parser.add_argument("--out-md", default="outputs/live_review_ws_all_30m_enriched.md")
    args = parser.parse_args()

    rank = pd.read_csv(args.ranking)
    trades = pd.read_csv(args.trades)

    client = PolymarketClient()

    market_ids = sorted({str(x) for x in rank["event_id"].tolist()})
    title_map: dict[str, str] = {}
    for mid in market_ids:
        try:
            m = client.get_market(mid)
            title_map[mid] = str(m.get("question") or "")
        except Exception:
            title_map[mid] = ""

    rank["market_id"] = rank["event_id"].astype(str)
    rank["market_question"] = rank["market_id"].map(title_map).fillna("")
    cols = ["market_id", "market_question"] + [c for c in rank.columns if c not in {"market_id", "market_question"}]
    rank = rank[cols]
    rank.to_csv(args.out_ranking, index=False)

    tr = trades.copy()
    tr["market_id"] = tr["event_id"].astype(str)
    tr["market_question"] = tr["market_id"].map(title_map).fillna("")

    top10 = rank.head(10)
    by_market = (
        tr.groupby(["market_id", "market_question"], as_index=False)
        .agg(
            trades=("pnl", "size"),
            win_rate=("pnl", lambda s: (s > 0).mean()),
            total_pnl=("pnl", "sum"),
            avg_pnl=("pnl", "mean"),
        )
        .sort_values("total_pnl", ascending=False)
    )

    lines = []
    lines.append("# Live Paper复盘（带市场标题）")
    lines.append("")
    lines.append("## Top 10 市场收益排行")
    lines.append("")
    lines.append(_df_to_md_table(top10[["market_id", "market_question", "trades", "win_rate", "total_pnl", "avg_pnl"]]))
    lines.append("")
    lines.append("## 全部成交市场表现")
    lines.append("")
    lines.append(_df_to_md_table(by_market))
    lines.append("")
    lines.append("## 交易样本（最近10笔）")
    lines.append("")
    lines.append(
        _df_to_md_table(tr.tail(10)[["market_id", "market_question", "entry_ts", "exit_ts", "side", "pnl", "holding_steps"]])
    )

    Path(args.out_md).write_text("\n".join(lines), encoding="utf-8")

    print(f"saved {args.out_ranking}")
    print(f"saved {args.out_md}")


if __name__ == "__main__":
    main()
