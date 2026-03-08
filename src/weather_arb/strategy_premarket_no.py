from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .market_classifier import classify_premarket_market, PremarketType


@dataclass(frozen=True)
class PremarketNoConfig:
    min_no_price: float = 0.70
    max_no_price: float = 0.93
    take_profit_no_price: float = 0.95
    max_holding_steps: int = 240
    fee_bps: float = 8.0
    max_single_market_weight: float = 0.10
    max_event_weight: float = 0.30
    target_min_active_positions: int = 5
    target_max_active_positions: int = 10


class PremarketNoLadderStrategy:
    """Systematic NO strategy for premarket FDV/Airdrop markets.

    Convention: market_prob is YES price, so NO price = 1 - market_prob.
    """

    def __init__(self, config: PremarketNoConfig | None = None) -> None:
        self.cfg = config or PremarketNoConfig()

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        q_col = out["market_question"] if "market_question" in out.columns else pd.Series([""] * len(out))
        out["premarket_type"] = q_col.map(classify_premarket_market)
        out["no_price"] = (1.0 - out["market_prob"].astype(float)).clip(0.001, 0.999)
        out["entry_dir"] = 0

        eligible = out["premarket_type"].isin([PremarketType.FDV, PremarketType.AIRDROP])
        band = (out["no_price"] >= self.cfg.min_no_price) & (out["no_price"] <= self.cfg.max_no_price)
        out.loc[eligible & band, "entry_dir"] = 1  # long NO (mapped to short YES at execution layer)
        return out

    def backtest(self, df: pd.DataFrame) -> dict[str, Any]:
        data = self.generate_signals(df).sort_values(["ts", "event_id"]).reset_index(drop=True)
        fee = self.cfg.fee_bps / 10000.0
        trades: list[dict[str, Any]] = []

        open_positions: dict[str, dict[str, Any]] = {}
        max_concurrent_positions = 0
        entries_count = 0

        max_active = max(1, int(self.cfg.target_max_active_positions))
        per_event_cap = max(1, int(self.cfg.max_event_weight / max(self.cfg.max_single_market_weight, 1e-9)))

        for _, row in data.iterrows():
            event_id = str(row["event_id"])
            no_price = float(row["no_price"])
            signal = int(row["entry_dir"])

            pos = open_positions.get(event_id)
            if pos is not None:
                pos["hold_steps"] += 1
                gross = no_price - float(pos["entry_no_price"])
                should_exit = no_price >= self.cfg.take_profit_no_price or pos["hold_steps"] >= self.cfg.max_holding_steps
                if should_exit:
                    costs = (abs(float(pos["entry_no_price"])) + abs(no_price)) * fee
                    pnl = gross - costs
                    trades.append(
                        {
                            "event_id": event_id,
                            "entry_ts": pos["entry_ts"],
                            "exit_ts": row["ts"],
                            "side": "LONG_NO",
                            "entry_price": float(pos["entry_no_price"]),
                            "exit_price": no_price,
                            "pnl": pnl,
                            "holding_steps": int(pos["hold_steps"]),
                            "position_weight": float(pos["position_weight"]),
                        }
                    )
                    open_positions.pop(event_id, None)
                continue

            if signal != 1:
                continue

            if len(open_positions) >= max_active:
                continue

            event_open = sum(1 for e in open_positions.keys() if e == event_id)
            if event_open >= per_event_cap:
                continue

            open_positions[event_id] = {
                "entry_no_price": no_price,
                "entry_ts": row["ts"],
                "hold_steps": 0,
                "position_weight": self.cfg.max_single_market_weight,
            }
            entries_count += 1
            max_concurrent_positions = max(max_concurrent_positions, len(open_positions))

        td = pd.DataFrame(trades)
        if td.empty:
            return {
                "summary": {
                    "n_trades": 0,
                    "entries_count": int(entries_count),
                    "max_concurrent_positions": int(max_concurrent_positions),
                },
                "trades": td,
            }

        r = td["pnl"].to_numpy(dtype=float)
        summary = {
            "n_trades": int(len(td)),
            "entries_count": int(entries_count),
            "max_concurrent_positions": int(max_concurrent_positions),
            "win_rate": float(np.mean(r > 0)),
            "avg_pnl_per_trade": float(np.mean(r)),
            "total_pnl": float(np.sum(r)),
        }
        return {"summary": summary, "trades": td}
