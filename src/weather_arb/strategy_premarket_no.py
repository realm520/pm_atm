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
        data = self.generate_signals(df).sort_values(["event_id", "ts"]).reset_index(drop=True)
        fee = self.cfg.fee_bps / 10000.0
        trades: list[dict[str, Any]] = []

        for event_id, g in data.groupby("event_id", sort=False):
            pos = 0
            entry_no_price = 0.0
            entry_ts = None
            hold_steps = 0

            for _, row in g.iterrows():
                no_price = float(row["no_price"])
                signal = int(row["entry_dir"])

                if pos == 0 and signal == 1:
                    pos = 1
                    entry_no_price = no_price
                    entry_ts = row["ts"]
                    hold_steps = 0
                    continue

                if pos != 0:
                    hold_steps += 1
                    gross = no_price - entry_no_price
                    should_exit = no_price >= self.cfg.take_profit_no_price or hold_steps >= self.cfg.max_holding_steps
                    if should_exit:
                        costs = (abs(entry_no_price) + abs(no_price)) * fee
                        pnl = gross - costs
                        trades.append(
                            {
                                "event_id": event_id,
                                "entry_ts": entry_ts,
                                "exit_ts": row["ts"],
                                "side": "LONG_NO",
                                "entry_price": entry_no_price,
                                "exit_price": no_price,
                                "pnl": pnl,
                                "holding_steps": hold_steps,
                            }
                        )
                        pos = 0

        td = pd.DataFrame(trades)
        if td.empty:
            return {"summary": {"n_trades": 0}, "trades": td}

        r = td["pnl"].to_numpy(dtype=float)
        summary = {
            "n_trades": int(len(td)),
            "win_rate": float(np.mean(r > 0)),
            "avg_pnl_per_trade": float(np.mean(r)),
            "total_pnl": float(np.sum(r)),
        }
        return {"summary": summary, "trades": td}
