from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategyConfig:
    entry_z: float = 1.6
    exit_z: float = 0.35
    max_holding_steps: int = 16
    fee_bps: float = 8.0
    stop_loss: float = -0.08


class WeatherMispricingStrategy:
    model_cols = ["ecmwf_prob", "gfs_prob", "hrrr_prob", "nam_prob", "ukmo_prob", "cmc_prob"]

    def __init__(self, config: StrategyConfig) -> None:
        self.cfg = config

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "consensus_prob" not in out.columns:
            out["consensus_prob"] = out[self.model_cols].mean(axis=1).clip(1e-4, 1 - 1e-4)

        out["mispricing"] = out["consensus_prob"] - out["market_prob"]
        out["mispricing_z"] = out.groupby("event_id")["mispricing"].transform(
            lambda s: (s - s.rolling(72, min_periods=20).mean())
            / (s.rolling(72, min_periods=20).std(ddof=0) + 1e-9)
        )
        return out

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        out = self.prepare_features(df)
        out["entry_dir"] = 0
        out.loc[out["mispricing_z"] >= self.cfg.entry_z, "entry_dir"] = 1
        out.loc[out["mispricing_z"] <= -self.cfg.entry_z, "entry_dir"] = -1
        return out

    def backtest(self, df: pd.DataFrame) -> dict[str, Any]:
        data = self.generate_signals(df).sort_values(["event_id", "ts"]).reset_index(drop=True)
        fee = self.cfg.fee_bps / 10000.0

        trades: list[dict[str, Any]] = []

        for event_id, g in data.groupby("event_id", sort=False):
            pos = 0
            entry_price = 0.0
            entry_ts = None
            hold_steps = 0

            for _, row in g.iterrows():
                z = float(row["mispricing_z"]) if pd.notna(row["mispricing_z"]) else np.nan
                market = float(row["market_prob"])
                signal = int(row["entry_dir"])

                if pos == 0 and np.isfinite(z) and signal != 0:
                    pos = signal
                    entry_price = market
                    entry_ts = row["ts"]
                    hold_steps = 0
                    continue

                if pos != 0:
                    hold_steps += 1
                    gross = pos * (market - entry_price)
                    should_exit = (
                        (np.isfinite(z) and abs(z) <= self.cfg.exit_z)
                        or hold_steps >= self.cfg.max_holding_steps
                        or gross <= self.cfg.stop_loss
                    )

                    if should_exit:
                        costs = (abs(entry_price) + abs(market)) * fee
                        pnl = gross - costs
                        trades.append(
                            {
                                "event_id": event_id,
                                "entry_ts": entry_ts,
                                "exit_ts": row["ts"],
                                "side": "LONG_YES" if pos > 0 else "SHORT_YES",
                                "entry_price": entry_price,
                                "exit_price": market,
                                "pnl": pnl,
                                "holding_steps": hold_steps,
                                "entry_z": float(row["mispricing_z"]),
                            }
                        )
                        pos = 0

        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            return {"summary": {"n_trades": 0}, "trades": trades_df}

        returns = trades_df["pnl"].to_numpy(dtype=float)
        equity = np.cumsum(returns)
        peak = np.maximum.accumulate(equity)

        summary = {
            "n_trades": int(len(trades_df)),
            "win_rate": float(np.mean(returns > 0)),
            "avg_pnl_per_trade": float(np.mean(returns)),
            "total_pnl": float(np.sum(returns)),
            "sharpe_approx": float((np.mean(returns) / (np.std(returns, ddof=1) + 1e-12)) * np.sqrt(365)),
            "max_drawdown": float(np.min(equity - peak)),
        }
        return {"summary": summary, "trades": trades_df}
