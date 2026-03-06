from __future__ import annotations

from dataclasses import dataclass
from itertools import product

import pandas as pd

from .strategy import StrategyConfig, WeatherMispricingStrategy


@dataclass(frozen=True)
class WalkForwardConfig:
    train_size: int = 3000
    test_size: int = 800
    step_size: int = 500


def walk_forward_backtest(df: pd.DataFrame, cfg: StrategyConfig, wf: WalkForwardConfig) -> pd.DataFrame:
    rows = []
    start = 0

    while start + wf.train_size + wf.test_size <= len(df):
        test_slice = df.iloc[start + wf.train_size : start + wf.train_size + wf.test_size]
        strat = WeatherMispricingStrategy(cfg)
        result = strat.backtest(test_slice)
        summary = result["summary"]
        rows.append(
            {
                "start": start,
                "end": start + wf.train_size + wf.test_size,
                "n_trades": summary.get("n_trades", 0),
                "win_rate": summary.get("win_rate", 0.0),
                "total_pnl": summary.get("total_pnl", 0.0),
                "sharpe": summary.get("sharpe_approx", 0.0),
            }
        )
        start += wf.step_size

    return pd.DataFrame(rows)


def parameter_grid_search(df: pd.DataFrame) -> pd.DataFrame:
    entries = [1.2, 1.6, 2.0]
    exits = [0.2, 0.35, 0.5]
    max_holds = [8, 16, 24]

    rows = []
    for e, x, h in product(entries, exits, max_holds):
        cfg = StrategyConfig(entry_z=e, exit_z=x, max_holding_steps=h)
        strat = WeatherMispricingStrategy(cfg)
        summary = strat.backtest(df)["summary"]
        rows.append(
            {
                "entry_z": e,
                "exit_z": x,
                "max_holding_steps": h,
                "n_trades": summary.get("n_trades", 0),
                "win_rate": summary.get("win_rate", 0.0),
                "total_pnl": summary.get("total_pnl", 0.0),
                "sharpe": summary.get("sharpe_approx", 0.0),
            }
        )

    return pd.DataFrame(rows).sort_values(["sharpe", "total_pnl"], ascending=False)
