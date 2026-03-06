#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from weather_arb.model_ensemble import DynamicModelEnsembler
from weather_arb.strategy import StrategyConfig, WeatherMispricingStrategy
from weather_arb.validation import parameter_grid_search, walk_forward_backtest, WalkForwardConfig


MODEL_COLS = ["ecmwf_prob", "gfs_prob", "hrrr_prob", "nam_prob", "ukmo_prob", "cmc_prob"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Weather mispricing strategy backtest")
    parser.add_argument("--input", required=True, help="CSV path")
    parser.add_argument("--out-dir", default="outputs", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input)
    for col in ["ts", "event_id", "market_prob", "label", *MODEL_COLS]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    ensembler = DynamicModelEnsembler(MODEL_COLS)
    featured = ensembler.fit_transform(df, label_col="label")

    strat = WeatherMispricingStrategy(StrategyConfig())
    result = strat.backtest(featured)
    print("Summary:", result["summary"])

    trades = result["trades"]
    if not trades.empty:
        trades.to_csv(out_dir / "trades.csv", index=False)

    wf = walk_forward_backtest(featured, StrategyConfig(), WalkForwardConfig())
    wf.to_csv(out_dir / "walk_forward.csv", index=False)

    grid = parameter_grid_search(featured)
    grid.to_csv(out_dir / "grid_search.csv", index=False)

    print(f"Saved outputs to {out_dir.resolve()}")


if __name__ == "__main__":
    main()
