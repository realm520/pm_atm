import pandas as pd

from weather_arb.strategy import StrategyConfig, WeatherMispricingStrategy


def test_backtest_smoke() -> None:
    rows = []
    for i in range(300):
        rows.append(
            {
                "ts": i,
                "event_id": "evt-1",
                "market_prob": 0.50 + (0.04 if i % 20 < 10 else -0.04),
                "ecmwf_prob": 0.56,
                "gfs_prob": 0.55,
                "hrrr_prob": 0.54,
                "nam_prob": 0.55,
                "ukmo_prob": 0.56,
                "cmc_prob": 0.55,
            }
        )

    df = pd.DataFrame(rows)
    strat = WeatherMispricingStrategy(StrategyConfig(entry_z=0.8, exit_z=0.2))
    result = strat.backtest(df)

    assert "summary" in result
    assert "trades" in result
    assert result["summary"]["n_trades"] >= 1
