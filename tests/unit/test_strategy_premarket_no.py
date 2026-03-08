import pandas as pd

from weather_arb.strategy_premarket_no import PremarketNoLadderStrategy, PremarketNoConfig


def test_premarket_no_generate_signal_only_for_fdv_airdrop() -> None:
    s = PremarketNoLadderStrategy(PremarketNoConfig(min_no_price=0.7, max_no_price=0.93))
    df = pd.DataFrame(
        [
            {"event_id": "1", "ts": 1, "market_prob": 0.2, "market_question": "Will FDV exceed $10B?"},
            {"event_id": "2", "ts": 1, "market_prob": 0.2, "market_question": "Will token launch in May?"},
        ]
    )
    out = s.generate_signals(df)
    assert int(out.iloc[0]["entry_dir"]) == 1
    assert int(out.iloc[1]["entry_dir"]) == 0


def test_premarket_no_backtest_has_trades() -> None:
    s = PremarketNoLadderStrategy(PremarketNoConfig(min_no_price=0.7, max_no_price=0.93, take_profit_no_price=0.85))
    df = pd.DataFrame(
        [
            {"event_id": "1", "ts": 1, "market_prob": 0.25, "market_question": "FDV above 10B?"},
            {"event_id": "1", "ts": 2, "market_prob": 0.10, "market_question": "FDV above 10B?"},
        ]
    )
    res = s.backtest(df)
    assert int(res["summary"]["n_trades"]) >= 1


def test_premarket_no_respects_max_active_positions() -> None:
    s = PremarketNoLadderStrategy(
        PremarketNoConfig(min_no_price=0.7, max_no_price=0.93, target_max_active_positions=2, max_holding_steps=100)
    )
    rows = []
    for i in range(5):
        rows.append({"event_id": str(i), "ts": 1, "market_prob": 0.2, "market_question": "FDV above 10B?"})
    df = pd.DataFrame(rows)
    res = s.backtest(df)
    assert int(res["summary"]["entries_count"]) <= 2
    assert int(res["summary"]["max_concurrent_positions"]) <= 2
