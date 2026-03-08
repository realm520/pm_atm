import pandas as pd

from weather_arb.engine import PaperArbEngine


class DummyStrategy:
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["mispricing_z"] = 0.0
        out["entry_dir"] = 0
        return out

    def backtest(self, df: pd.DataFrame):
        return {"summary": {"n_trades": 0}, "trades": pd.DataFrame()}


def test_engine_accepts_strategy_override() -> None:
    engine = PaperArbEngine(strategy=DummyStrategy())
    df = pd.DataFrame([{"event_id": "1", "ts": 1, "market_prob": 0.5, "bestBid": 0.49, "bestAsk": 0.51}])
    out = engine.run(df)
    assert "summary" in out
    assert int(out["summary"]["n_trades"]) == 0
