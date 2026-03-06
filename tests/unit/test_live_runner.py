from pathlib import Path

from weather_arb.engine import PaperArbEngine
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider


def test_live_runner_normalize_and_dump(tmp_path: Path) -> None:
    out_file = tmp_path / "live_trades.csv"
    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.6),
        config=LiveRunnerConfig(eval_every_ticks=1, out_csv=str(out_file)),
    )

    row = runner._normalize_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"})
    assert row is not None
    assert row["event_id"] == "m1"
    assert 0.0 < row["ecmwf_prob"] < 1.0

    # no trades should not create file
    n = runner._dump_new_trades(runner.engine.run(__import__("pandas").DataFrame([row]))["trades"])
    assert n == 0
    assert not out_file.exists()
