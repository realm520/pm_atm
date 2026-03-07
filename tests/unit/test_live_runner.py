import asyncio
from pathlib import Path

import pandas as pd

from weather_arb.engine import PaperArbEngine
from weather_arb.live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider


def test_live_runner_normalize_and_dump(tmp_path: Path) -> None:
    out_file = tmp_path / "live_trades.csv"
    summary_file = tmp_path / "live_summary.csv"

    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.6),
        config=LiveRunnerConfig(
            eval_every_ticks=1,
            out_csv=str(out_file),
            summary_csv=str(summary_file),
            events_jsonl=str(tmp_path / "events.jsonl"),
            error_log=str(tmp_path / "errors.log"),
        ),
    )

    row = runner._normalize_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"})
    assert row is not None
    assert row["event_id"] == "m1"
    assert 0.0 < row["ecmwf_prob"] < 1.0

    # no trades should not create trades file
    n = runner._dump_new_trades(runner.engine.run(pd.DataFrame([row]))["trades"])
    assert n == 0
    assert not out_file.exists()

    # on_tick should write summary rows
    asyncio.run(runner.on_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"}))
    assert summary_file.exists()
    df = pd.read_csv(summary_file)
    assert len(df) == 1
    assert int(df.iloc[0]["tick_count"]) == 1
