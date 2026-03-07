import asyncio
from pathlib import Path

import pandas as pd

from weather_arb.engine import PaperArbEngine
import pytest

from weather_arb.live import CircuitBreakerTriggered, LivePaperRunner, LiveRunnerConfig, StaticForecastProvider


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


def test_live_runner_circuit_breaker_by_daily_loss_limit(tmp_path: Path) -> None:
    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.6),
        config=LiveRunnerConfig(
            eval_every_ticks=1,
            out_csv=str(tmp_path / "live_trades.csv"),
            summary_csv=str(tmp_path / "live_summary.csv"),
            events_jsonl=str(tmp_path / "events.jsonl"),
            error_log=str(tmp_path / "errors.log"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            hard_daily_loss_limit=0.0,
        ),
    )

    with pytest.raises(CircuitBreakerTriggered):
        asyncio.run(runner.on_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"}))


def test_live_runner_circuit_breaker_by_kill_switch(tmp_path: Path) -> None:
    kill = tmp_path / "STOP"
    kill.write_text("1", encoding="utf-8")
    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.6),
        config=LiveRunnerConfig(
            eval_every_ticks=1,
            out_csv=str(tmp_path / "live_trades.csv"),
            summary_csv=str(tmp_path / "live_summary.csv"),
            events_jsonl=str(tmp_path / "events.jsonl"),
            error_log=str(tmp_path / "errors.log"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            kill_switch_path=str(kill),
        ),
    )

    with pytest.raises(CircuitBreakerTriggered):
        asyncio.run(runner.on_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"}))
