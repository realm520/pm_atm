import asyncio
from pathlib import Path

import pandas as pd

from weather_arb.engine import PaperArbEngine
import pytest

from weather_arb.live import CircuitBreakerTriggered, LivePaperRunner, LiveRunnerConfig, StaticForecastProvider
from weather_arb.strategy_premarket_no import PremarketNoConfig, PremarketNoLadderStrategy


class DummyExecutionService:
    def __init__(self, hard_stop: bool = False) -> None:
        self.hard_stop = hard_stop
        self.submitted: list[dict] = []

    def submit(self, intent):
        self.submitted.append({"event_id": intent.event_id, "asset_id": intent.asset_id, "side": intent.side.value})
        return intent

    def refresh_recent(self, limit: int = 200):
        return []

    def risk_flags(self, minutes: int = 5):
        return {
            "reject_rate": 0.2 if self.hard_stop else 0.0,
            "reject_warn": False,
            "reject_crit": False,
            "hard_stop": self.hard_stop,
            "consecutive_rejected": 0,
        }


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


def test_live_runner_circuit_breaker_by_execution_hard_stop(tmp_path: Path) -> None:
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
        ),
        execution_service=DummyExecutionService(hard_stop=True),
    )

    with pytest.raises(CircuitBreakerTriggered):
        asyncio.run(runner.on_tick({"id": "m1", "price": 0.52, "timestamp": "2026-01-01T00:00:00Z"}))


def test_live_runner_submit_execution_from_signal_entry_and_exit(tmp_path: Path) -> None:
    exec_svc = DummyExecutionService(hard_stop=False)
    runner = LivePaperRunner(
        engine=PaperArbEngine(),
        forecast_provider=StaticForecastProvider(0.99),
        config=LiveRunnerConfig(
            eval_every_ticks=1,
            out_csv=str(tmp_path / "live_trades.csv"),
            summary_csv=str(tmp_path / "live_summary.csv"),
            events_jsonl=str(tmp_path / "events.jsonl"),
            error_log=str(tmp_path / "errors.log"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            hard_daily_loss_limit=-999,
        ),
        execution_service=exec_svc,
    )

    runner.event_latest_asset_id["m1"] = "a1"
    df = pd.DataFrame([
        {
            "ts": "t1",
            "event_id": "m1",
            "market_prob": 0.4,
            "ecmwf_prob": 0.99,
            "gfs_prob": 0.99,
            "hrrr_prob": 0.99,
            "nam_prob": 0.99,
            "ukmo_prob": 0.99,
            "cmc_prob": 0.99,
        }
    ])

    def fake_entry(_df):
        out = _df.copy()
        out["mispricing_z"] = 2.0
        out["entry_dir"] = 1
        return out

    runner.engine.strategy.generate_signals = fake_entry  # type: ignore[method-assign]
    runner._process_execution_signals(
        df,
        {"event_id": "m1", "market_prob": 0.4, "ts": "t1"},
        {"best_ask": 0.401, "best_bid": 0.399},
    )

    assert len(exec_svc.submitted) == 1
    assert exec_svc.submitted[0]["event_id"] == "m1"
    assert exec_svc.submitted[0]["asset_id"] == "a1"
    assert exec_svc.submitted[0]["side"] == "BUY"

    def fake_exit(_df):
        out = _df.copy()
        out["mispricing_z"] = 0.0
        out["entry_dir"] = 0
        return out

    runner.engine.strategy.generate_signals = fake_exit  # type: ignore[method-assign]
    runner._process_execution_signals(
        df,
        {"event_id": "m1", "market_prob": 0.41, "ts": "t2"},
        {"best_ask": 0.411, "best_bid": 0.409},
    )
    assert len(exec_svc.submitted) == 2
    assert exec_svc.submitted[1]["side"] == "SELL"


def test_live_runner_premarket_no_maps_to_short_yes_and_respects_max_active(tmp_path: Path) -> None:
    exec_svc = DummyExecutionService(hard_stop=False)
    strategy = PremarketNoLadderStrategy(PremarketNoConfig(target_max_active_positions=1, max_holding_steps=1000))
    engine = PaperArbEngine(strategy=strategy)

    runner = LivePaperRunner(
        engine=engine,
        forecast_provider=StaticForecastProvider(0.6),
        config=LiveRunnerConfig(
            eval_every_ticks=1,
            out_csv=str(tmp_path / "live_trades.csv"),
            summary_csv=str(tmp_path / "live_summary.csv"),
            events_jsonl=str(tmp_path / "events.jsonl"),
            error_log=str(tmp_path / "errors.log"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            hard_daily_loss_limit=-999,
        ),
        execution_service=exec_svc,
    )

    runner.event_latest_asset_id["m1"] = "a1"
    runner.event_latest_asset_id["m2"] = "a2"

    df = pd.DataFrame(
        [
            {"event_id": "m1", "ts": 1, "market_prob": 0.2, "market_question": "Will FDV exceed $10B?"},
            {"event_id": "m2", "ts": 1, "market_prob": 0.2, "market_question": "Will FDV exceed $20B?"},
        ]
    )

    runner._process_execution_signals(df, {"event_id": "m1", "market_prob": 0.2, "ts": 1}, {"best_bid": 0.2, "best_ask": 0.21})
    runner._process_execution_signals(df, {"event_id": "m2", "market_prob": 0.2, "ts": 1}, {"best_bid": 0.2, "best_ask": 0.21})

    assert len(exec_svc.submitted) == 1
    assert exec_svc.submitted[0]["side"] == "SELL"
