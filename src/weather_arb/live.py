from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from .engine import PaperArbEngine


class ForecastProvider(Protocol):
    def get_probabilities(self, event_id: str, tick: dict[str, Any]) -> dict[str, float]: ...


class StaticForecastProvider:
    """Fallback provider: uses fixed probabilities when no weather feed is integrated."""

    def __init__(self, value: float = 0.55) -> None:
        self.value = value

    def get_probabilities(self, event_id: str, tick: dict[str, Any]) -> dict[str, float]:
        v = float(min(max(self.value, 0.001), 0.999))
        return {
            "ecmwf_prob": v,
            "gfs_prob": v,
            "hrrr_prob": v,
            "nam_prob": v,
            "ukmo_prob": v,
            "cmc_prob": v,
        }


@dataclass(frozen=True)
class LiveRunnerConfig:
    eval_every_ticks: int = 10
    history_limit: int = 5000
    out_csv: str = "outputs/live_trades.csv"


class LivePaperRunner:
    """Consumes realtime ticks and repeatedly evaluates paper engine."""

    def __init__(
        self,
        engine: PaperArbEngine,
        forecast_provider: ForecastProvider,
        config: LiveRunnerConfig | None = None,
    ) -> None:
        self.engine = engine
        self.forecast_provider = forecast_provider
        self.cfg = config or LiveRunnerConfig()
        self.rows: list[dict[str, Any]] = []
        self.seen_trade_keys: set[str] = set()
        self.tick_count = 0

    def _normalize_tick(self, tick: dict[str, Any]) -> dict[str, Any] | None:
        event_id = tick.get("id") or tick.get("market_id") or tick.get("marketId")
        if event_id is None:
            return None

        market_prob = (
            tick.get("lastTradePrice")
            or tick.get("last_trade_price")
            or tick.get("outcomePrice")
            or tick.get("price")
        )
        if market_prob is None:
            return None

        ts = tick.get("timestamp") or tick.get("updatedAt") or tick.get("ts") or pd.Timestamp.utcnow().isoformat()
        model_probs = self.forecast_provider.get_probabilities(str(event_id), tick)

        return {
            "ts": ts,
            "event_id": str(event_id),
            "market_prob": float(market_prob),
            **model_probs,
        }

    def _dump_new_trades(self, trades: pd.DataFrame) -> int:
        if trades.empty:
            return 0

        out_path = Path(self.cfg.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        new_rows = []
        for _, r in trades.iterrows():
            key = f"{r['event_id']}|{r['entry_ts']}|{r['exit_ts']}|{r['side']}"
            if key in self.seen_trade_keys:
                continue
            self.seen_trade_keys.add(key)
            new_rows.append(r.to_dict())

        if not new_rows:
            return 0

        new_df = pd.DataFrame(new_rows)
        write_header = not out_path.exists()
        new_df.to_csv(out_path, mode="a", header=write_header, index=False)
        return len(new_df)

    async def on_tick(self, tick: dict[str, Any]) -> None:
        row = self._normalize_tick(tick)
        if row is None:
            return

        self.rows.append(row)
        if len(self.rows) > self.cfg.history_limit:
            self.rows = self.rows[-self.cfg.history_limit :]

        self.tick_count += 1
        if self.tick_count % self.cfg.eval_every_ticks != 0:
            return

        df = pd.DataFrame(self.rows)
        result = self.engine.run(df)
        summary = result["summary"]
        n_new = self._dump_new_trades(result["trades"])

        print(
            f"[live] ticks={self.tick_count} trades={summary.get('n_trades', 0)} "
            f"new_trades={n_new} total_pnl={summary.get('total_pnl', 0.0):.4f}"
        )

    async def run_polling(self, streamer, market_id: str) -> None:
        await streamer.stream_market(market_id, self.on_tick)

    async def run_ws(self, ws_streamer) -> None:
        await ws_streamer.stream(self.on_tick)


def run_async(coro: asyncio.Future) -> None:
    asyncio.run(coro)
