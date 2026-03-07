from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
import traceback

import pandas as pd
import requests

from .engine import PaperArbEngine


class ForecastProvider(Protocol):
    def get_probabilities(self, event_id: str, tick: dict[str, Any]) -> dict[str, float]: ...


class CircuitBreakerTriggered(RuntimeError):
    """Raised when runtime guardrails halt the live runner."""


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
    summary_csv: str = "outputs/live_summary.csv"
    events_jsonl: str = "logs/live_events.jsonl"
    error_log: str = "logs/live_errors.log"
    alerts_jsonl: str = "logs/live_alerts.jsonl"
    kill_switch_path: str = ""
    hard_daily_loss_limit: float = -12.0
    max_runtime_errors: int = 50
    alert_cooldown_sec: float = 120.0
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_thread_id: int = 0


class LivePaperRunner:
    """Consumes realtime ticks and repeatedly evaluates paper engine."""

    @staticmethod
    def _safe_print(message: str) -> None:
        try:
            print(message)
        except BrokenPipeError:
            # Stdout pipe can disappear when running detached/piped; ignore logging failure.
            return

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
        self.runtime_error_count = 0
        self._halted = False
        self._last_alert_at: dict[str, pd.Timestamp] = {}

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

        ts = tick.get("timestamp") or tick.get("updatedAt") or tick.get("ts") or pd.Timestamp.now("UTC").isoformat()
        model_probs = self.forecast_provider.get_probabilities(str(event_id), tick)

        return {
            "ts": ts,
            "event_id": str(event_id),
            "market_prob": float(market_prob),
            **model_probs,
        }

    def _append_event(self, event_type: str, payload: dict[str, Any]) -> None:
        out_path = Path(self.cfg.events_jsonl)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "logged_at": pd.Timestamp.now("UTC").isoformat(),
            "event_type": event_type,
            **payload,
        }
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(pd.Series(record).to_json(force_ascii=False) + "\n")

    def _append_error(self, context: str, exc: Exception) -> None:
        out_path = Path(self.cfg.error_log)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(f"[{pd.Timestamp.now('UTC').isoformat()}] {context}: {type(exc).__name__}: {exc}\n")
            f.write(traceback.format_exc())
            f.write("\n")

    def _send_telegram_alert(self, level: str, message: str) -> None:
        if not self.cfg.telegram_bot_token or not self.cfg.telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage"
        payload = {"chat_id": self.cfg.telegram_chat_id, "text": f"[{level.upper()}] {message}"}
        if self.cfg.telegram_thread_id > 0:
            payload["message_thread_id"] = int(self.cfg.telegram_thread_id)
        try:
            requests.post(url, json=payload, timeout=3)
        except Exception:
            return

    def _append_alert(self, level: str, code: str, message: str, payload: dict[str, Any] | None = None) -> None:
        now = pd.Timestamp.now("UTC")
        last = self._last_alert_at.get(code)
        if last is not None and (now - last).total_seconds() < self.cfg.alert_cooldown_sec:
            return
        self._last_alert_at[code] = now

        out_path = Path(self.cfg.alerts_jsonl)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        record: dict[str, Any] = {
            "logged_at": now.isoformat(),
            "level": level,
            "code": code,
            "message": message,
        }
        if payload:
            record.update(payload)
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(pd.Series(record).to_json(force_ascii=False) + "\n")
        self._append_event("alert", record)
        self._safe_print(f"[live][{level}] {code}: {message}")
        self._send_telegram_alert(level, f"{code}: {message}")

    def _trigger_circuit_breaker(self, reason: str, payload: dict[str, Any] | None = None) -> None:
        if self._halted:
            raise CircuitBreakerTriggered(reason)
        self._halted = True
        self._append_alert("critical", "circuit_breaker", f"halted by {reason}", payload=payload or {"reason": reason})
        self._append_event("circuit_breaker", {"reason": reason, **(payload or {})})
        raise CircuitBreakerTriggered(reason)

    def _append_summary_row(self, market_id: str, summary: dict[str, Any], n_new: int) -> None:
        out_path = Path(self.cfg.summary_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "logged_at": pd.Timestamp.now("UTC").isoformat(),
            "market_id": market_id,
            "tick_count": self.tick_count,
            "n_trades": int(summary.get("n_trades", 0) or 0),
            "new_trades": int(n_new),
            "open_positions": int(summary.get("open_positions", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0.0) or 0.0),
            "avg_pnl": float(summary.get("avg_pnl", 0.0) or 0.0),
            "win_rate": float(summary.get("win_rate", 0.0) or 0.0),
        }

        df = pd.DataFrame([row])
        write_header = not out_path.exists()
        df.to_csv(out_path, mode="a", header=write_header, index=False)
        self._append_event("summary", row)

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
        for r in new_rows:
            self._append_event("trade", r)
        return len(new_df)

    async def on_tick(self, tick: dict[str, Any]) -> None:
        if self._halted:
            raise CircuitBreakerTriggered("already_halted")
        if self.cfg.kill_switch_path and Path(self.cfg.kill_switch_path).exists():
            self._trigger_circuit_breaker("kill_switch")

        try:
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
            self._append_summary_row(row["event_id"], summary, n_new)

            total_pnl = float(summary.get("total_pnl", 0.0) or 0.0)
            if total_pnl <= self.cfg.hard_daily_loss_limit:
                self._trigger_circuit_breaker(
                    "daily_loss_limit",
                    payload={"total_pnl": total_pnl, "hard_daily_loss_limit": self.cfg.hard_daily_loss_limit},
                )

            self._safe_print(
                f"[live] ticks={self.tick_count} trades={summary.get('n_trades', 0)} "
                f"new_trades={n_new} total_pnl={summary.get('total_pnl', 0.0):.4f} "
                f"open_positions={summary.get('open_positions', 0)}"
            )
        except CircuitBreakerTriggered:
            raise
        except Exception as exc:
            self.runtime_error_count += 1
            self._append_error("on_tick", exc)
            self._append_event("error", {"context": "on_tick", "message": str(exc)})
            self._safe_print(f"[live][error] on_tick failed: {type(exc).__name__}: {exc}")
            if self.runtime_error_count >= self.cfg.max_runtime_errors:
                self._trigger_circuit_breaker(
                    "runtime_error_limit",
                    payload={
                        "runtime_error_count": self.runtime_error_count,
                        "max_runtime_errors": self.cfg.max_runtime_errors,
                    },
                )

    async def run_polling(self, streamer, market_id: str, max_seconds: float | None = None) -> None:
        self._append_event("run_start", {"mode": "poll", "market_id": market_id, "max_seconds": max_seconds})
        try:
            if max_seconds and max_seconds > 0:
                await asyncio.wait_for(streamer.stream_market(market_id, self.on_tick), timeout=max_seconds)
            else:
                await streamer.stream_market(market_id, self.on_tick)
        except asyncio.TimeoutError:
            streamer.stop()
            self._append_event("run_stop", {"reason": "timeout", "max_seconds": max_seconds})
            self._safe_print(f"[live] reached max_seconds={max_seconds}, graceful stop")
        except CircuitBreakerTriggered as exc:
            streamer.stop()
            self._append_event("run_stop", {"reason": "circuit_breaker", "message": str(exc)})
            self._safe_print(f"[live][critical] polling stopped by circuit breaker: {exc}")
        except Exception as exc:
            self._append_error("run_polling", exc)
            self._append_event("error", {"context": "run_polling", "message": str(exc)})
            self._safe_print(f"[live][error] polling loop failed: {type(exc).__name__}: {exc}")
            raise

    async def run_ws(self, ws_streamer, max_seconds: float | None = None) -> None:
        self._append_event("run_start", {"mode": "ws", "max_seconds": max_seconds})
        try:
            if max_seconds and max_seconds > 0:
                await asyncio.wait_for(ws_streamer.stream(self.on_tick), timeout=max_seconds)
            else:
                await ws_streamer.stream(self.on_tick)
        except asyncio.TimeoutError:
            ws_streamer.stop()
            self._append_event("run_stop", {"reason": "timeout", "max_seconds": max_seconds})
            self._safe_print(f"[live] reached max_seconds={max_seconds}, graceful stop")
        except CircuitBreakerTriggered as exc:
            ws_streamer.stop()
            self._append_event("run_stop", {"reason": "circuit_breaker", "message": str(exc)})
            self._safe_print(f"[live][critical] ws stopped by circuit breaker: {exc}")
        except Exception as exc:
            self._append_error("run_ws", exc)
            self._append_event("error", {"context": "run_ws", "message": str(exc)})
            self._safe_print(f"[live][error] ws loop failed: {type(exc).__name__}: {exc}")
            raise


def run_async(coro: asyncio.Future) -> None:
    asyncio.run(coro)
