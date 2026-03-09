from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Protocol
import traceback

import pandas as pd
import requests

from .engine import PaperArbEngine
from .orders import ExecutionIntent, OrderRecord, OrderSide, UNFILLED_TERMINAL_STATUSES


class ForecastProvider(Protocol):
    def get_probabilities(self, event_id: str, tick: dict[str, Any]) -> dict[str, float]: ...


class ExecutionHealthProvider(Protocol):
    def submit(self, intent: ExecutionIntent) -> Any: ...
    def refresh_recent(self, limit: int = 200) -> list[Any]: ...
    def risk_flags(self, minutes: int = 5) -> dict[str, bool | float | int]: ...
    def get_order_by_client_id(self, client_order_id: str) -> OrderRecord | None: ...


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
    entry_price_buffer: float = 0.003
    exit_price_buffer: float = 0.002
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_thread_id: int = 0


class LivePaperRunner:
    """Consumes realtime ticks and repeatedly evaluates paper engine."""

    _MIN_ORDER_NOTIONAL: float = 1.0  # Polymarket minimum order value in USD

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
        execution_service: ExecutionHealthProvider | None = None,
        market_yes_no: dict[str, tuple[str, str]] | None = None,
    ) -> None:
        self.engine = engine
        self.forecast_provider = forecast_provider
        self.execution_service = execution_service
        self.cfg = config or LiveRunnerConfig()
        self.rows: list[dict[str, Any]] = []
        self.seen_trade_keys: set[str] = set()
        self.event_latest_asset_id: dict[str, str] = {}
        self.event_no_asset_id: dict[str, str] = {k: v[1] for k, v in (market_yes_no or {}).items()}
        self.event_no_latest_tick: dict[str, dict] = {}
        self.live_positions: dict[str, dict[str, Any]] = {}
        self.execution_submitted_keys: set[str] = set()
        self.tick_count = 0
        self._raw_tick_attempts = 0
        self.runtime_error_count = 0
        self._halted = False
        self._last_alert_at: dict[str, pd.Timestamp] = {}

    async def _normalize_tick(self, tick: dict[str, Any]) -> dict[str, Any] | None:
        self._raw_tick_attempts += 1
        event_id = tick.get("id") or tick.get("market_id") or tick.get("marketId")
        if event_id is None:
            if self._raw_tick_attempts <= 20:
                self._safe_print(f"[live][debug] tick dropped: no event_id, keys={list(tick.keys())[:8]}")
            return None

        event_id_str = str(event_id)
        asset_id = tick.get("asset_id") or tick.get("assetId")

        # NO token tick: store for SHORT_YES execution, skip strategy evaluation
        no_asset = self.event_no_asset_id.get(event_id_str)
        if no_asset and asset_id and str(asset_id) == no_asset:
            self.event_no_latest_tick[event_id_str] = tick
            return None

        market_prob = (
            tick.get("lastTradePrice")
            or tick.get("last_trade_price")
            or tick.get("outcomePrice")
            or tick.get("price")
        )
        if market_prob is None:
            if self._raw_tick_attempts <= 20:
                self._safe_print(f"[live][debug] tick dropped: no price, event_id={event_id}, keys={list(tick.keys())[:8]}")
            return None

        ts = tick.get("timestamp") or tick.get("updatedAt") or tick.get("ts") or pd.Timestamp.now("UTC").isoformat()
        loop = asyncio.get_running_loop()
        model_probs = await loop.run_in_executor(
            None, self.forecast_provider.get_probabilities, str(event_id), tick
        )

        if asset_id:
            self.event_latest_asset_id[event_id_str] = str(asset_id)

        market_question = tick.get("market_question") or tick.get("question") or tick.get("title") or ""

        return {
            "ts": ts,
            "event_id": event_id_str,
            "market_prob": float(market_prob),
            "market_question": str(market_question),
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

    def _check_execution_health(self) -> None:
        if self.execution_service is None:
            return
        self.execution_service.refresh_recent(limit=200)
        flags = self.execution_service.risk_flags(minutes=5)
        if bool(flags.get("reject_warn")):
            self._append_alert("warning", "execution_reject_warn", f"reject_rate={float(flags.get('reject_rate', 0.0)):.2%}")
        if bool(flags.get("reject_crit")):
            self._append_alert("critical", "execution_reject_crit", f"reject_rate={float(flags.get('reject_rate', 0.0)):.2%}")
        if bool(flags.get("hard_stop")):
            self._trigger_circuit_breaker("execution_hard_stop", payload={"execution_flags": flags})

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

    @staticmethod
    def _clamp_price(v: float) -> float:
        return min(0.999, max(0.001, float(v)))

    def _limit_price_from_tick(self, tick: dict[str, Any], *, side: OrderSide, action: str, fallback: float) -> float:
        best_bid = tick.get("bestBid") or tick.get("best_bid")
        best_ask = tick.get("bestAsk") or tick.get("best_ask")
        b = self.cfg.entry_price_buffer if action == "entry" else self.cfg.exit_price_buffer

        try:
            if side == OrderSide.BUY:
                if best_ask is not None:
                    return self._clamp_price(float(best_ask) + b)
                return self._clamp_price(float(fallback) + b)
            if best_bid is not None:
                return self._clamp_price(float(best_bid) - b)
            return self._clamp_price(float(fallback) - b)
        except Exception:
            return self._clamp_price(fallback)

    def _no_price(self, event_id: str, fallback: float) -> float:
        """从 NO token 最新 tick 获取真实 NO 价格，如无数据则回退到 1-YES_price。"""
        t = self.event_no_latest_tick.get(event_id, {})
        v = t.get("lastTradePrice") or t.get("last_trade_price") or t.get("outcomePrice") or t.get("price")
        return float(v) if v is not None else fallback

    def _submit_execution_intent(self, *, event_id: str, side: OrderSide, qty: float, limit_price: float, action: str, ts: Any, asset_id: str | None = None, tick: dict[str, Any] | None = None) -> str | None:
        """Submit an execution intent. Returns client_order_id on success, None if skipped."""
        if self.execution_service is None:
            return None
        if asset_id is None:
            asset_id = self.event_latest_asset_id.get(event_id)
        if not asset_id:
            self._append_alert("warning", "execution_skip_no_asset", f"skip {action} {event_id}: missing asset_id mapping")
            return None

        key = f"{action}|{event_id}|{ts}|{side.value}"
        if key in self.execution_submitted_keys:
            return None

        clamped_price = self._clamp_price(float(limit_price))
        clamped_qty = float(max(0.001, qty))
        if clamped_price * clamped_qty < self._MIN_ORDER_NOTIONAL:
            bumped_qty = math.ceil(self._MIN_ORDER_NOTIONAL / clamped_price)
            size_key = "bestAskSize" if side == OrderSide.BUY else "bestBidSize"
            raw_size = tick.get(size_key) if tick else None
            available_size = float(raw_size) if raw_size is not None else None
            if available_size is not None and available_size < bumped_qty:
                self._append_alert(
                    "warning",
                    "execution_skip_insufficient_depth",
                    f"skip {action} {event_id}: need qty={bumped_qty} but {size_key}={available_size} (price={clamped_price})",
                )
                return None
            clamped_qty = float(bumped_qty)
            self._append_event(
                "execution_qty_bumped",
                {"action": action, "event_id": event_id, "original_qty": qty, "bumped_qty": clamped_qty, "price": clamped_price},
            )

        intent = ExecutionIntent(
            event_id=event_id,
            asset_id=asset_id,
            side=side,
            qty=clamped_qty,
            limit_price=clamped_price,
            timeout_sec=15.0,
            client_order_id=f"live-{key}",
        )
        self.execution_service.submit(intent)
        self.execution_submitted_keys.add(key)
        self._append_event(
            "execution_submit",
            {
                "action": action,
                "event_id": event_id,
                "asset_id": asset_id,
                "side": side.value,
                "qty": intent.qty,
                "limit_price": intent.limit_price,
                "client_order_id": intent.client_order_id,
            },
        )
        return intent.client_order_id

    def _process_execution_signals(self, df: pd.DataFrame, row: dict[str, Any], tick: dict[str, Any]) -> None:
        if self.execution_service is None or df.empty:
            return

        sig_df = self.engine.strategy.generate_signals(df)
        if sig_df.empty:
            return

        cfg = getattr(self.engine.strategy, "cfg", None)
        is_premarket_no = hasattr(cfg, "take_profit_no_price")

        event_id = str(row["event_id"])
        cur_rows = sig_df[sig_df["event_id"].astype(str) == event_id]
        if cur_rows.empty:
            return
        cur = cur_rows.iloc[-1]

        z = cur.get("mispricing_z")
        signal = int(cur.get("entry_dir", 0) or 0)
        market_prob = float(cur.get("market_prob") or row.get("market_prob") or 0.5)
        no_price = 1.0 - market_prob

        pos = self.live_positions.get(event_id)
        if pos is None:
            if signal == 0:
                return

            if not is_premarket_no and (z is None or not math.isfinite(float(z))):
                return

            max_active = int(getattr(cfg, "target_max_active_positions", 0) or 0)
            if max_active > 0 and len(self.live_positions) >= max_active:
                self._append_event("execution_skip", {"reason": "max_active_positions", "event_id": event_id, "max_active": max_active})
                return

            if is_premarket_no:
                # Long NO == short YES in execution layer
                side = OrderSide.SELL
                live_side = "LONG_NO"
                entry_ref_price = no_price
                entry_asset_id = None
                entry_tick = tick
            else:
                if signal > 0:
                    side = OrderSide.BUY
                    live_side = "LONG_YES"
                    entry_ref_price = market_prob
                    entry_asset_id = None  # 使用 YES token（默认）
                    entry_tick = tick
                else:
                    # SHORT_YES：做多 NO token，需要 NO token asset_id 和真实 NO 价格
                    no_asset_id = self.event_no_asset_id.get(event_id)
                    if not no_asset_id:
                        self._append_event("execution_skip", {"reason": "short_yes_no_asset_id", "event_id": event_id})
                        return
                    no_tick = self.event_no_latest_tick.get(event_id)
                    if not no_tick:
                        self._append_event("execution_skip", {"reason": "short_yes_no_tick_yet", "event_id": event_id})
                        return
                    side = OrderSide.BUY
                    live_side = "SHORT_YES"
                    entry_ref_price = self._no_price(event_id, 1.0 - market_prob)
                    entry_asset_id = no_asset_id
                    entry_tick = no_tick

            entry_client_order_id = self._submit_execution_intent(
                event_id=event_id,
                side=side,
                qty=float(self.engine.cfg.base_trade_qty),
                limit_price=self._limit_price_from_tick(entry_tick, side=side, action="entry", fallback=entry_ref_price),
                action="entry",
                ts=cur.get("ts") or row.get("ts"),
                asset_id=entry_asset_id,
                tick=entry_tick,
            )
            self.live_positions[event_id] = {
                "side": live_side,
                "entry_price": entry_ref_price,
                "hold_steps": 0,
                "entry_ts": cur.get("ts") or row.get("ts"),
                "entry_client_order_id": entry_client_order_id,
            }
            return

        pos["hold_steps"] = int(pos.get("hold_steps", 0)) + 1

        if pos["side"] == "LONG_NO" and is_premarket_no:
            gross = no_price - float(pos.get("entry_price", no_price))
            should_exit = (
                no_price >= float(getattr(cfg, "take_profit_no_price", 0.95))
                or pos["hold_steps"] >= int(getattr(cfg, "max_holding_steps", 240))
            )
            exit_side = OrderSide.BUY
            exit_asset_id = None
            exit_tick = tick
        elif pos["side"] == "SHORT_YES":
            # SHORT_YES = LONG NO token；用真实 NO 价格计算 P&L
            cur_no_price = self._no_price(event_id, 1.0 - market_prob)
            gross = cur_no_price - float(pos.get("entry_price", cur_no_price))
            should_exit = (
                (z is not None and math.isfinite(float(z)) and abs(float(z)) <= self.engine.strategy.cfg.exit_z)
                or pos["hold_steps"] >= self.engine.strategy.cfg.max_holding_steps
                or gross <= self.engine.strategy.cfg.stop_loss
            )
            exit_side = OrderSide.SELL
            exit_asset_id = self.event_no_asset_id.get(event_id)
            exit_tick = self.event_no_latest_tick.get(event_id, tick)
        else:
            gross = market_prob - float(pos.get("entry_price", market_prob))
            should_exit = (
                (z is not None and math.isfinite(float(z)) and abs(float(z)) <= self.engine.strategy.cfg.exit_z)
                or pos["hold_steps"] >= self.engine.strategy.cfg.max_holding_steps
                or gross <= self.engine.strategy.cfg.stop_loss
            )
            exit_side = OrderSide.SELL
            exit_asset_id = None
            exit_tick = tick

        if not should_exit:
            return

        # 出场前检查入场订单是否真正成交，避免因入场未成交导致 SELL "not enough balance"
        # entry_client_order_id 首次确认已成交后会被置 None，后续 tick 跳过此查询
        entry_coid = pos.get("entry_client_order_id")
        if entry_coid and self.execution_service is not None:
            entry_order = self.execution_service.get_order_by_client_id(entry_coid)
            if entry_order is not None:
                if entry_order.status in UNFILLED_TERMINAL_STATUSES:
                    self._append_event(
                        "execution_skip",
                        {
                            "reason": "entry_not_filled",
                            "event_id": event_id,
                            "entry_client_order_id": entry_coid,
                            "entry_status": str(entry_order.status),
                        },
                    )
                    self._safe_print(
                        f"[live][warning] skip exit {event_id}: entry order {entry_coid} status={entry_order.status}, no tokens to sell"
                    )
                    self.live_positions.pop(event_id, None)
                    return
                # 入场已确认活跃/成交，清除 ID 避免后续 tick 重复查询
                pos["entry_client_order_id"] = None

        self._submit_execution_intent(
            event_id=event_id,
            side=exit_side,
            qty=float(self.engine.cfg.base_trade_qty),
            limit_price=self._limit_price_from_tick(exit_tick, side=exit_side, action="exit", fallback=market_prob),
            action="exit",
            ts=cur.get("ts") or row.get("ts"),
            asset_id=exit_asset_id,
            tick=exit_tick,
        )
        self.live_positions.pop(event_id, None)

    def bootstrap_positions_from_snapshot(
        self,
        snapshots: list[dict[str, Any]],
        asset_to_market_id: dict[str, str],
        market_yes_no: dict[str, tuple[str, str]],
    ) -> None:
        """Pre-populate live_positions from broker snapshot at startup.

        Call this after market metadata is resolved and before the WS/poll loop starts.
        Any asset_id found in *snapshots* that maps to a known market is loaded into
        live_positions with hold_steps=0 (entry already confirmed filled).
        """
        no_asset_ids = {v[1] for v in market_yes_no.values()}
        n_bootstrapped = 0

        for snap in snapshots:
            asset_id = str(snap.get("asset_id", ""))
            size = float(snap.get("size") or 0)
            avg_price = snap.get("avg_price")

            market_id = asset_to_market_id.get(asset_id)
            if not market_id or size <= 0:
                continue

            if market_id in self.live_positions:
                continue  # already loaded (shouldn't happen at startup)

            side = "SHORT_YES" if asset_id in no_asset_ids else "LONG_YES"
            entry_price = float(avg_price) if avg_price is not None else 0.5

            self.live_positions[market_id] = {
                "side": side,
                "entry_price": entry_price,
                "hold_steps": 0,
                "entry_ts": pd.Timestamp.now("UTC").isoformat(),
                "entry_client_order_id": None,  # already filled, skip fill-check
            }
            self._append_event(
                "position_bootstrapped",
                {
                    "market_id": market_id,
                    "asset_id": asset_id,
                    "side": side,
                    "entry_price": entry_price,
                    "size": size,
                },
            )
            self._safe_print(
                f"[live][startup] bootstrapped position: market={market_id} side={side} "
                f"entry_price={entry_price:.4f} size={size}"
            )
            n_bootstrapped += 1

        if n_bootstrapped:
            self._safe_print(
                f"[live][startup] {n_bootstrapped} existing position(s) loaded, strategy continues from here"
            )
        else:
            self._safe_print("[live][startup] no existing positions found, starting fresh")

    async def on_tick(self, tick: dict[str, Any]) -> None:
        if self._halted:
            raise CircuitBreakerTriggered("already_halted")
        if self.cfg.kill_switch_path and Path(self.cfg.kill_switch_path).exists():
            self._trigger_circuit_breaker("kill_switch")

        try:
            row = await self._normalize_tick(tick)
            if row is None:
                return

            self.rows.append(row)
            if len(self.rows) > self.cfg.history_limit:
                self.rows = self.rows[-self.cfg.history_limit :]

            self.tick_count += 1
            if self.tick_count % self.cfg.eval_every_ticks != 0:
                return

            df = pd.DataFrame(self.rows)
            self._process_execution_signals(df, row, tick)

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

            self._check_execution_health()

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
        self._safe_print(f"[live] run_ws starting, max_seconds={max_seconds}")
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
