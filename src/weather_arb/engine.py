from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .execution import ExecutionConfig, OrderBookLevel, SlippageModel
from .risk import RiskConfig, RiskManager
from .strategy import StrategyConfig, WeatherMispricingStrategy
from .strategy_base import Strategy


@dataclass(frozen=True)
class EngineConfig:
    base_trade_qty: float = 1.0
    cold_market_trade_qty: float = 0.5
    hot_market_spread_threshold: float = 0.02
    low_liquidity_spread_threshold: float = 0.05
    entry_z_low_liquidity_add: float = 0.8


class PaperArbEngine:
    """Paper-trading orchestrator: signal -> risk check -> slippage-adjusted fills."""

    def __init__(
        self,
        strategy_cfg: StrategyConfig | None = None,
        risk_cfg: RiskConfig | None = None,
        execution_cfg: ExecutionConfig | None = None,
        engine_cfg: EngineConfig | None = None,
        strategy: Strategy | None = None,
    ) -> None:
        self.strategy = strategy or WeatherMispricingStrategy(strategy_cfg or StrategyConfig())
        self.risk = RiskManager(risk_cfg or RiskConfig())
        self.exec_model = SlippageModel(execution_cfg or ExecutionConfig())
        self.cfg = engine_cfg or EngineConfig()

    @staticmethod
    def _spread_from_row(row: pd.Series, fallback_price: float) -> float:
        best_bid = row.get("bestBid")
        best_ask = row.get("bestAsk")
        try:
            if best_bid is not None and best_ask is not None:
                return max(0.0, float(best_ask) - float(best_bid))
        except Exception:
            pass
        return min(0.1, max(0.005, fallback_price * 0.04))

    def run(self, df: pd.DataFrame) -> dict[str, Any]:
        data = self.strategy.generate_signals(df).sort_values(["ts", "event_id"]).reset_index(drop=True)

        open_positions: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        block_counts: dict[str, int] = {}

        day_realized_pnl = 0.0
        market_realized_pnl: dict[str, float] = {}
        consecutive_losses = 0
        cooldown_until_idx = -1

        for i, row in data.iterrows():
            event_id = str(row["event_id"])
            price = float(row["market_prob"])
            z = float(row["mispricing_z"]) if pd.notna(row["mispricing_z"]) else None
            spread = self._spread_from_row(row, fallback_price=price)

            asks = [
                OrderBookLevel(price=min(price + max(0.005, spread / 2), 0.999), size=3.0),
                OrderBookLevel(price=min(price + max(0.02, spread), 0.999), size=20.0),
            ]
            bids = [
                OrderBookLevel(price=max(price - max(0.005, spread / 2), 0.001), size=3.0),
                OrderBookLevel(price=max(price - max(0.02, spread), 0.001), size=20.0),
            ]

            # exit first
            for p in list(open_positions):
                if p["event_id"] != event_id:
                    continue

                hold = p["hold"] + 1
                side = p["side"]
                gross = (price - p["entry_price"]) if side == "LONG_YES" else (p["entry_price"] - price)
                should_exit = (
                    (z is not None and abs(z) <= self.strategy.cfg.exit_z)
                    or hold >= self.strategy.cfg.max_holding_steps
                    or gross <= self.strategy.cfg.stop_loss
                )

                if should_exit:
                    if side == "LONG_YES":
                        exit_px = self.exec_model.estimate_fill_price("SELL", p["qty"], asks=asks, bids=bids)
                    else:
                        exit_px = self.exec_model.estimate_fill_price("BUY", p["qty"], asks=asks, bids=bids)

                    pnl = self.exec_model.trade_pnl(side, p["entry_fill"], exit_px, qty=p["qty"])
                    day_realized_pnl += pnl
                    market_realized_pnl[event_id] = market_realized_pnl.get(event_id, 0.0) + pnl

                    if pnl <= 0:
                        consecutive_losses += 1
                        if consecutive_losses >= self.risk.cfg.max_consecutive_losses > 0:
                            cooldown_until_idx = i + self.risk.cfg.cooldown_steps
                            block_counts["cooldown_triggered"] = block_counts.get("cooldown_triggered", 0) + 1
                            consecutive_losses = 0
                    else:
                        consecutive_losses = 0

                    trades.append(
                        {
                            "event_id": event_id,
                            "entry_ts": p["entry_ts"],
                            "exit_ts": row["ts"],
                            "side": side,
                            "entry_price": p["entry_fill"],
                            "exit_price": exit_px,
                            "pnl": pnl,
                            "holding_steps": hold,
                        }
                    )
                    open_positions.remove(p)
                else:
                    p["hold"] = hold

            signal = int(row["entry_dir"])
            if signal == 0 or z is None:
                continue

            exists = any(p["event_id"] == event_id for p in open_positions)
            if exists:
                continue

            adaptive_entry_z = self.strategy.cfg.entry_z
            if spread >= self.cfg.low_liquidity_spread_threshold:
                adaptive_entry_z += self.cfg.entry_z_low_liquidity_add
            if abs(z) < adaptive_entry_z:
                block_counts["adaptive_entry_filter"] = block_counts.get("adaptive_entry_filter", 0) + 1
                continue

            qty = self.cfg.base_trade_qty if spread <= self.cfg.hot_market_spread_threshold else self.cfg.cold_market_trade_qty
            side = "LONG_YES" if signal > 0 else "SHORT_YES"
            allowed, reason = self.risk.can_open(
                event_id=event_id,
                qty=qty,
                price=price,
                open_positions=open_positions,
                day_realized_pnl=day_realized_pnl,
                market_realized_pnl=market_realized_pnl,
                in_cooldown=(i < cooldown_until_idx),
            )
            if not allowed:
                block_counts[reason] = block_counts.get(reason, 0) + 1
                continue

            if side == "LONG_YES":
                entry_fill = self.exec_model.estimate_fill_price("BUY", qty, asks=asks, bids=bids)
            else:
                entry_fill = self.exec_model.estimate_fill_price("SELL", qty, asks=asks, bids=bids)

            open_positions.append(
                {
                    "event_id": event_id,
                    "entry_ts": row["ts"],
                    "side": side,
                    "qty": qty,
                    "entry_price": price,
                    "entry_fill": entry_fill,
                    "hold": 0,
                    "risk_reason": reason,
                }
            )

        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            return {
                "summary": {
                    "n_trades": 0,
                    "open_positions": len(open_positions),
                    "block_counts": block_counts,
                    "markets_traded": 0,
                },
                "trades": trades_df,
            }

        rets = trades_df["pnl"].astype(float).to_numpy()
        summary = {
            "n_trades": int(len(trades_df)),
            "win_rate": float((rets > 0).mean()),
            "total_pnl": float(rets.sum()),
            "avg_pnl": float(rets.mean()),
            "open_positions": len(open_positions),
            "markets_traded": int(trades_df["event_id"].nunique()),
            "block_counts": block_counts,
        }
        return {"summary": summary, "trades": trades_df}
