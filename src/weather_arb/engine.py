from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .execution import ExecutionConfig, OrderBookLevel, SlippageModel
from .risk import RiskConfig, RiskManager
from .strategy import StrategyConfig, WeatherMispricingStrategy


@dataclass(frozen=True)
class EngineConfig:
    trade_qty: float = 1.0


class PaperArbEngine:
    """Paper-trading orchestrator: signal -> risk check -> slippage-adjusted fills."""

    def __init__(
        self,
        strategy_cfg: StrategyConfig | None = None,
        risk_cfg: RiskConfig | None = None,
        execution_cfg: ExecutionConfig | None = None,
        engine_cfg: EngineConfig | None = None,
    ) -> None:
        self.strategy = WeatherMispricingStrategy(strategy_cfg or StrategyConfig())
        self.risk = RiskManager(risk_cfg or RiskConfig())
        self.exec_model = SlippageModel(execution_cfg or ExecutionConfig())
        self.cfg = engine_cfg or EngineConfig()

    def run(self, df: pd.DataFrame) -> dict[str, Any]:
        data = self.strategy.generate_signals(df).sort_values(["ts", "event_id"]).reset_index(drop=True)

        open_positions: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        day_realized_pnl = 0.0

        for _, row in data.iterrows():
            event_id = row["event_id"]
            price = float(row["market_prob"])
            z = float(row["mispricing_z"]) if pd.notna(row["mispricing_z"]) else None

            asks = [OrderBookLevel(price=min(price + 0.01, 0.999), size=3.0), OrderBookLevel(price=min(price + 0.03, 0.999), size=20.0)]
            bids = [OrderBookLevel(price=max(price - 0.01, 0.001), size=3.0), OrderBookLevel(price=max(price - 0.03, 0.001), size=20.0)]

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

            side = "LONG_YES" if signal > 0 else "SHORT_YES"
            allowed, reason = self.risk.can_open(
                event_id=event_id,
                qty=self.cfg.trade_qty,
                price=price,
                open_positions=open_positions,
                day_realized_pnl=day_realized_pnl,
            )
            if not allowed:
                continue

            if side == "LONG_YES":
                entry_fill = self.exec_model.estimate_fill_price("BUY", self.cfg.trade_qty, asks=asks, bids=bids)
            else:
                entry_fill = self.exec_model.estimate_fill_price("SELL", self.cfg.trade_qty, asks=asks, bids=bids)

            open_positions.append(
                {
                    "event_id": event_id,
                    "entry_ts": row["ts"],
                    "side": side,
                    "qty": self.cfg.trade_qty,
                    "entry_price": price,
                    "entry_fill": entry_fill,
                    "hold": 0,
                    "risk_reason": reason,
                }
            )

        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            return {"summary": {"n_trades": 0, "open_positions": len(open_positions)}, "trades": trades_df}

        rets = trades_df["pnl"].astype(float).to_numpy()
        summary = {
            "n_trades": int(len(trades_df)),
            "win_rate": float((rets > 0).mean()),
            "total_pnl": float(rets.sum()),
            "avg_pnl": float(rets.mean()),
            "open_positions": len(open_positions),
        }
        return {"summary": summary, "trades": trades_df}
