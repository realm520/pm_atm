from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RiskConfig:
    max_positions: int = 8
    max_event_notional: float = 2.0
    max_total_notional: float = 8.0
    daily_loss_limit: float = -2.5


class RiskManager:
    """Portfolio-level risk guardrails for paper/live strategy orchestration."""

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.cfg = config or RiskConfig()

    def can_open(
        self,
        event_id: str,
        qty: float,
        price: float,
        open_positions: list[dict[str, Any]],
        day_realized_pnl: float,
    ) -> tuple[bool, str]:
        if day_realized_pnl <= self.cfg.daily_loss_limit:
            return False, "daily_loss_limit"

        if len(open_positions) >= self.cfg.max_positions:
            return False, "max_positions"

        event_notional = sum(p["qty"] * p["entry_price"] for p in open_positions if p["event_id"] == event_id)
        total_notional = sum(p["qty"] * p["entry_price"] for p in open_positions)
        new_notional = qty * price

        if event_notional + new_notional > self.cfg.max_event_notional:
            return False, "max_event_notional"

        if total_notional + new_notional > self.cfg.max_total_notional:
            return False, "max_total_notional"

        return True, "ok"
