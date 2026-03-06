from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class ExecutionConfig:
    taker_fee_bps: float = 8.0
    impact_coef: float = 0.12


class SlippageModel:
    """Simple execution model using order-book depth and non-linear impact penalty."""

    def __init__(self, config: ExecutionConfig | None = None) -> None:
        self.cfg = config or ExecutionConfig()

    def estimate_fill_price(self, side: str, qty: float, asks: Iterable[OrderBookLevel], bids: Iterable[OrderBookLevel]) -> float:
        if qty <= 0:
            raise ValueError("qty must be positive")

        levels = list(asks if side.upper() == "BUY" else bids)
        if not levels:
            raise ValueError("orderbook side is empty")

        remaining = qty
        notional = 0.0
        filled = 0.0

        for lv in levels:
            take = min(remaining, lv.size)
            if take <= 0:
                continue
            notional += take * lv.price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break

        if filled <= 0:
            raise ValueError("unable to fill quantity")

        # if depth is not enough, assume worst-level sweeping for residual
        if remaining > 1e-12:
            worst = levels[-1].price
            notional += remaining * worst
            filled += remaining

        vwap = notional / filled
        impact = self.cfg.impact_coef * (qty / max(filled, 1e-12)) ** 2
        fee = self.cfg.taker_fee_bps / 10000.0

        if side.upper() == "BUY":
            return vwap * (1 + fee + impact)
        return vwap * (1 - fee - impact)

    def trade_pnl(self, side: str, entry: float, exit: float, qty: float = 1.0) -> float:
        if side.upper() == "LONG_YES":
            return (exit - entry) * qty
        if side.upper() == "SHORT_YES":
            return (entry - exit) * qty
        raise ValueError(f"unknown side={side}")
