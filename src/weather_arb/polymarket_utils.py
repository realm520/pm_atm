"""Polymarket 下单精度工具。

Polymarket CLOB 精度规则：
- BUY:  maker amount = size * price（USDC），最多 2 位小数；
        taker amount = size（份额），最多 5 位小数
- SELL: maker amount = size（份额），最多 5 位小数；
        taker amount = size * price（USDC），最多 2 位小数
"""
from __future__ import annotations


def sanitize_order_amounts(side: str, price: float, size: float) -> tuple[float, float]:
    """返回符合 Polymarket 精度要求的 (price, size)。"""
    price = round(price, 4)
    size = round(size, 5)
    if side.upper() == "BUY":
        usdc = round(size * price, 2)
        size = round(usdc / price, 5) if price > 0 else size
    # SELL: size（份额）已满足 5 位精度，taker usdc 由交易所计算，无需调整
    return price, size
