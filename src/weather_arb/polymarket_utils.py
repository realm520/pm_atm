"""Polymarket 下单精度工具。

Polymarket CLOB 精度规则：
- BUY:  maker amount = size * price（USDC），最多 2 位小数；
        taker amount = size（份额），最多 4 位小数
- SELL: maker amount = size（份额），最多 5 位小数；
        taker amount = size * price（USDC），最多 2 位小数
"""
from __future__ import annotations

_PRICE_DECIMALS = 4
_USDC_DECIMALS = 2       # maker/taker USDC 精度（1 美分）
_BUY_SIZE_DECIMALS = 4   # BUY taker（份额）精度
_SELL_SIZE_DECIMALS = 5  # SELL maker（份额）精度


def sanitize_order_amounts(side: str, price: float, size: float) -> tuple[float, float]:
    """返回符合 Polymarket 精度要求的 (price, size)。"""
    price = round(price, _PRICE_DECIMALS)
    if side.upper() == "BUY":
        usdc = round(size * price, _USDC_DECIMALS)
        size = round(usdc / price, _BUY_SIZE_DECIMALS) if price > 0 else round(size, _BUY_SIZE_DECIMALS)
    else:
        # SELL: maker=size（份额），taker usdc 由交易所计算
        size = round(size, _SELL_SIZE_DECIMALS)
    return price, size
