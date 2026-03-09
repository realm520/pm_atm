"""Polymarket 下单精度工具。

Polymarket CLOB 精度规则：
- BUY:  maker amount = size * price（USDC），最多 2 位小数；
        taker amount = size（份额），最多 4 位小数
- SELL: maker amount = size（份额），最多 5 位小数；
        taker amount = size * price（USDC），最多 2 位小数

约束来源：Polymarket API 要求 USDC 金额为整数分钱（≤2 位小数），
与市场 tick size（0.01 / 0.001 / 0.0001）无关。
py_clob_client SDK 内部会将 size round_down 到 2 位小数，再乘以 price 得到
USDC amount。因此 price 必须保持 2 位精度，否则 floor(size,2) × price 会超限。
"""
from __future__ import annotations

# USDC 金额必须是整数分钱：price 保持 2 位精度，确保 floor(size,2) × price ≤2 位
_PRICE_DECIMALS = 2
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
