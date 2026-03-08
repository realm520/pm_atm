from __future__ import annotations

import re
from enum import StrEnum


class PremarketType(StrEnum):
    FDV = "fdv"
    AIRDROP = "airdrop"
    TOKEN_LAUNCH = "token_launch"
    OTHER = "other"


_FDV_PATTERNS = [r"\bfdv\b", r"fully diluted valuation", r"valuation"]
_AIRDROP_PATTERNS = [r"\bairdrop\b", r"token allocation", r"claim"]
_TOKEN_LAUNCH_PATTERNS = [r"token launch", r"launch token", r"tge", r"go live", r"listing"]


def classify_premarket_market(question: str) -> PremarketType:
    q = (question or "").lower()
    if any(re.search(p, q) for p in _FDV_PATTERNS):
        return PremarketType.FDV
    if any(re.search(p, q) for p in _AIRDROP_PATTERNS):
        return PremarketType.AIRDROP
    if any(re.search(p, q) for p in _TOKEN_LAUNCH_PATTERNS):
        return PremarketType.TOKEN_LAUNCH
    return PremarketType.OTHER


def is_premarket_eligible(question: str) -> bool:
    t = classify_premarket_market(question)
    return t in {PremarketType.FDV, PremarketType.AIRDROP}
