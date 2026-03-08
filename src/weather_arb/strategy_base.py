from __future__ import annotations

from typing import Protocol, Any

import pandas as pd


class Strategy(Protocol):
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def backtest(self, df: pd.DataFrame) -> dict[str, Any]: ...
