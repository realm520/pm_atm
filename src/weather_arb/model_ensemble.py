from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EnsembleWeightConfig:
    lookback: int = 200
    floor_weight: float = 0.03
    smoothing: float = 0.25


class DynamicModelEnsembler:
    """Compute dynamic model weights using inverse Brier score over rolling windows."""

    def __init__(self, model_columns: Iterable[str], config: EnsembleWeightConfig | None = None) -> None:
        self.model_columns = list(model_columns)
        self.config = config or EnsembleWeightConfig()

    def fit_transform(self, df: pd.DataFrame, label_col: str = "label") -> pd.DataFrame:
        out = df.copy()
        missing = [c for c in self.model_columns + [label_col] if c not in out.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        weights = np.full(len(self.model_columns), 1.0 / len(self.model_columns), dtype=float)
        weight_history: list[np.ndarray] = []

        for i in range(len(out)):
            lo = max(0, i - self.config.lookback)
            hist = out.iloc[lo:i]

            if len(hist) >= 30:
                briers = []
                y = hist[label_col].astype(float).to_numpy()
                for col in self.model_columns:
                    p = np.clip(hist[col].astype(float).to_numpy(), 1e-6, 1 - 1e-6)
                    briers.append(float(np.mean((p - y) ** 2)))

                inv = 1.0 / (np.asarray(briers) + 1e-8)
                inv = np.maximum(inv, self.config.floor_weight)
                new_weights = inv / inv.sum()
                weights = (1 - self.config.smoothing) * weights + self.config.smoothing * new_weights

            weight_history.append(weights.copy())

        w = np.vstack(weight_history)
        for idx, col in enumerate(self.model_columns):
            out[f"w_{col}"] = w[:, idx]

        out["consensus_prob"] = 0.0
        for col in self.model_columns:
            out["consensus_prob"] += out[col] * out[f"w_{col}"]

        out["consensus_prob"] = out["consensus_prob"].clip(1e-4, 1 - 1e-4)
        return out
