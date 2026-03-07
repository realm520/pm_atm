from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable

import numpy as np
import pandas as pd
import requests


@dataclass(frozen=True)
class WeatherEventConfig:
    latitude: float
    longitude: float
    variable: str = "temperature_2m"
    threshold: float = 30.0
    direction: str = "above"  # above | below
    horizon_hours: int = 24


@dataclass(frozen=True)
class OpenMeteoConfig:
    base_url: str = "https://api.open-meteo.com/v1/forecast"
    timeout_sec: int = 12
    cache_ttl_sec: int = 300


class OpenMeteoMultiModelProvider:
    """Build pseudo-probabilities from multi-model weather forecasts.

    Note: Open-Meteo model naming can change. We request a broad model set and
    use whichever series are returned successfully.
    """

    MODEL_KEYS = ["ecmwf", "gfs", "hrrr", "nam", "ukmo", "cmc"]

    def __init__(
        self,
        event_map: dict[str, WeatherEventConfig],
        config: OpenMeteoConfig | None = None,
        http_get: Callable[..., Any] | None = None,
    ) -> None:
        self.event_map = event_map
        self.cfg = config or OpenMeteoConfig()
        self.http_get = http_get or requests.get
        self._cache: dict[str, tuple[float, dict[str, float]]] = {}

    def _fetch_series(self, cfg: WeatherEventConfig) -> dict[str, np.ndarray]:
        params = {
            "latitude": cfg.latitude,
            "longitude": cfg.longitude,
            "hourly": cfg.variable,
            "forecast_days": 3,
            # Best-effort list; unsupported models will be ignored by API.
            "models": ",".join([
                "ecmwf_ifs025",
                "gfs_seamless",
                "hrrr_conus",
                "nam_conus",
                "ukmo_global_deterministic_10km",
                "gem_seamless",
            ]),
        }

        resp = self.http_get(self.cfg.base_url, params=params, timeout=self.cfg.timeout_sec)
        resp.raise_for_status()
        payload = resp.json()

        hourly = payload.get("hourly", {})
        t = hourly.get("time", [])
        if not t:
            return {}

        # Open-Meteo typically returns one combined series. If model-split fields
        # are not present, we replicate into all keys as a fallback.
        base = hourly.get(cfg.variable)
        if base is None:
            return {}

        base_arr = np.asarray(base, dtype=float)
        out: dict[str, np.ndarray] = {}

        for key in self.MODEL_KEYS:
            model_specific = hourly.get(f"{cfg.variable}_{key}")
            if model_specific is not None:
                out[key] = np.asarray(model_specific, dtype=float)
            else:
                out[key] = base_arr

        return out

    @staticmethod
    def _idx_for_horizon(times: list[str], horizon_hours: int) -> int:
        now = pd.Timestamp.now("UTC")
        target = now + pd.Timedelta(hours=horizon_hours)
        arr = pd.to_datetime(pd.Series(times), utc=True, errors="coerce")
        if arr.isna().all():
            return min(horizon_hours, max(0, len(times) - 1))
        diffs = (arr - target).abs()
        return int(diffs.idxmin())

    @staticmethod
    def _event_probability(values: np.ndarray, threshold: float, direction: str) -> np.ndarray:
        # Smooth probability proxy from point forecast: logistic distance to threshold
        scale = max(0.5, float(np.std(values) + 1e-6))
        if direction == "below":
            z = (threshold - values) / scale
        else:
            z = (values - threshold) / scale
        return 1.0 / (1.0 + np.exp(-z))

    def get_probabilities(self, event_id: str, tick: dict[str, Any]) -> dict[str, float]:
        cfg = self.event_map.get(event_id)
        if cfg is None:
            # fallback: neutral-ish prior
            return {
                "ecmwf_prob": 0.5,
                "gfs_prob": 0.5,
                "hrrr_prob": 0.5,
                "nam_prob": 0.5,
                "ukmo_prob": 0.5,
                "cmc_prob": 0.5,
            }

        now = time.time()
        cached = self._cache.get(event_id)
        if cached and now - cached[0] <= self.cfg.cache_ttl_sec:
            return cached[1]

        try:
            params = {
                "latitude": cfg.latitude,
                "longitude": cfg.longitude,
                "hourly": cfg.variable,
                "forecast_days": 3,
            }
            resp = self.http_get(self.cfg.base_url, params=params, timeout=self.cfg.timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])
            base = hourly.get(cfg.variable, [])
            if not times or not base:
                probs = {f"{k}_prob": 0.5 for k in self.MODEL_KEYS}
            else:
                idx = self._idx_for_horizon(times, cfg.horizon_hours)
                # best-effort: one series cloned into six model channels
                v = float(base[min(idx, len(base) - 1)])
                p_arr = self._event_probability(np.array([v]), cfg.threshold, cfg.direction)
                p = float(np.clip(p_arr[0], 0.001, 0.999))
                probs = {
                    "ecmwf_prob": p,
                    "gfs_prob": p,
                    "hrrr_prob": p,
                    "nam_prob": p,
                    "ukmo_prob": p,
                    "cmc_prob": p,
                }
            self._cache[event_id] = (now, probs)
            return probs
        except requests.RequestException:
            if cached:
                # stale-while-error
                return cached[1]
            return {f"{k}_prob": 0.5 for k in self.MODEL_KEYS}
