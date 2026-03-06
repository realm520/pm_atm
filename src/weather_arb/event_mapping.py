from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

import requests

from .weather_provider import WeatherEventConfig


class GeoCoder:
    def __init__(self, base_url: str = "https://geocoding-api.open-meteo.com/v1/search", timeout_sec: int = 10) -> None:
        self.base_url = base_url
        self.timeout_sec = timeout_sec

    def geocode(self, place_name: str) -> tuple[float, float] | None:
        resp = requests.get(self.base_url, params={"name": place_name, "count": 1}, timeout=self.timeout_sec)
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            return None
        first = results[0]
        return float(first["latitude"]), float(first["longitude"])


def _extract_place(question: str) -> str | None:
    patterns = [
        r"in\s+([A-Za-z\s\-]+?)(?:\s+(?:exceed|above|below|over|under|by|before|after|tomorrow|today|next)|\?|$)",
        r"at\s+([A-Za-z\s\-]+?)(?:\s+(?:exceed|above|below|over|under|by|before|after|tomorrow|today|next)|\?|$)",
        r"for\s+([A-Za-z\s\-]+?)(?:\s+(?:exceed|above|below|over|under|by|before|after|tomorrow|today|next)|\?|$)",
    ]
    q = question.strip()
    for p in patterns:
        m = re.search(p, q, re.IGNORECASE)
        if m:
            place = m.group(1).strip(" .,")
            if len(place) >= 2:
                return place
    return None


def _extract_threshold(question: str) -> float | None:
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:°?C|celsius|inches|inch|mm)", question, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def infer_weather_config_from_question(question: str, geocoder: GeoCoder | None = None) -> WeatherEventConfig | None:
    q = question.lower()
    geocoder = geocoder or GeoCoder()

    variable = "temperature_2m"
    threshold = _extract_threshold(question) or 30.0
    direction = "above"

    if any(k in q for k in ["snow", "snowfall", "inch"]):
        variable = "snowfall"
        threshold = _extract_threshold(question) or 5.0
        direction = "above"
    elif any(k in q for k in ["rain", "precip", "precipitation"]):
        variable = "precipitation"
        threshold = _extract_threshold(question) or 10.0
        direction = "above"
    elif any(k in q for k in ["frost", "freeze", "below", "under", "subzero"]):
        variable = "temperature_2m"
        threshold = _extract_threshold(question) or 0.0
        direction = "below"
    elif any(k in q for k in ["heat", "hot", "above", "over", "temperature"]):
        variable = "temperature_2m"
        threshold = _extract_threshold(question) or 30.0
        direction = "above"

    place = _extract_place(question)
    if not place:
        return None

    coord = geocoder.geocode(place)
    if coord is None:
        return None

    lat, lon = coord
    return WeatherEventConfig(
        latitude=lat,
        longitude=lon,
        variable=variable,
        threshold=threshold,
        direction=direction,
        horizon_hours=24,
    )


def build_event_map_from_markets(markets: list[dict[str, Any]], geocoder: GeoCoder | None = None) -> dict[str, dict[str, Any]]:
    geocoder = geocoder or GeoCoder()
    out: dict[str, dict[str, Any]] = {}
    for m in markets:
        market_id = str(m.get("id"))
        q = str(m.get("question") or "")
        cfg = infer_weather_config_from_question(q, geocoder=geocoder)
        if cfg is None:
            continue
        out[market_id] = asdict(cfg)
    return out
