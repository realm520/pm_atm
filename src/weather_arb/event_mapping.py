from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

import requests

from .weather_provider import WeatherEventConfig

# simple aliases to improve geocoding hit-rate
PLACE_ALIASES = {
    "nyc": "New York",
    "new york city": "New York",
    "la": "Los Angeles",
    "sf": "San Francisco",
    "dc": "Washington",
    "uk": "London",
    "us": "United States",
}


class GeoCoder:
    def __init__(self, base_url: str = "https://geocoding-api.open-meteo.com/v1/search", timeout_sec: int = 10) -> None:
        self.base_url = base_url
        self.timeout_sec = timeout_sec
        self._cache: dict[str, tuple[float, float] | None] = {}

    def geocode(self, place_name: str) -> tuple[float, float] | None:
        key = place_name.strip().lower()
        if key in self._cache:
            return self._cache[key]

        query = PLACE_ALIASES.get(key, place_name)
        resp = requests.get(self.base_url, params={"name": query, "count": 1}, timeout=self.timeout_sec)
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            self._cache[key] = None
            return None

        first = results[0]
        coord = float(first["latitude"]), float(first["longitude"])
        self._cache[key] = coord
        return coord


def _clean_place(raw: str) -> str | None:
    place = (raw or "").strip(" .,")
    place = re.sub(r"\b(today|tomorrow|tonight|next|on|by|before|after)\b.*$", "", place, flags=re.IGNORECASE).strip(" .,")
    if len(place) < 2:
        return None
    return place


def _extract_place(question: str) -> str | None:
    patterns = [
        # Will highest temperature in London be ...
        r"\bin\s+([A-Za-z\s\-'.]+?)\s+(?:be|have|reach|hit|exceed|above|below|over|under|between|less|more)\b",
        # generic fallbacks
        r"\bat\s+([A-Za-z\s\-'.]+?)(?:\s+(?:be|have|reach|hit|exceed|above|below|over|under|between|less|more|by|before|after|tomorrow|today|next)|\?|$)",
        r"\bfor\s+([A-Za-z\s\-'.]+?)(?:\s+(?:be|have|reach|hit|exceed|above|below|over|under|between|less|more|by|before|after|tomorrow|today|next)|\?|$)",
        # Will NYC have less than ...
        r"^\s*will\s+([A-Za-z\s\-'.]+?)\s+(?:have|be|reach|hit)\b",
    ]
    q = question.strip()
    for p in patterns:
        m = re.search(p, q, re.IGNORECASE)
        if m:
            place = _clean_place(m.group(1))
            if place:
                return place
    return None


def _extract_value_unit(question: str) -> tuple[float | None, str | None]:
    # between 40-41°F -> 40.5, °C, inches, mm
    m_between = re.search(
        r"between\s+(-?\d+(?:\.\d+)?)\s*[-to]{1,3}\s*(-?\d+(?:\.\d+)?)\s*(°?C|°?F|celsius|fahrenheit|inches|inch|mm)",
        question,
        re.IGNORECASE,
    )
    if m_between:
        lo, hi = float(m_between.group(1)), float(m_between.group(2))
        return (lo + hi) / 2.0, m_between.group(3).lower()

    m = re.search(r"(-?\d+(?:\.\d+)?)\s*(°?C|°?F|celsius|fahrenheit|inches|inch|mm)", question, re.IGNORECASE)
    if m:
        return float(m.group(1)), m.group(2).lower()

    return None, None


def _to_metric_threshold(value: float | None, unit: str | None, variable: str) -> float | None:
    if value is None:
        return None

    if variable == "temperature_2m":
        if unit in {"°f", "f", "fahrenheit"}:
            return (value - 32.0) * 5.0 / 9.0
        return value

    if variable == "snowfall":
        # Open-Meteo snowfall is cm
        if unit in {"inches", "inch"}:
            return value * 2.54
        if unit == "mm":
            return value / 10.0
        return value

    if variable == "precipitation":
        # Open-Meteo precipitation is mm
        if unit in {"inches", "inch"}:
            return value * 25.4
        return value

    return value


def infer_weather_config_from_question(question: str, geocoder: GeoCoder | None = None) -> WeatherEventConfig | None:
    q = question.lower()
    geocoder = geocoder or GeoCoder()

    variable = "temperature_2m"
    direction = "above"

    if any(k in q for k in ["snow", "snowfall"]):
        variable = "snowfall"
        direction = "above"
    elif any(k in q for k in ["rain", "precip", "precipitation"]):
        variable = "precipitation"
        direction = "above"
    elif any(k in q for k in ["frost", "freeze", "below", "under", "subzero", "less than"]):
        variable = "temperature_2m"
        direction = "below"
    elif any(k in q for k in ["heat", "hot", "above", "over", "temperature", "highest temperature"]):
        variable = "temperature_2m"
        direction = "above"

    raw_value, unit = _extract_value_unit(question)
    threshold = _to_metric_threshold(raw_value, unit, variable)

    if threshold is None:
        if variable == "snowfall":
            threshold = 12.7  # 5 inches -> 12.7 cm
        elif variable == "precipitation":
            threshold = 10.0
        elif direction == "below":
            threshold = 0.0
        else:
            threshold = 30.0

    # explicit direction cues in text override default
    if any(k in q for k in ["below", "under", "less than"]):
        direction = "below"
    elif any(k in q for k in ["above", "over", "more than", "exceed"]):
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
        threshold=float(threshold),
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
