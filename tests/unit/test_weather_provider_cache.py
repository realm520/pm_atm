from weather_arb.weather_provider import OpenMeteoConfig, OpenMeteoMultiModelProvider, WeatherEventConfig


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_weather_provider_cache_and_stale_fallback() -> None:
    calls = {"n": 0}
    payload = {
        "hourly": {
            "time": ["2026-03-06T00:00", "2026-03-06T01:00"],
            "temperature_2m": [28.0, 32.0],
        }
    }

    def fake_get(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp(payload)
        raise Exception("network")

    p = OpenMeteoMultiModelProvider(
        event_map={
            "m1": WeatherEventConfig(latitude=10, longitude=20, variable="temperature_2m", threshold=30.0, direction="above", horizon_hours=1)
        },
        config=OpenMeteoConfig(cache_ttl_sec=999),
        http_get=fake_get,
    )

    x = p.get_probabilities("m1", {})
    y = p.get_probabilities("m1", {})
    assert x == y
    assert calls["n"] == 1
