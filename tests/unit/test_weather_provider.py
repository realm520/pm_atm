from weather_arb.weather_provider import OpenMeteoMultiModelProvider, WeatherEventConfig


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_openmeteo_provider_basic() -> None:
    payload = {
        "hourly": {
            "time": ["2026-03-06T00:00", "2026-03-06T01:00"],
            "temperature_2m": [28.0, 32.0],
        }
    }

    def fake_get(*args, **kwargs):
        return _Resp(payload)

    p = OpenMeteoMultiModelProvider(
        event_map={
            "m1": WeatherEventConfig(latitude=10, longitude=20, variable="temperature_2m", threshold=30.0, direction="above", horizon_hours=1)
        },
        http_get=fake_get,
    )

    probs = p.get_probabilities("m1", tick={})
    assert set(probs.keys()) == {"ecmwf_prob", "gfs_prob", "hrrr_prob", "nam_prob", "ukmo_prob", "cmc_prob"}
    assert all(0.0 < v < 1.0 for v in probs.values())
