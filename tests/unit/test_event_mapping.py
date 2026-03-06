from weather_arb.event_mapping import build_event_map_from_markets, infer_weather_config_from_question


class DummyGeo:
    def geocode(self, place_name: str):
        return (41.88, -87.63)


def test_infer_snow_question() -> None:
    q = "Will snowfall in Chicago exceed 10 inches by next week?"
    cfg = infer_weather_config_from_question(q, geocoder=DummyGeo())
    assert cfg is not None
    assert cfg.variable == "snowfall"
    assert cfg.direction == "above"
    assert cfg.threshold == 10.0


def test_build_event_map() -> None:
    markets = [
        {"id": "m1", "question": "Will temperature in Tokyo exceed 35C tomorrow?"},
        {"id": "m2", "question": "Will BTC close above 100k?"},
    ]
    out = build_event_map_from_markets(markets, geocoder=DummyGeo())
    assert "m1" in out
    assert "m2" not in out
