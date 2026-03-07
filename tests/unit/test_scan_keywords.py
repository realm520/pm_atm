import importlib.util
from pathlib import Path


def _load_scan_module():
    p = Path(__file__).resolve().parents[2] / "scripts" / "scan_all_weather_markets.py"
    spec = importlib.util.spec_from_file_location("scan_all_weather_markets", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_weather_keyword_filter() -> None:
    mod = _load_scan_module()
    assert mod.is_weather_market("Will snowfall in Chicago exceed 10 inches?")
    assert mod.is_weather_market("Will temperature in Tokyo exceed 30C tomorrow?")
    assert mod.is_weather_market("Will a hurricane make landfall in the US by May 31?")
    assert not mod.is_weather_market("Will BTC close above 100k?")
    assert not mod.is_weather_market("Will Jonas Wind be the top goal scorer in the 2025–26 Bundesliga season?")
    assert not mod.is_weather_market("Will the Miami Heat win the NBA Eastern Conference Finals?")
