"""Weather mispricing arbitrage research toolkit."""

from .engine import EngineConfig, PaperArbEngine
from .event_mapping import GeoCoder, build_event_map_from_markets, infer_weather_config_from_question
from .execution import ExecutionConfig, SlippageModel
from .live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider
from .model_ensemble import DynamicModelEnsembler, EnsembleWeightConfig
from .risk import RiskConfig, RiskManager
from .strategy import StrategyConfig, WeatherMispricingStrategy
from .weather_provider import OpenMeteoMultiModelProvider, WeatherEventConfig

__all__ = [
    "StrategyConfig",
    "WeatherMispricingStrategy",
    "EnsembleWeightConfig",
    "DynamicModelEnsembler",
    "ExecutionConfig",
    "SlippageModel",
    "RiskConfig",
    "RiskManager",
    "EngineConfig",
    "PaperArbEngine",
    "LiveRunnerConfig",
    "LivePaperRunner",
    "StaticForecastProvider",
    "WeatherEventConfig",
    "OpenMeteoMultiModelProvider",
    "GeoCoder",
    "infer_weather_config_from_question",
    "build_event_map_from_markets",
]
