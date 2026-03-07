"""Weather mispricing arbitrage research toolkit."""

from .engine import EngineConfig, PaperArbEngine
from .event_mapping import GeoCoder, build_event_map_from_markets, infer_weather_config_from_question
from .execution import ExecutionConfig, SlippageModel
from .execution_service import ExecutionService, ExecutionServiceConfig
from .exchange_sim import SimExchangeExecutor
from .live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider
from .order_store import SqliteOrderStore
from .orders import ExecutionIntent, Fill, OrderRecord, OrderSide, OrderStatus
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
    "OrderSide",
    "OrderStatus",
    "Fill",
    "OrderRecord",
    "ExecutionIntent",
    "SqliteOrderStore",
    "ExecutionService",
    "ExecutionServiceConfig",
    "SimExchangeExecutor",
    "WeatherEventConfig",
    "OpenMeteoMultiModelProvider",
    "GeoCoder",
    "infer_weather_config_from_question",
    "build_event_map_from_markets",
]
