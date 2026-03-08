"""Weather mispricing arbitrage research toolkit."""

from .engine import EngineConfig, PaperArbEngine
from .event_mapping import GeoCoder, build_event_map_from_markets, infer_weather_config_from_question
from .execution import ExecutionConfig, SlippageModel
from .execution_service import ExecutionService, ExecutionServiceConfig
from .exchange_sim import SimExchangeExecutor
from .live import LivePaperRunner, LiveRunnerConfig, StaticForecastProvider
from .order_store import SqliteOrderStore
from .orders import ExecutionIntent, Fill, OrderRecord, OrderSide, OrderStatus
from .polymarket_account import PolymarketAccount, PolymarketAccountManager, PolymarketApiCreds
from .polymarket_direct_trader import DirectOrderRequest, PolymarketDirectTrader
from .polymarket_executor import PolymarketExecutionConfig, PolymarketLiveExecutor
from .polymarket_sdk_executor import PolymarketSdkExecutor, PolymarketSdkExecutorConfig
from .model_ensemble import DynamicModelEnsembler, EnsembleWeightConfig
from .risk import RiskConfig, RiskManager
from .strategy import StrategyConfig, WeatherMispricingStrategy
from .strategy_base import Strategy
from .strategy_premarket_no import PremarketNoConfig, PremarketNoLadderStrategy
from .market_classifier import PremarketType, classify_premarket_market, is_premarket_eligible
from .weather_provider import OpenMeteoMultiModelProvider, WeatherEventConfig

__all__ = [
    "Strategy",
    "StrategyConfig",
    "WeatherMispricingStrategy",
    "PremarketNoConfig",
    "PremarketNoLadderStrategy",
    "PremarketType",
    "classify_premarket_market",
    "is_premarket_eligible",
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
    "PolymarketExecutionConfig",
    "PolymarketLiveExecutor",
    "PolymarketApiCreds",
    "PolymarketAccount",
    "PolymarketAccountManager",
    "DirectOrderRequest",
    "PolymarketDirectTrader",
    "PolymarketSdkExecutor",
    "PolymarketSdkExecutorConfig",
    "WeatherEventConfig",
    "OpenMeteoMultiModelProvider",
    "GeoCoder",
    "infer_weather_config_from_question",
    "build_event_map_from_markets",
]
