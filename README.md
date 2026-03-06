# Weather Mispricing Arbitrage (Research)

用于研究“天气预测模型共识概率 vs 预测市场隐含概率”之间错价的策略框架。

## 功能

- 六模型动态加权（基于 rolling Brier score）
- 错价 z-score 触发开平仓
- 交易成本建模（fee/slippage）
- walk-forward 回测
- 参数网格搜索
- Polymarket 只读市场数据客户端（发现+快照）
- 实时轮询流（可替换为 WS）
- 执行层滑点模型（盘口深度 + 冲击）
- 风控层（持仓数、单事件、组合敞口、日内止损）

## 目录

```text
src/weather_arb/
  model_ensemble.py
  strategy.py
  validation.py
  polymarket.py
  realtime.py
  execution.py
  risk.py
  engine.py
config/
  weather_events.example.json
scripts/
  run_backtest.py
  run_live_paper.py
  smoke_interfaces.py
tests/unit/
  test_strategy_smoke.py
  test_execution_risk.py
  test_engine_smoke.py
```

## 运行

```bash
uv sync --dev
uv run python scripts/run_backtest.py --input weather_market_data.csv --out-dir outputs
```

## 输入 CSV 最少字段

- `ts`
- `event_id`
- `market_prob`
- `label` (最终真实结果, 0/1)
- `ecmwf_prob`, `gfs_prob`, `hrrr_prob`, `nam_prob`, `ukmo_prob`, `cmc_prob`

## 输出

- `outputs/trades.csv`
- `outputs/walk_forward.csv`
- `outputs/grid_search.csv`

## 实时纸交易（轮询）

```bash
# 静态概率（默认）
uv run python scripts/run_live_paper.py --mode poll --market-id <MARKET_ID> --poll-interval 2 --eval-every 10

# 接入 Open-Meteo 天气事件配置
uv run python scripts/run_live_paper.py --mode poll --market-id <MARKET_ID> --weather-config config/weather_events.example.json
```

## 实时纸交易（WebSocket）

```bash
uv run python scripts/run_live_paper.py --mode ws --ws-url <WS_URL> --subscribe-json '{"type":"subscribe"}'
```

## 接口冒烟测试（推荐先跑）

```bash
uv run python scripts/smoke_interfaces.py --ticks 5
# 或指定市场
uv run python scripts/smoke_interfaces.py --market-id 531202 --ticks 5
```

## 注意

这是研究框架，不构成投资建议。实盘前请补齐：
- 实时数据时间戳对齐与延迟测量
- 订单簿冲击与流动性建模
- 风险限额与熔断机制
