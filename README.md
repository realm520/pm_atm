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
  generate_weather_config.py
  scan_all_weather_markets.py
  install_weather_scan_cron.sh
  enrich_market_report.py
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
uv run python scripts/run_live_paper.py --mode poll --market-id <MARKET_ID> --weather-config config/weather_events.generated.json

# 指定运行时长（秒），到点自动退出
uv run python scripts/run_live_paper.py --mode poll --market-id <MARKET_ID> --weather-config config/weather_events.generated.json --max-seconds 1800
```

## 实时纸交易（WebSocket）

```bash
# Polymarket 专用（推荐，默认 provider=polymarket）
uv run python scripts/run_live_paper.py --mode ws --market-ids 1427437,1498390 --max-seconds 1800

# 直接从 weather 配置读取全部市场ID（内部自动转换为 CLOB asset_ids 订阅）
uv run python scripts/run_live_paper.py --mode ws --weather-config config/weather_events.generated.json --all-from-weather-config --max-seconds 1800

# 通用 WS provider（自定义）
uv run python scripts/run_live_paper.py --mode ws --ws-provider generic --ws-url <WS_URL> --subscribe-json '{"type":"subscribe"}'
```

## 接口冒烟测试（推荐先跑）

```bash
uv run python scripts/smoke_interfaces.py --ticks 5
# 或指定市场
uv run python scripts/smoke_interfaces.py --market-id 531202 --ticks 5
```

## 自动生成天气事件配置（从 market question 推断）

```bash
# 扫描开放市场并生成配置
uv run python scripts/generate_weather_config.py --limit 100 --out config/weather_events.generated.json

# 或指定市场ID
uv run python scripts/generate_weather_config.py --market-id 531202 --out config/weather_events.generated.json
```

## 全量/增量扫描（生产建议）

```bash
# 全量重建（建议每天1次）
uv run python scripts/scan_all_weather_markets.py --full --limit 1000

# 增量更新（建议每10分钟）
uv run python scripts/scan_all_weather_markets.py --limit 1000
```

会输出：
- `config/weather_events.generated.json`（当前有效配置）
- `config/weather_scan_state.json`（扫描状态）
- `config/snapshots/weather_events.<timestamp>.json`（版本快照）

Live paper 会输出：
- `outputs/live_trades.csv`（交易明细）
- `outputs/live_summary.csv`（每次评估快照：tick、PnL、持仓、胜率）
- `logs/live_events.jsonl`（结构化事件日志：run_start/summary/trade/error/run_stop）
- `logs/live_errors.log`（异常堆栈）
- `logs/live_run_meta.json`（本次运行参数快照）
- `logs/live_ws_raw.jsonl`（原始WS消息，订阅问题排查用）

可选参数：
- `--weather-cache-ttl`：天气数据缓存秒数（默认300），降低 Open-Meteo 请求频率

## 市场标题增强报告

```bash
uv run python scripts/enrich_market_report.py \
  --ranking outputs/market_pnl_ranking_ws_all_30m.csv \
  --trades outputs/live_trades_ws_all_30m.csv
```

输出：
- `outputs/market_pnl_ranking_ws_all_30m_enriched.csv`
- `outputs/live_review_ws_all_30m_enriched.md`

## 安装定时任务（cron）

```bash
bash scripts/install_weather_scan_cron.sh
crontab -l | grep scan_all_weather_markets.py
```

## 注意

这是研究框架，不构成投资建议。实盘前请补齐：
- 实时数据时间戳对齐与延迟测量
- 订单簿冲击与流动性建模
- 风险限额与熔断机制
